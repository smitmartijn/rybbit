import { FastifyReply, FastifyRequest } from "fastify";
import { z } from "zod";
import { createServiceLogger } from "../../lib/logger/logger.js";
import { ingestEvent } from "./ingestEvent.js";
import { trackingPayloadSchema } from "./trackingPayload.js";
import { resolveTrackingRequest, type TrackingRequest } from "./trackingRequest.js";

const logger = createServiceLogger("track-batch");

const MAX_BATCH_EVENTS = 500;
// A batch exists to report events a few seconds or minutes late. Anything
// older would land in a session that has long since closed and a day whose
// usage has already been counted.
const MAX_EVENT_AGE_MS = 24 * 60 * 60 * 1000;
// Acknowledged events not yet ingested, across all batches on this worker.
// Past this the endpoint answers 503 rather than buffering without bound.
const MAX_PENDING_EVENTS = 20_000;
// Each event costs up to two Redis round-trips; this many run at once.
const INGEST_CONCURRENCY = 25;

const batchBodySchema = z.object({
  events: z.array(z.unknown()).min(1).max(MAX_BATCH_EVENTS),
});

const timestampSchema = z.string().datetime({ offset: true });

interface RejectedEvent {
  index: number;
  error: string;
  details?: unknown;
}

/**
 * Ingests acknowledged batches after their HTTP response has gone out.
 *
 * This is the one place ingestion runs detached from a request, so it carries
 * what a request would otherwise provide: a bound on how much is outstanding,
 * and a way for shutdown to wait for it.
 */
class BatchIngestion {
  private pendingEvents = 0;
  private running = new Set<Promise<void>>();

  hasRoomFor(count: number): boolean {
    return this.pendingEvents + count <= MAX_PENDING_EVENTS;
  }

  submit(trackingRequests: TrackingRequest[]): void {
    this.pendingEvents += trackingRequests.length;
    const run = this.ingestAll(trackingRequests).finally(() => {
      this.running.delete(run);
    });
    this.running.add(run);
  }

  private async ingestAll(trackingRequests: TrackingRequest[]): Promise<void> {
    let next = 0;
    const worker = async () => {
      while (next < trackingRequests.length) {
        const trackingRequest = trackingRequests[next++];
        try {
          await ingestEvent(trackingRequest);
        } catch (error) {
          logger.error({ err: error, siteId: trackingRequest.site.siteId }, "Error ingesting batched event");
        } finally {
          this.pendingEvents--;
        }
      }
    };
    await Promise.all(Array.from({ length: Math.min(INGEST_CONCURRENCY, trackingRequests.length) }, worker));
  }

  /** Waits for every acknowledged batch to finish. Only for shutdown. */
  async drain(): Promise<void> {
    while (this.running.size > 0) {
      await Promise.all(this.running);
    }
  }
}

export const batchIngestion = new BatchIngestion();

/**
 * Split one raw batch entry into its event payload and the moment it happened.
 * `timestamp` is the only field a batched event has beyond a single one.
 */
function parseBatchEvent(raw: unknown, index: number, now: number) {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    return { rejected: { index, error: "Event must be an object" } satisfies RejectedEvent };
  }

  const { timestamp, ...body } = raw as Record<string, unknown>;

  const payload = trackingPayloadSchema.safeParse(body);
  if (!payload.success) {
    return { rejected: { index, error: "Invalid payload", details: payload.error.flatten() } satisfies RejectedEvent };
  }

  if (timestamp === undefined) {
    return { payload: payload.data, occurredAt: new Date(now) };
  }

  const parsedTimestamp = timestampSchema.safeParse(timestamp);
  if (!parsedTimestamp.success) {
    return { rejected: { index, error: "timestamp must be an ISO 8601 datetime" } satisfies RejectedEvent };
  }

  const occurredAtMs = Date.parse(parsedTimestamp.data);
  if (now - occurredAtMs > MAX_EVENT_AGE_MS) {
    return { rejected: { index, error: "timestamp is more than 24 hours old" } satisfies RejectedEvent };
  }

  // A sender's clock running ahead must not put events in the future.
  return { payload: payload.data, occurredAt: new Date(Math.min(occurredAtMs, now)) };
}

/**
 * HTTP entry point for server-side senders reporting many events at once.
 *
 * Unlike `/api/track` this answers before ingesting: everything that can fail
 * the request (shape, credentials, Site) is settled first, then the events are
 * handed to `batchIngestion` and the caller is released. A batch's events
 * carry their own timestamps, so the endpoint is only open to a bearer token
 * that may ingest for every Site in it.
 */
export async function trackBatch(request: FastifyRequest, reply: FastifyReply) {
  try {
    const authHeader = request.headers["authorization"];
    if (typeof authHeader !== "string" || !authHeader.startsWith("Bearer ")) {
      return reply.status(401).send({ success: false, error: "API key required" });
    }

    const body = batchBodySchema.safeParse(request.body);
    if (!body.success) {
      return reply.status(400).send({
        success: false,
        error: `Body must be { events: [...] } with 1 to ${MAX_BATCH_EVENTS} events`,
      });
    }

    const now = Date.now();
    const rejected: RejectedEvent[] = [];
    const trackingRequests: TrackingRequest[] = [];

    // One at a time on purpose: the first event of each Site warms the Site
    // Configuration and credential caches, and the rest resolve from memory.
    for (const [index, raw] of body.data.events.entries()) {
      const parsed = parseBatchEvent(raw, index, now);
      if (parsed.rejected) {
        rejected.push(parsed.rejected);
        continue;
      }

      const trackingRequest = await resolveTrackingRequest(request, parsed.payload, {
        occurredAt: parsed.occurredAt,
      });

      if (!trackingRequest) {
        return reply.status(404).send({ success: false, error: `Site not found: ${parsed.payload.site_id}` });
      }
      if (!trackingRequest.trustedServerSideIngestion) {
        return reply.status(403).send({
          success: false,
          error: `API key cannot ingest events for site ${parsed.payload.site_id}`,
        });
      }

      trackingRequests.push(trackingRequest);
    }

    if (trackingRequests.length === 0) {
      return reply.status(400).send({ success: false, error: "No valid events", accepted: 0, rejected });
    }

    if (!batchIngestion.hasRoomFor(trackingRequests.length)) {
      logger.warn({ events: trackingRequests.length }, "Batch refused - ingestion backlog full");
      return reply
        .status(503)
        .header("Retry-After", "5")
        .send({ success: false, error: "Ingestion backlog full, retry shortly" });
    }

    batchIngestion.submit(trackingRequests);

    return reply.status(202).send({ success: true, accepted: trackingRequests.length, rejected });
  } catch (error) {
    logger.error({ err: error }, "Error tracking batch");
    return reply.status(500).send({ success: false, error: "Failed to track batch" });
  }
}
