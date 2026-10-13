import type { FastifyReply, FastifyRequest } from "fastify";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  resolveTrackingRequest: vi.fn(),
  ingestEvent: vi.fn(),
}));

vi.mock("./trackingRequest.js", () => ({
  resolveTrackingRequest: mocks.resolveTrackingRequest,
}));

vi.mock("./ingestEvent.js", () => ({
  ingestEvent: mocks.ingestEvent,
}));

import { batchIngestion, trackBatch } from "./trackBatch.js";

const NOW = new Date("2026-10-13T12:00:00.000Z");

const pageview = (overrides: Record<string, unknown> = {}) => ({
  type: "pageview",
  site_id: "site_abc",
  pathname: "/settings",
  ...overrides,
});

function requestWithBody(body: unknown, headers: Record<string, string> = { authorization: "Bearer sk_test" }) {
  return { body, headers, ip: "198.51.100.10" } as unknown as FastifyRequest;
}

function replyStub() {
  const reply = {
    status: vi.fn(function (this: typeof reply) {
      return this;
    }),
    header: vi.fn(function (this: typeof reply) {
      return this;
    }),
    send: vi.fn(function (this: typeof reply) {
      return this;
    }),
  };
  return reply as unknown as FastifyReply & typeof reply;
}

describe("trackBatch", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ now: NOW });
    mocks.resolveTrackingRequest.mockImplementation(async (_request, payload, options) => ({
      payload,
      site: { siteId: 42 },
      trustedServerSideIngestion: true,
      receivedAt: options.occurredAt,
    }));
    mocks.ingestEvent.mockResolvedValue({ status: "tracked", sessionId: "session-alice" });
  });

  afterEach(async () => {
    await batchIngestion.drain();
    vi.useRealTimers();
  });

  it("requires a bearer token", async () => {
    const reply = replyStub();

    await trackBatch(requestWithBody({ events: [pageview()] }, {}), reply);

    expect(reply.status).toHaveBeenCalledWith(401);
    expect(mocks.resolveTrackingRequest).not.toHaveBeenCalled();
  });

  it.each([[{}], [{ events: [] }], [{ events: Array.from({ length: 501 }, () => pageview()) }], [[pageview()]]])(
    "rejects a malformed batch body",
    async body => {
      const reply = replyStub();

      await trackBatch(requestWithBody(body), reply);

      expect(reply.status).toHaveBeenCalledWith(400);
      expect(mocks.ingestEvent).not.toHaveBeenCalled();
    }
  );

  it("acknowledges the batch, then ingests every event", async () => {
    const reply = replyStub();

    await trackBatch(requestWithBody({ events: [pageview(), pageview({ pathname: "/about" })] }), reply);

    expect(reply.status).toHaveBeenCalledWith(202);
    expect(reply.send).toHaveBeenCalledWith({ success: true, accepted: 2, rejected: [] });

    await batchIngestion.drain();
    expect(mocks.ingestEvent).toHaveBeenCalledTimes(2);
  });

  it("answers without waiting for ingestion to finish", async () => {
    let finishIngest!: () => void;
    mocks.ingestEvent.mockReturnValue(new Promise<void>(resolve => (finishIngest = resolve)));
    const reply = replyStub();

    await trackBatch(requestWithBody({ events: [pageview()] }), reply);

    expect(reply.status).toHaveBeenCalledWith(202);
    finishIngest();
  });

  it("stamps each event with its own timestamp, and now when it has none", async () => {
    const reply = replyStub();

    await trackBatch(
      requestWithBody({ events: [pageview({ timestamp: "2026-10-13T11:58:30.000Z" }), pageview()] }),
      reply
    );
    await batchIngestion.drain();

    const stamped = mocks.ingestEvent.mock.calls.map(([trackingRequest]) => trackingRequest.receivedAt.toISOString());
    expect(stamped.sort()).toEqual(["2026-10-13T11:58:30.000Z", "2026-10-13T12:00:00.000Z"]);
    // `timestamp` is batch-only: it never reaches the strict event schema.
    expect(mocks.resolveTrackingRequest.mock.calls[0][1]).not.toHaveProperty("timestamp");
  });

  it("pulls a timestamp from a fast clock back to now", async () => {
    const reply = replyStub();

    await trackBatch(requestWithBody({ events: [pageview({ timestamp: "2026-10-13T12:07:00.000Z" })] }), reply);
    await batchIngestion.drain();

    expect(mocks.ingestEvent.mock.calls[0][0].receivedAt).toEqual(NOW);
  });

  it("rejects bad events by index and ingests the rest", async () => {
    const reply = replyStub();

    await trackBatch(
      requestWithBody({
        events: [
          pageview(),
          { type: "custom_event", site_id: "site_abc" }, // no event_name
          pageview({ timestamp: "yesterday" }),
          pageview({ timestamp: "2026-10-12T11:59:59.000Z" }), // just over 24h old
          "not an object",
        ],
      }),
      reply
    );
    await batchIngestion.drain();

    expect(reply.status).toHaveBeenCalledWith(202);
    const [response] = reply.send.mock.calls[0] as unknown as [{ accepted: number; rejected: { index: number }[] }];
    expect(response.accepted).toBe(1);
    expect(response.rejected.map(r => r.index)).toEqual([1, 2, 3, 4]);
    expect(mocks.ingestEvent).toHaveBeenCalledOnce();
  });

  it("answers 400 when no event survives validation", async () => {
    const reply = replyStub();

    await trackBatch(requestWithBody({ events: [{ type: "pageview" }] }), reply);

    expect(reply.status).toHaveBeenCalledWith(400);
    expect(mocks.ingestEvent).not.toHaveBeenCalled();
  });

  it("ingests nothing when one event names an unknown Site", async () => {
    mocks.resolveTrackingRequest.mockImplementation(async (_request, payload) =>
      payload.site_id === "nope" ? null : { payload, site: { siteId: 42 }, trustedServerSideIngestion: true }
    );
    const reply = replyStub();

    await trackBatch(requestWithBody({ events: [pageview(), pageview({ site_id: "nope" })] }), reply);
    await batchIngestion.drain();

    expect(reply.status).toHaveBeenCalledWith(404);
    expect(mocks.ingestEvent).not.toHaveBeenCalled();
  });

  it("ingests nothing when the key may not ingest for a Site in the batch", async () => {
    mocks.resolveTrackingRequest.mockImplementation(async (_request, payload) => ({
      payload,
      site: { siteId: 42 },
      trustedServerSideIngestion: payload.site_id !== "someone_elses",
    }));
    const reply = replyStub();

    await trackBatch(requestWithBody({ events: [pageview(), pageview({ site_id: "someone_elses" })] }), reply);
    await batchIngestion.drain();

    expect(reply.status).toHaveBeenCalledWith(403);
    expect(mocks.ingestEvent).not.toHaveBeenCalled();
  });

  it("keeps ingesting the batch when one event throws", async () => {
    mocks.ingestEvent.mockRejectedValueOnce(new Error("redis down"));
    const reply = replyStub();

    await trackBatch(requestWithBody({ events: [pageview(), pageview(), pageview()] }), reply);
    await batchIngestion.drain();

    expect(mocks.ingestEvent).toHaveBeenCalledTimes(3);
  });

  it("answers 503 while the backlog of acknowledged events is full", async () => {
    let finishIngest!: () => void;
    mocks.ingestEvent.mockReturnValue(new Promise<void>(resolve => (finishIngest = resolve)));
    const full = { events: Array.from({ length: 500 }, () => pageview()) };

    // 40 batches of 500 fill the 20,000-event backlog while nothing drains.
    for (let i = 0; i < 40; i++) {
      await trackBatch(requestWithBody(full), replyStub());
    }
    const reply = replyStub();
    await trackBatch(requestWithBody({ events: [pageview()] }), reply);

    expect(reply.status).toHaveBeenCalledWith(503);
    expect(reply.header).toHaveBeenCalledWith("Retry-After", "5");

    finishIngest();
    mocks.ingestEvent.mockResolvedValue({ status: "tracked", sessionId: "s" });
    await batchIngestion.drain();

    // Room again once the backlog has been worked off.
    const after = replyStub();
    await trackBatch(requestWithBody({ events: [pageview()] }), after);
    expect(after.status).toHaveBeenCalledWith(202);
  });
});
