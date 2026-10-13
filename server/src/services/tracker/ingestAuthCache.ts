import crypto from "crypto";

// A granted decision is served without touching Postgres for this long, then
// re-verified in the background while the old answer keeps being served.
const GRANT_FRESH_MS = 60 * 1000;
// Past this age a grant is no longer served stale: if re-verification has been
// failing for this long, the event waits on a real answer again.
const GRANT_MAX_STALE_MS = 10 * 60 * 1000;
// Denials are kept only long enough to absorb a burst. A denial can be a
// database blip rather than a bad key, and a wrongly-denied server-side event
// is recorded under the reporting server's IP, so it must clear quickly.
const DENIAL_FRESH_MS = 5 * 1000;
const MAX_ENTRIES = 1000;

interface Entry {
  trusted: boolean;
  verifiedAt: number;
  /** In-flight verification, shared by every event that arrives meanwhile. */
  pending?: Promise<boolean>;
}

/**
 * Per-worker memo of "may this bearer token ingest for this Site?".
 *
 * Verifying an API key costs a Postgres read and a write to the key's own row.
 * A server-side integration sends every event under one key, so without this
 * each event queues on that row and on the worker's small connection pool —
 * the tracking endpoint is the one place a key is checked thousands of times a
 * minute. The trade is that a revoked or re-scoped key keeps ingesting for up
 * to about a minute, and its last-used time advances once a minute, not per
 * event.
 */
class IngestAuthCache {
  private entries = new Map<string, Entry>();

  private cacheKey(token: string, siteId: number): string {
    // Never hold the credential itself in memory longer than the request.
    return `${siteId}:${crypto.createHash("sha256").update(token).digest("hex")}`;
  }

  private verify(key: string, entry: Entry | undefined, load: () => Promise<boolean>): Promise<boolean> {
    const pending = load()
      .then(trusted => {
        this.entries.delete(key);
        this.entries.set(key, { trusted, verifiedAt: Date.now() });
        this.evictIfOverCapacity();
        return trusted;
      })
      .catch(() => {
        // Leave whatever was cached in place and let the next event retry.
        const current = this.entries.get(key);
        if (current?.pending === pending) {
          if (entry) {
            current.pending = undefined;
          } else {
            this.entries.delete(key);
          }
        }
        return entry !== undefined && entry.trusted && Date.now() - entry.verifiedAt < GRANT_MAX_STALE_MS;
      });

    if (entry) {
      entry.pending = pending;
    } else {
      this.entries.set(key, { trusted: false, verifiedAt: 0, pending });
    }
    return pending;
  }

  private evictIfOverCapacity(): void {
    for (const key of this.entries.keys()) {
      if (this.entries.size <= MAX_ENTRIES) break;
      this.entries.delete(key);
    }
  }

  async isTrusted(token: string, siteId: number, load: () => Promise<boolean>): Promise<boolean> {
    const key = this.cacheKey(token, siteId);
    const entry = this.entries.get(key);

    if (!entry || entry.verifiedAt === 0) {
      return entry?.pending ?? this.verify(key, undefined, load);
    }

    const age = Date.now() - entry.verifiedAt;
    if (age < (entry.trusted ? GRANT_FRESH_MS : DENIAL_FRESH_MS)) {
      return entry.trusted;
    }

    const pending = entry.pending ?? this.verify(key, entry, load);
    // A stale grant answers immediately; the refresh lands for later events.
    return entry.trusted && age < GRANT_MAX_STALE_MS ? true : pending;
  }

  clear(): void {
    this.entries.clear();
  }
}

export const ingestAuthCache = new IngestAuthCache();
