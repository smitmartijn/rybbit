import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ingestAuthCache } from "./ingestAuthCache.js";

const MINUTE = 60 * 1000;

describe("ingestAuthCache", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    ingestAuthCache.clear();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("keeps decisions apart per token and per Site", async () => {
    const load = vi.fn().mockResolvedValueOnce(true).mockResolvedValueOnce(false).mockResolvedValueOnce(false);

    expect(await ingestAuthCache.isTrusted("key_a", 1, load)).toBe(true);
    expect(await ingestAuthCache.isTrusted("key_a", 2, load)).toBe(false);
    expect(await ingestAuthCache.isTrusted("key_b", 1, load)).toBe(false);
    expect(load).toHaveBeenCalledTimes(3);
  });

  it("serves a stale grant immediately while re-verifying in the background", async () => {
    await ingestAuthCache.isTrusted("key", 1, async () => true);
    vi.advanceTimersByTime(MINUTE + 1);

    let finishRefresh!: (trusted: boolean) => void;
    const refresh = vi.fn(() => new Promise<boolean>(resolve => (finishRefresh = resolve)));

    // Answered without waiting on the unresolved refresh, which is shared.
    expect(await ingestAuthCache.isTrusted("key", 1, refresh)).toBe(true);
    expect(await ingestAuthCache.isTrusted("key", 1, refresh)).toBe(true);
    expect(refresh).toHaveBeenCalledOnce();

    // A revoked key stops being trusted once the refresh lands.
    finishRefresh(false);
    await vi.advanceTimersByTimeAsync(0);
    expect(await ingestAuthCache.isTrusted("key", 1, refresh)).toBe(false);
  });

  it("keeps serving a grant when re-verification fails", async () => {
    await ingestAuthCache.isTrusted("key", 1, async () => true);
    vi.advanceTimersByTime(MINUTE + 1);

    const failing = vi.fn().mockRejectedValue(new Error("postgres down"));
    expect(await ingestAuthCache.isTrusted("key", 1, failing)).toBe(true);
    await vi.advanceTimersByTimeAsync(0);

    // The failure cleared the in-flight slot, so the next event retries.
    expect(await ingestAuthCache.isTrusted("key", 1, failing)).toBe(true);
    expect(failing).toHaveBeenCalledTimes(2);
  });

  it("waits for a real answer once a grant is too old to serve stale", async () => {
    await ingestAuthCache.isTrusted("key", 1, async () => true);
    vi.advanceTimersByTime(10 * MINUTE + 1);

    expect(await ingestAuthCache.isTrusted("key", 1, async () => false)).toBe(false);
  });

  it("re-checks a denial after a few seconds", async () => {
    const load = vi.fn().mockResolvedValueOnce(false).mockResolvedValueOnce(true);

    expect(await ingestAuthCache.isTrusted("key", 1, load)).toBe(false);
    expect(await ingestAuthCache.isTrusted("key", 1, load)).toBe(false);
    expect(load).toHaveBeenCalledOnce();

    vi.advanceTimersByTime(5001);
    expect(await ingestAuthCache.isTrusted("key", 1, load)).toBe(true);
  });

  it("does not cache a first verification that threw", async () => {
    const load = vi.fn().mockRejectedValueOnce(new Error("postgres down")).mockResolvedValueOnce(true);

    expect(await ingestAuthCache.isTrusted("key", 1, load)).toBe(false);
    expect(await ingestAuthCache.isTrusted("key", 1, load)).toBe(true);
  });
});
