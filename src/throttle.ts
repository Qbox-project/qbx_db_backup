import { PassThrough, Transform } from "node:stream";

const BURST_ALLOWANCE_MS = 100;

export function createThrottle(bytesPerSecond: number): Transform {
  if (bytesPerSecond <= 0) return new PassThrough();

  const bytesPerMs = bytesPerSecond / 1000;
  let allowedAt = 0;
  let pending: NodeJS.Timeout | null = null;

  return new Transform({
    transform(chunk: Buffer, _encoding, callback) {
      const now = Date.now();
      // Time spent idle earns no credit beyond a short burst, so a stall is not followed by a flood.
      allowedAt = Math.max(allowedAt, now - BURST_ALLOWANCE_MS) + chunk.length / bytesPerMs;
      const wait = allowedAt - now;
      if (wait < 1) {
        callback(null, chunk);
        return;
      }
      pending = setTimeout(() => {
        pending = null;
        callback(null, chunk);
      }, wait);
    },
    destroy(failure, callback) {
      if (pending !== null) clearTimeout(pending);
      callback(failure);
    },
  });
}
