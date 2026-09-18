import { describe, expect, it } from "bun:test";
import { PassThrough, Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { createThrottle } from "./throttle";

async function drain(
  source: Readable,
  bytesPerSecond: number,
): Promise<{ bytes: number; ms: number }> {
  const started = Date.now();
  let bytes = 0;
  const throttle = createThrottle(bytesPerSecond);
  throttle.on("data", (chunk: Buffer) => {
    bytes += chunk.length;
  });
  await pipeline(source, throttle);
  return { bytes, ms: Date.now() - started };
}

const chunks = (count: number, size: number) =>
  Readable.from(Array.from({ length: count }, () => Buffer.alloc(size)));

describe("createThrottle", () => {
  it("passes data straight through when unlimited", async () => {
    expect(createThrottle(0)).toBeInstanceOf(PassThrough);
    const result = await drain(chunks(50, 64 * 1024), 0);
    expect(result.bytes).toBe(50 * 64 * 1024);
    expect(result.ms).toBeLessThan(200);
  });

  it("holds the stream to the configured rate without losing data", async () => {
    const result = await drain(chunks(40, 16 * 1024), 1024 * 1024);
    expect(result.bytes).toBe(40 * 16 * 1024);
    expect(result.ms).toBeGreaterThanOrEqual(450);
    expect(result.ms).toBeLessThan(1500);
  });

  it("stops waiting when destroyed", async () => {
    const throttle = createThrottle(1);
    throttle.write(Buffer.alloc(1024));
    throttle.destroy();
    await new Promise((resolve) => throttle.once("close", resolve));
    expect(throttle.destroyed).toBe(true);
  });
});
