import { createHash } from "node:crypto";
import { createWriteStream } from "node:fs";
import { rm } from "node:fs/promises";
import { type Readable, Transform } from "node:stream";
import { ZipArchive } from "archiver";
import { type AttemptState, type DumpBinary, type DumpTarget, runDump } from "./dump";
import { createThrottle } from "./throttle";

export type SpoolJob = {
  target: DumpTarget;
  binary: DumpBinary;
  attempt: AttemptState;
  path: string;
  entryName: string;
  zipLevel: number;
  maxMbPerSecond: number;
};

export type SpoolProgress = {
  bytesSql: number;
  bytesZip: number;
};

export type SpoolResult = SpoolProgress & {
  sha256: string;
  warnings: string[];
};

export type SpoolOptions = {
  entryName: string;
  zipLevel: number;
  onZipBytes?: (total: number) => void;
};

export type Spool = {
  append: (source: Readable) => void;
  finish: () => Promise<{ bytesZip: number; sha256: string }>;
  abort: () => Promise<void>;
  failed: Promise<never>;
};

export function spoolDump(
  job: SpoolJob,
  onProgress: (progress: SpoolProgress) => void,
): { done: Promise<SpoolResult>; kill: () => void } {
  const progress: SpoolProgress = { bytesSql: 0, bytesZip: 0 };
  const spool = createSpool(job.path, {
    entryName: job.entryName,
    zipLevel: job.zipLevel,
    onZipBytes: (total) => {
      progress.bytesZip = total;
      onProgress(progress);
    },
  });
  const dump = runDump(job.target, { binary: job.binary, ...job.attempt });
  const paced = createThrottle(job.maxMbPerSecond * 1024 * 1024);
  dump.stdout.on("data", (chunk: Buffer) => {
    progress.bytesSql += chunk.length;
    onProgress(progress);
  });
  dump.stdout.pipe(paced);
  spool.append(paced);

  const done = (async () => {
    try {
      const { warnings } = await Promise.race([dump.done, spool.failed]);
      const written = await spool.finish();
      return { bytesSql: progress.bytesSql, ...written, warnings };
    } catch (failure) {
      dump.kill();
      await spool.abort().catch(() => {});
      throw failure;
    }
  })();

  return { done, kill: dump.kill };
}

export function createSpool(target: string, options: SpoolOptions): Spool {
  const zip = new ZipArchive({ zlib: { level: options.zipLevel }, forceZip64: true });
  const counter = createCounter(options.onZipBytes);
  const file = createWriteStream(target);
  let source: Readable | null = null;

  const flushed = new Promise<void>((resolve, reject) => {
    file.once("close", () => resolve());
    file.once("error", reject);
    counter.transform.once("error", reject);
    zip.on("error", (zipError: Error) => reject(new Error(`Zip failed: ${zipError.message}`)));
    zip.on("warning", (warning: Error & { code?: string }) => {
      if (warning.code !== "ENOENT") reject(new Error(`Zip warning: ${warning.message}`));
    });
  });

  const failed = new Promise<never>((_, reject) => {
    flushed.catch((flushError: Error) => {
      source?.destroy(flushError);
      reject(flushError);
    });
  });
  void failed.catch(() => {});

  zip.pipe(counter.transform).pipe(file);

  return {
    append: (input) => {
      source = input;
      zip.append(input, { name: options.entryName });
    },
    finish: async () => {
      await zip.finalize();
      await flushed;
      return { bytesZip: counter.bytes(), sha256: counter.digest() };
    },
    abort: async () => {
      zip.abort();
      counter.transform.destroy();
      file.destroy();
      await rm(target, { force: true });
    },
    failed,
  };
}

function createCounter(onBytes: ((total: number) => void) | undefined) {
  const hash = createHash("sha256");
  let total = 0;
  const transform = new Transform({
    transform(chunk: Buffer, _encoding, callback) {
      total += chunk.length;
      hash.update(chunk);
      onBytes?.(total);
      callback(null, chunk);
    },
  });
  return { transform, bytes: () => total, digest: () => hash.digest("hex") };
}
