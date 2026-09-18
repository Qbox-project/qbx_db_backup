import { mkdir, mkdtemp, rm } from "node:fs/promises";
import path from "node:path";
import {
  type BackupSink,
  createSpool,
  keepLocalCopy,
  type OpenSink,
  type SinkOpenOptions,
} from "../zip-sink";
import type { S3Client } from "./client";

export type S3SinkOptions = {
  client: S3Client;
  s3Key: string;
  tmpDir: string;
  keepLocalPath?: string;
  maxBytes?: number;
};

export class S3Sink implements BackupSink {
  constructor(private readonly options: S3SinkOptions) {}

  async open(options: SinkOpenOptions): Promise<OpenSink> {
    const root = this.options.tmpDir;
    await mkdir(root, { recursive: true });
    const dir = await mkdtemp(path.join(root, "qbx-s3-sink-"));
    const spoolPath = path.join(dir, "backup.zip");
    const spool = createSpool(spoolPath, options, this.options.maxBytes);
    const opts = this.options;

    return {
      append: spool.append,
      failed: spool.failed,
      finish: async () => {
        try {
          const result = await spool.finish();
          if (opts.maxBytes !== undefined && result.bytesZip > opts.maxBytes) {
            throw new Error(
              `Backup zip is ${result.bytesZip} bytes, exceeding the ${opts.maxBytes} byte limit`,
            );
          }

          await opts.client.putFile(opts.s3Key, spoolPath, result.bytesZip, result.sha256);

          const localCopy = await keepLocalCopy(spoolPath, opts.keepLocalPath);
          const s3Location = `s3://${opts.client.bucket}/${opts.s3Key}`;
          return localCopy === null
            ? { ...result, location: s3Location }
            : { ...result, location: `${s3Location} (+ local: ${localCopy})` };
        } finally {
          await rm(dir, { recursive: true, force: true });
        }
      },
      abort: async () => {
        await spool.abort();
        await rm(dir, { recursive: true, force: true });
      },
    };
  }
}
