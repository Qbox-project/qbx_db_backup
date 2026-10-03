import { createReadStream } from "node:fs";
import { copyFile, mkdir, mkdtemp, rename, rm } from "node:fs/promises";
import { request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";
import path from "node:path";

export type SpooledZip = {
  bytesZip: number;
  sha256: string;
};

export type SinkResult = SpooledZip & {
  location?: string;
};

export type OpenSink = {
  spoolPath: string;
  finish: (spooled: SpooledZip) => Promise<SinkResult>;
  abort: () => Promise<void>;
};

export type BackupSink = {
  open: () => Promise<OpenSink>;
};

export type UploadInfo = {
  sizeBytes: number;
  sha256: string;
};

export type UploadTarget = {
  url: string;
  headers?: Record<string, string>;
};

export type UploadSinkOptions = {
  resolveUpload: (info: UploadInfo) => Promise<UploadTarget>;
  maxBytes?: number;
  tmpDir: string;
  keepLocalPath?: string;
};

export class LocalFileSink implements BackupSink {
  constructor(private readonly filePath: string) {}

  async open(): Promise<OpenSink> {
    await mkdir(path.dirname(this.filePath), { recursive: true });
    const partPath = `${this.filePath}.part`;
    const filePath = this.filePath;
    return {
      spoolPath: partPath,
      finish: async (spooled) => {
        await rename(partPath, filePath);
        return { ...spooled, location: filePath };
      },
      abort: () => rm(partPath, { force: true }),
    };
  }
}

export class UploadSink implements BackupSink {
  constructor(private readonly options: UploadSinkOptions) {}

  async open(): Promise<OpenSink> {
    const root = this.options.tmpDir;
    await mkdir(root, { recursive: true });
    const dir = await mkdtemp(path.join(root, "qbx-db-backup-"));
    const spoolPath = path.join(dir, "backup.zip");
    const upload = this.options;
    return {
      spoolPath,
      finish: async (result) => {
        try {
          if (upload.maxBytes !== undefined && result.bytesZip > upload.maxBytes) {
            throw new Error(
              `Backup zip is ${result.bytesZip} bytes, over the ${upload.maxBytes} byte limit for this job`,
            );
          }
          const target = await upload.resolveUpload({
            sizeBytes: result.bytesZip,
            sha256: result.sha256,
          });
          await putFile(target.url, spoolPath, result.bytesZip, target.headers ?? {});
          const location = await keepLocalCopy(spoolPath, upload.keepLocalPath);
          return location === null ? result : { ...result, location };
        } finally {
          await rm(dir, { recursive: true, force: true });
        }
      },
      abort: () => rm(dir, { recursive: true, force: true }),
    };
  }
}

export async function keepLocalCopy(
  spoolPath: string,
  destination?: string,
): Promise<string | null> {
  if (destination === undefined || destination.length === 0) return null;
  await mkdir(path.dirname(destination), { recursive: true });
  const partPath = `${destination}.part`;
  await copyFile(spoolPath, partPath);
  await rename(partPath, destination);
  return destination;
}

/** S3/R2 presigned single PUT rejects chunked bodies, so the length must be known up front. */
function putFile(
  url: string,
  filePath: string,
  size: number,
  headers: Record<string, string>,
): Promise<void> {
  const target = new URL(url);
  const send = target.protocol === "http:" ? httpRequest : httpsRequest;
  return new Promise((resolve, reject) => {
    const req = send(
      target,
      {
        method: "PUT",
        headers: {
          "Content-Type": "application/zip",
          ...headers,
          "Content-Length": String(size),
        },
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (chunk: Buffer) => {
          if (chunks.length < 32) chunks.push(chunk);
        });
        res.on("error", reject);
        res.on("end", () => {
          const status = res.statusCode ?? 0;
          if (status >= 200 && status < 300) {
            resolve();
            return;
          }
          const body = Buffer.concat(chunks).toString("utf8").slice(0, 300);
          reject(new Error(`Upload failed with HTTP ${status}${body ? `: ${body}` : ""}`));
        });
      },
    );
    req.on("error", reject);
    const body = createReadStream(filePath);
    body.on("error", reject);
    body.pipe(req);
  });
}
