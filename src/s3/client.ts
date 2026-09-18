import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { open } from "node:fs/promises";
import { request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";
import { Readable } from "node:stream";
import { sha256Hex, signS3Request, uriEncode } from "./signer";
import type {
  S3ClientOptions,
  S3DeleteResult,
  S3ListResult,
  S3ObjectInfo,
  S3PutOptions,
  S3PutResult,
} from "./types";

export class S3Error extends Error {
  constructor(
    public readonly code: string,
    message: string,
    public readonly statusCode: number,
    public readonly rawBody?: string,
  ) {
    super(`S3 Error [${code}]: ${message}`);
    this.name = "S3Error";
  }
}

const DEFAULT_TIMEOUT_MS = 60_000;
const MIB = 1024 * 1024;
const DEFAULT_MULTIPART_THRESHOLD_BYTES = 100 * MIB;
const DEFAULT_PART_SIZE_BYTES = 32 * MIB;
const MAX_PARTS = 10_000;
const PART_ATTEMPTS = 3;
const MAX_RESPONSE_BYTES = 16 * MIB;
const BODY_SLICE_BYTES = 64 * 1024;
const HASH_SLICE_BYTES = MIB;

export class S3Client {
  public readonly bucket: string;
  public readonly region: string;
  public readonly endpoint?: string;
  public readonly forcePathStyle: boolean;
  private readonly accessKeyId: string;
  private readonly secretAccessKey: string;
  private readonly timeoutMs: number;
  private readonly multipartThresholdBytes: number;
  private readonly partSizeBytes: number;

  constructor(options: S3ClientOptions) {
    if (!options.bucket || options.bucket.trim().length === 0) {
      throw new Error("S3 bucket name is required");
    }
    if (!options.accessKeyId || options.accessKeyId.trim().length === 0) {
      throw new Error("S3 access key ID is required");
    }
    if (!options.secretAccessKey || options.secretAccessKey.trim().length === 0) {
      throw new Error("S3 secret access key is required");
    }

    this.bucket = options.bucket.trim();
    this.accessKeyId = options.accessKeyId.trim();
    this.secretAccessKey = options.secretAccessKey.trim();
    this.region = options.region?.trim() || "us-east-1";
    this.endpoint = options.endpoint?.trim() || undefined;
    this.timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    this.multipartThresholdBytes =
      options.multipartThresholdBytes ?? DEFAULT_MULTIPART_THRESHOLD_BYTES;
    this.partSizeBytes = options.partSizeBytes ?? DEFAULT_PART_SIZE_BYTES;

    // By default, custom endpoints use path style (e.g. MinIO, R2), while AWS standard uses virtual-hosted
    this.forcePathStyle =
      options.forcePathStyle ?? (this.endpoint !== undefined && this.endpoint.length > 0);
  }

  public buildUrl(key = "", query?: Record<string, string>): URL {
    let baseUrl: string;
    const cleanKey = uriEncode(key.replace(/^\/+/, ""), false);

    if (this.endpoint) {
      const ep = this.endpoint.replace(/\/+$/, "");
      if (this.forcePathStyle) {
        baseUrl = cleanKey.length > 0 ? `${ep}/${this.bucket}/${cleanKey}` : `${ep}/${this.bucket}`;
      } else {
        const parsed = new URL(ep);
        baseUrl = `${parsed.protocol}//${this.bucket}.${parsed.host}${parsed.pathname.replace(/\/+$/, "")}/${cleanKey}`;
      }
    } else {
      if (this.forcePathStyle) {
        baseUrl =
          cleanKey.length > 0
            ? `https://s3.${this.region}.amazonaws.com/${this.bucket}/${cleanKey}`
            : `https://s3.${this.region}.amazonaws.com/${this.bucket}`;
      } else {
        baseUrl =
          cleanKey.length > 0
            ? `https://${this.bucket}.s3.${this.region}.amazonaws.com/${cleanKey}`
            : `https://${this.bucket}.s3.${this.region}.amazonaws.com/`;
      }
    }

    const url = new URL(baseUrl);
    if (query) {
      for (const [k, v] of Object.entries(query)) {
        if (v !== undefined) {
          url.searchParams.set(k, v);
        }
      }
    }
    return url;
  }

  public async putObject(
    key: string,
    body: Readable | Buffer,
    sizeBytes: number,
    payloadSha256: string,
    options?: S3PutOptions,
  ): Promise<S3PutResult> {
    const url = this.buildUrl(key);
    const contentType = options?.contentType ?? "application/zip";

    const customHeaders: Record<string, string> = {
      "content-type": contentType,
      "content-length": String(sizeBytes),
    };

    if (options?.metadata) {
      for (const [mKey, mVal] of Object.entries(options.metadata)) {
        customHeaders[`x-amz-meta-${mKey.toLowerCase()}`] = mVal;
      }
    }

    const signed = signS3Request({
      method: "PUT",
      url,
      headers: customHeaders,
      payloadHash: payloadSha256,
      accessKeyId: this.accessKeyId,
      secretAccessKey: this.secretAccessKey,
      region: this.region,
    });

    const response = await this.executeRequest(signed, body);
    const etag = (response.headers.etag as string | undefined)?.replace(/"/g, "") ?? "";
    return { etag };
  }

  /** A single PUT is capped at 5 GB by S3 and restarts from zero on any network blip. */
  public async putFile(
    key: string,
    filePath: string,
    sizeBytes: number,
    payloadSha256: string,
  ): Promise<S3PutResult> {
    if (sizeBytes <= this.multipartThresholdBytes) {
      return this.putObject(key, createReadStream(filePath), sizeBytes, payloadSha256);
    }

    const partSize = Math.max(this.partSizeBytes, Math.ceil(sizeBytes / MAX_PARTS));
    const uploadId = await this.createMultipartUpload(key);
    const file = await open(filePath, "r");
    try {
      const parts: UploadedPart[] = [];
      for (let offset = 0; offset < sizeBytes; offset += partSize) {
        const length = Math.min(partSize, sizeBytes - offset);
        const chunk = Buffer.alloc(length);
        const { bytesRead } = await file.read(chunk, 0, length, offset);
        if (bytesRead !== length) {
          throw new Error(`Backup zip changed size during upload (read ${bytesRead} of ${length})`);
        }
        const partNumber = parts.length + 1;
        parts.push({ partNumber, etag: await this.uploadPart(key, uploadId, partNumber, chunk) });
      }
      return await this.completeMultipartUpload(key, uploadId, parts);
    } catch (failure) {
      await this.abortMultipartUpload(key, uploadId).catch(() => {});
      throw failure;
    } finally {
      await file.close();
    }
  }

  private async createMultipartUpload(key: string): Promise<string> {
    const response = await this.send("POST", key, { uploads: "" }, undefined, {
      "content-type": "application/zip",
    });
    const uploadId = /<UploadId>(.*?)<\/UploadId>/s.exec(response.body)?.[1]?.trim();
    if (!uploadId) throw new Error("S3 did not return an UploadId for the multipart upload");
    return unescapeXml(uploadId);
  }

  private async uploadPart(
    key: string,
    uploadId: string,
    partNumber: number,
    chunk: Buffer,
  ): Promise<string> {
    const query = { partNumber: String(partNumber), uploadId };
    for (let attempt = 1; ; attempt += 1) {
      try {
        const response = await this.send("PUT", key, query, chunk);
        const etag = response.headers.etag;
        if (typeof etag !== "string" || etag.length === 0) {
          throw new Error(`S3 did not return an ETag for part ${partNumber}`);
        }
        return etag;
      } catch (failure) {
        const rejected =
          failure instanceof S3Error && failure.statusCode >= 400 && failure.statusCode < 500;
        if (rejected || attempt >= PART_ATTEMPTS) throw failure;
      }
    }
  }

  private async completeMultipartUpload(
    key: string,
    uploadId: string,
    parts: UploadedPart[],
  ): Promise<S3PutResult> {
    const partsXml = parts
      .map(
        (part) =>
          `<Part><PartNumber>${part.partNumber}</PartNumber><ETag>${escapeXml(part.etag)}</ETag></Part>`,
      )
      .join("");
    const body = Buffer.from(
      `<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUpload>${partsXml}</CompleteMultipartUpload>`,
      "utf8",
    );
    const response = await this.send("POST", key, { uploadId }, body, {
      "content-type": "application/xml",
    });
    // S3 can answer 200 and still report a failure in the body.
    if (response.body.includes("<Error>")) {
      throw parseS3ErrorXml(response.body, response.statusCode);
    }
    const etag = /<ETag>(.*?)<\/ETag>/s.exec(response.body)?.[1]?.trim() ?? "";
    return { etag: unescapeXml(etag).replace(/"/g, "") };
  }

  private async abortMultipartUpload(key: string, uploadId: string): Promise<void> {
    await this.send("DELETE", key, { uploadId });
  }

  private async send(
    method: string,
    key: string,
    query: Record<string, string>,
    body?: Buffer,
    headers: Record<string, string> = {},
  ) {
    const signed = signS3Request({
      method,
      url: this.buildUrl(key, query),
      headers: body ? { ...headers, "content-length": String(body.length) } : headers,
      payloadHash: body ? await hashInSlices(body) : sha256Hex(""),
      accessKeyId: this.accessKeyId,
      secretAccessKey: this.secretAccessKey,
      region: this.region,
    });
    return this.executeRequest(signed, body);
  }

  public async listObjectsV2(options?: {
    prefix?: string;
    continuationToken?: string;
    maxKeys?: number;
  }): Promise<S3ListResult> {
    const query: Record<string, string> = { "list-type": "2" };
    if (options?.prefix) query.prefix = options.prefix;
    if (options?.continuationToken) query["continuation-token"] = options.continuationToken;
    if (options?.maxKeys) query["max-keys"] = String(options.maxKeys);

    const url = this.buildUrl("", query);

    const signed = signS3Request({
      method: "GET",
      url,
      payloadHash: sha256Hex(""),
      accessKeyId: this.accessKeyId,
      secretAccessKey: this.secretAccessKey,
      region: this.region,
    });

    const response = await this.executeRequest(signed);
    return parseListObjectsV2Xml(response.body);
  }

  public async deleteObjects(keys: string[]): Promise<S3DeleteResult> {
    if (keys.length === 0) {
      return { deletedKeys: [], errors: [] };
    }

    const objectsXml = keys.map((k) => `<Object><Key>${escapeXml(k)}</Key></Object>`).join("");
    const xmlBody = `<?xml version="1.0" encoding="UTF-8"?><Delete><Quiet>false</Quiet>${objectsXml}</Delete>`;
    const bodyBuffer = Buffer.from(xmlBody, "utf8");
    const payloadHash = sha256Hex(bodyBuffer);

    const url = this.buildUrl("", { delete: "" });

    const signed = signS3Request({
      method: "POST",
      url,
      headers: {
        "content-type": "application/xml",
        "content-length": String(bodyBuffer.length),
        // AWS rejects multi-object deletes without it (MissingContentMD5).
        "content-md5": createHash("md5").update(bodyBuffer).digest("base64"),
      },
      payloadHash,
      accessKeyId: this.accessKeyId,
      secretAccessKey: this.secretAccessKey,
      region: this.region,
    });

    const response = await this.executeRequest(signed, bodyBuffer);
    return parseDeleteResultXml(response.body, keys);
  }

  public async testConnection(): Promise<{ ok: true; bucket: string; region: string }> {
    // Perform a lightweight ListObjectsV2 with max-keys=1 to verify credentials and bucket access
    await this.listObjectsV2({ maxKeys: 1 });
    return { ok: true, bucket: this.bucket, region: this.region };
  }

  private executeRequest(
    signed: { method: string; url: string; headers: Record<string, string> },
    body?: Readable | Buffer,
  ): Promise<{
    statusCode: number;
    headers: Record<string, string | string[] | undefined>;
    body: string;
  }> {
    const target = new URL(signed.url);
    const send = target.protocol === "http:" ? httpRequest : httpsRequest;

    return new Promise((resolve, reject) => {
      // Inactivity, not total duration: a large upload legitimately outlasts timeoutMs.
      const idle = setTimeout(() => {
        const stalled = new Error(`S3 request stalled for ${this.timeoutMs}ms`);
        req.destroy(stalled);
        reject(stalled);
      }, this.timeoutMs);
      const settle =
        <T>(finish: (value: T) => void) =>
        (value: T) => {
          clearTimeout(idle);
          finish(value);
        };
      const fail = settle(reject);

      const req = send(
        target,
        {
          method: signed.method,
          headers: signed.headers,
        },
        (res) => {
          const chunks: Buffer[] = [];
          let received = 0;
          res.on("data", (chunk: Buffer) => {
            idle.refresh();
            received += chunk.length;
            if (received > MAX_RESPONSE_BYTES) {
              const oversized = new Error(`S3 response exceeded ${MAX_RESPONSE_BYTES} bytes`);
              req.destroy(oversized);
              fail(oversized);
              return;
            }
            chunks.push(chunk);
          });
          res.on("error", fail);
          res.on("end", () => {
            const statusCode = res.statusCode ?? 0;
            const resBody = Buffer.concat(chunks).toString("utf8");

            if (statusCode >= 200 && statusCode < 300) {
              settle(resolve)({ statusCode, headers: res.headers, body: resBody });
              return;
            }

            fail(parseS3ErrorXml(resBody, statusCode));
          });
        },
      );

      req.on("error", fail);

      if (body) {
        // Buffers go out in slices so a slow link still shows progress to the idle timer.
        const source = Buffer.isBuffer(body) ? Readable.from(sliceBuffer(body)) : body;
        source.on("error", (streamErr) => {
          req.destroy(streamErr);
          fail(streamErr);
        });
        source.on("data", () => idle.refresh());
        source.pipe(req);
      } else {
        req.end();
      }
    });
  }
}

type UploadedPart = { partNumber: number; etag: string };

// Hashing a whole part in one call would stall the game server's main thread.
async function hashInSlices(buffer: Buffer): Promise<string> {
  const hash = createHash("sha256");
  for (let offset = 0; offset < buffer.length; offset += HASH_SLICE_BYTES) {
    if (offset > 0) await new Promise((resolve) => setImmediate(resolve));
    hash.update(buffer.subarray(offset, offset + HASH_SLICE_BYTES));
  }
  return hash.digest("hex");
}

function* sliceBuffer(buffer: Buffer): Generator<Buffer> {
  for (let offset = 0; offset < buffer.length; offset += BODY_SLICE_BYTES) {
    yield buffer.subarray(offset, offset + BODY_SLICE_BYTES);
  }
}

const XML_ENTITIES: Record<string, string> = {
  amp: "&",
  lt: "<",
  gt: ">",
  quot: '"',
  apos: "'",
};

export function unescapeXml(str: string): string {
  return str.replace(/&(?:#x([0-9a-f]+)|#(\d+)|(\w+));/gi, (entity, hex, decimal, name) => {
    if (hex !== undefined) return String.fromCodePoint(Number.parseInt(hex, 16));
    if (decimal !== undefined) return String.fromCodePoint(Number.parseInt(decimal, 10));
    return XML_ENTITIES[name.toLowerCase()] ?? entity;
  });
}

function decodeText(raw: string | undefined): string | undefined {
  return raw === undefined ? undefined : unescapeXml(raw.trim());
}

export function escapeXml(str: string): string {
  return str
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

export function parseS3ErrorXml(xml: string, statusCode: number): S3Error {
  const codeMatch = /<Code>(.*?)<\/Code>/s.exec(xml);
  const messageMatch = /<Message>(.*?)<\/Message>/s.exec(xml);

  const code = codeMatch?.[1]?.trim() ?? `HTTP_${statusCode}`;
  const message =
    decodeText(messageMatch?.[1]) ??
    (xml.slice(0, 200).trim() || `Request failed with HTTP status ${statusCode}`);

  return new S3Error(code, message, statusCode, xml);
}

export function parseListObjectsV2Xml(xml: string): S3ListResult {
  const isTruncated =
    /<IsTruncated>(true|false)<\/IsTruncated>/i.exec(xml)?.[1]?.toLowerCase() === "true";
  const nextToken = decodeText(
    /<NextContinuationToken>(.*?)<\/NextContinuationToken>/s.exec(xml)?.[1],
  );

  const objects: S3ObjectInfo[] = [];
  const contentsRegex = /<Contents>(.*?)<\/Contents>/gs;
  let match: RegExpExecArray | null = null;

  while (true) {
    match = contentsRegex.exec(xml);
    if (!match) break;
    const itemXml = match[1] ?? "";

    const key = decodeText(/<Key>(.*?)<\/Key>/s.exec(itemXml)?.[1]);
    const lastModifiedRaw = /<LastModified>(.*?)<\/LastModified>/s.exec(itemXml)?.[1]?.trim();
    const sizeRaw = /<Size>(\d+)<\/Size>/s.exec(itemXml)?.[1]?.trim();
    const etag = (
      / <ETag>(.*?)<\/ETag>/s.exec(itemXml)?.[1] ??
      /<ETag>(.*?)<\/ETag>/s.exec(itemXml)?.[1]?.trim() ??
      ""
    ).replace(/"/g, "");

    if (key && lastModifiedRaw) {
      objects.push({
        key,
        lastModified: new Date(lastModifiedRaw),
        sizeBytes: sizeRaw ? Number.parseInt(sizeRaw, 10) : 0,
        etag,
      });
    }
  }

  return {
    objects,
    isTruncated,
    nextContinuationToken: nextToken,
  };
}

export function parseDeleteResultXml(xml: string, fallbackKeys: string[]): S3DeleteResult {
  const deletedKeys: string[] = [];
  const errors: { key: string; code: string; message: string }[] = [];

  const deletedRegex = /<Deleted>(.*?)<\/Deleted>/gs;
  let dMatch: RegExpExecArray | null = null;
  while (true) {
    dMatch = deletedRegex.exec(xml);
    if (!dMatch) break;
    const k = decodeText(/<Key>(.*?)<\/Key>/s.exec(dMatch[1] ?? "")?.[1]);
    if (k) deletedKeys.push(k);
  }

  const errorRegex = /<Error>(.*?)<\/Error>/gs;
  let eMatch: RegExpExecArray | null = null;
  while (true) {
    eMatch = errorRegex.exec(xml);
    if (!eMatch) break;
    const eXml = eMatch[1] ?? "";
    const k = decodeText(/<Key>(.*?)<\/Key>/s.exec(eXml)?.[1]) ?? "";
    const code = /<Code>(.*?)<\/Code>/s.exec(eXml)?.[1]?.trim() ?? "Unknown";
    const msg = decodeText(/<Message>(.*?)<\/Message>/s.exec(eXml)?.[1]) ?? "";
    errors.push({ key: k, code, message: msg });
  }

  // If the S3 provider returned empty response on 200 OK without errors, all requested keys were deleted
  if (deletedKeys.length === 0 && errors.length === 0) {
    return { deletedKeys: [...fallbackKeys], errors: [] };
  }

  return { deletedKeys, errors };
}
