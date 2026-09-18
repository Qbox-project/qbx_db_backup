import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer, type IncomingHttpHeaders, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import path from "node:path";
import { Readable } from "node:stream";
import { parseListObjectsV2Xml, S3Client } from "./client";
import { sha256Hex } from "./signer";

type MockRequest = {
  method: string;
  url: string;
  headers: IncomingHttpHeaders;
  body: string;
};

describe("S3Client", () => {
  let server: Server;
  let mockPort: number;
  let receivedRequests: MockRequest[] = [];
  let mockHandler:
    | ((req: MockRequest) => { status: number; headers?: Record<string, string>; body: string })
    | null = null;

  beforeEach(async () => {
    receivedRequests = [];
    mockHandler = null;

    server = createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (c: Buffer) => chunks.push(c));
      req.on("end", () => {
        const bodyStr = Buffer.concat(chunks).toString("utf8");
        const mockReq: MockRequest = {
          method: req.method ?? "GET",
          url: req.url ?? "/",
          headers: req.headers,
          body: bodyStr,
        };
        receivedRequests.push(mockReq);

        const response = mockHandler ? mockHandler(mockReq) : { status: 200, body: "" };
        res.writeHead(response.status, response.headers ?? { "content-type": "application/xml" });
        res.end(response.body);
      });
    });

    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    mockPort = (server.address() as AddressInfo).port;
  });

  afterEach(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });

  it("constructs path-style and virtual-hosted URLs properly", () => {
    const r2Client = new S3Client({
      endpoint: "https://account123.r2.cloudflarestorage.com",
      bucket: "fivem-backups",
      accessKeyId: "KEY",
      secretAccessKey: "SECRET",
      region: "auto",
      forcePathStyle: true,
    });
    expect(r2Client.buildUrl("mydb/dump.zip").toString()).toBe(
      "https://account123.r2.cloudflarestorage.com/fivem-backups/mydb/dump.zip",
    );

    const awsClient = new S3Client({
      bucket: "my-aws-bucket",
      accessKeyId: "KEY",
      secretAccessKey: "SECRET",
      region: "us-west-2",
      forcePathStyle: false,
    });
    expect(awsClient.buildUrl("prod/backup.zip").toString()).toBe(
      "https://my-aws-bucket.s3.us-west-2.amazonaws.com/prod/backup.zip",
    );
  });

  it("streams putObject with SigV4 headers and payload hash", async () => {
    mockHandler = (req) => {
      expect(req.method).toBe("PUT");
      expect(req.url).toBe("/test-bucket/backups/dump.zip");
      expect(req.headers.authorization).toContain("AWS4-HMAC-SHA256");
      expect(req.headers["content-type"]).toBe("application/zip");
      expect(req.headers["x-amz-content-sha256"]).toBe(sha256Hex("test-content"));
      return { status: 200, headers: { etag: '"abc123etag"' }, body: "" };
    };

    const client = new S3Client({
      endpoint: `http://127.0.0.1:${mockPort}`,
      bucket: "test-bucket",
      accessKeyId: "TESTKEY",
      secretAccessKey: "TESTSECRET",
      region: "us-east-1",
      forcePathStyle: true,
    });

    const content = Buffer.from("test-content", "utf8");
    const result = await client.putObject(
      "backups/dump.zip",
      Readable.from([content]),
      content.length,
      sha256Hex(content),
    );

    expect(result.etag).toBe("abc123etag");
    expect(receivedRequests.length).toBe(1);
  });

  it("lists objects with ListObjectsV2 XML parsing", async () => {
    mockHandler = () => ({
      status: 200,
      body: `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Name>test-bucket</Name>
    <Prefix>backups/</Prefix>
    <KeyCount>2</KeyCount>
    <MaxKeys>1000</MaxKeys>
    <IsTruncated>false</IsTruncated>
    <Contents>
        <Key>backups/db-2026-09-09_12-00-00Z.zip</Key>
        <LastModified>2026-09-09T12:00:00.000Z</LastModified>
        <ETag>"123456"</ETag>
        <Size>1048576</Size>
    </Contents>
    <Contents>
        <Key>backups/db-2026-09-10_12-00-00Z.zip</Key>
        <LastModified>2026-09-10T12:00:00.000Z</LastModified>
        <ETag>"789012"</ETag>
        <Size>2097152</Size>
    </Contents>
</ListBucketResult>`,
    });

    const client = new S3Client({
      endpoint: `http://127.0.0.1:${mockPort}`,
      bucket: "test-bucket",
      accessKeyId: "TESTKEY",
      secretAccessKey: "TESTSECRET",
      region: "us-east-1",
      forcePathStyle: true,
    });

    const list = await client.listObjectsV2({ prefix: "backups/" });
    expect(list.isTruncated).toBe(false);
    expect(list.objects).toHaveLength(2);
    expect(list.objects[0]!.key).toBe("backups/db-2026-09-09_12-00-00Z.zip");
    expect(list.objects[0]!.sizeBytes).toBe(1048576);
    expect(list.objects[1]!.key).toBe("backups/db-2026-09-10_12-00-00Z.zip");
    expect(list.objects[1]!.sizeBytes).toBe(2097152);
  });

  it("deletes multiple objects in batch with DeleteObjects XML", async () => {
    mockHandler = (req) => {
      expect(req.method).toBe("POST");
      expect(req.url).toContain("delete");
      expect(req.body).toContain("<Key>backups/old1.zip</Key>");
      expect(req.body).toContain("<Key>backups/old2.zip</Key>");
      return {
        status: 200,
        body: `<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Deleted><Key>backups/old1.zip</Key></Deleted>
    <Deleted><Key>backups/old2.zip</Key></Deleted>
</DeleteResult>`,
      };
    };

    const client = new S3Client({
      endpoint: `http://127.0.0.1:${mockPort}`,
      bucket: "test-bucket",
      accessKeyId: "TESTKEY",
      secretAccessKey: "TESTSECRET",
      region: "us-east-1",
      forcePathStyle: true,
    });

    const result = await client.deleteObjects(["backups/old1.zip", "backups/old2.zip"]);
    expect(result.deletedKeys).toEqual(["backups/old1.zip", "backups/old2.zip"]);
    expect(result.errors).toHaveLength(0);
  });

  it("parses S3 error XML and throws S3Error with code and message", async () => {
    mockHandler = () => ({
      status: 403,
      body: `<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>AccessDenied</Code>
    <Message>Access Denied to bucket test-bucket</Message>
    <Resource>/test-bucket/file.zip</Resource>
</Error>`,
    });

    const client = new S3Client({
      endpoint: `http://127.0.0.1:${mockPort}`,
      bucket: "test-bucket",
      accessKeyId: "INVALID",
      secretAccessKey: "INVALID",
      region: "us-east-1",
      forcePathStyle: true,
    });

    await expect(
      client.putObject("file.zip", Readable.from([Buffer.from("a")]), 1, sha256Hex("a")),
    ).rejects.toThrow(/S3 Error \[AccessDenied\]: Access Denied/);
  });

  it("lets an upload that keeps sending data outlast timeoutMs", async () => {
    const client = new S3Client({
      endpoint: `http://127.0.0.1:${mockPort}`,
      bucket: "test-bucket",
      accessKeyId: "TESTKEY",
      secretAccessKey: "TESTSECRET",
      timeoutMs: 200,
    });

    const chunkCount = 8;
    let sent = 0;
    const slowBody = new Readable({
      read() {
        if (sent >= chunkCount) {
          this.push(null);
          return;
        }
        sent += 1;
        setTimeout(() => this.push(Buffer.alloc(16)), 60);
      },
    });

    await client.putObject("slow.zip", slowBody, chunkCount * 16, "UNSIGNED-PAYLOAD");
    expect(receivedRequests.length).toBe(1);
  });

  it("rejects when the server stops responding", async () => {
    const silent = createServer(() => {});
    await new Promise<void>((resolve) => silent.listen(0, "127.0.0.1", resolve));
    const client = new S3Client({
      endpoint: `http://127.0.0.1:${(silent.address() as AddressInfo).port}`,
      bucket: "test-bucket",
      accessKeyId: "TESTKEY",
      secretAccessKey: "TESTSECRET",
      timeoutMs: 100,
    });

    try {
      await expect(client.testConnection()).rejects.toThrow(/stalled for 100ms/);
    } finally {
      silent.closeAllConnections();
      await new Promise<void>((resolve) => silent.close(() => resolve()));
    }
  });

  describe("putFile", () => {
    const content = "0123456789abcdefghijKLMNO";
    let dir: string;
    let filePath: string;

    beforeEach(async () => {
      dir = await mkdtemp(path.join(tmpdir(), "qbx-s3-client-"));
      filePath = path.join(dir, "backup.zip");
      await writeFile(filePath, content);
    });

    afterEach(async () => {
      await rm(dir, { recursive: true, force: true });
    });

    const multipartClient = () =>
      new S3Client({
        endpoint: `http://127.0.0.1:${mockPort}`,
        bucket: "test-bucket",
        accessKeyId: "TESTKEY",
        secretAccessKey: "TESTSECRET",
        multipartThresholdBytes: 10,
        partSizeBytes: 10,
      });

    it("uses a single PUT at or below the threshold", async () => {
      const client = new S3Client({
        endpoint: `http://127.0.0.1:${mockPort}`,
        bucket: "test-bucket",
        accessKeyId: "TESTKEY",
        secretAccessKey: "TESTSECRET",
      });
      await client.putFile("small.zip", filePath, content.length, sha256Hex(content));
      expect(receivedRequests.map((r) => `${r.method} ${r.url}`)).toEqual([
        "PUT /test-bucket/small.zip",
      ]);
      expect(receivedRequests[0]?.body).toBe(content);
    });

    it("splits a large file into parts and completes the upload", async () => {
      mockHandler = (req) => {
        if (req.url.endsWith("?uploads=")) {
          return { status: 200, body: "<R><UploadId>up&amp;1</UploadId></R>" };
        }
        if (req.method === "PUT") {
          const part = new URL(req.url, "http://x").searchParams.get("partNumber");
          return { status: 200, headers: { etag: `"etag-${part}"` }, body: "" };
        }
        return { status: 200, body: "<R><ETag>&quot;final-3&quot;</ETag></R>" };
      };

      const result = await multipartClient().putFile(
        "big.zip",
        filePath,
        content.length,
        sha256Hex(content),
      );

      expect(result.etag).toBe("final-3");
      const parts = receivedRequests.filter((r) => r.method === "PUT");
      expect(parts.map((r) => r.body)).toEqual(["0123456789", "abcdefghij", "KLMNO"]);
      expect(parts.every((r) => r.url.includes("uploadId=up%261"))).toBe(true);
      expect(parts[2]?.headers["x-amz-content-sha256"]).toBe(sha256Hex("KLMNO"));
      const complete = receivedRequests.at(-1);
      expect(complete?.method).toBe("POST");
      expect(complete?.body).toContain(
        "<Part><PartNumber>2</PartNumber><ETag>&quot;etag-2&quot;</ETag></Part>",
      );
    });

    it("retries a part after a server error", async () => {
      let failuresLeft = 1;
      mockHandler = (req) => {
        if (req.url.endsWith("?uploads=")) {
          return { status: 200, body: "<R><UploadId>u1</UploadId></R>" };
        }
        if (req.method === "PUT" && failuresLeft > 0) {
          failuresLeft -= 1;
          return { status: 503, body: "<Error><Code>SlowDown</Code></Error>" };
        }
        if (req.method === "PUT") return { status: 200, headers: { etag: '"e"' }, body: "" };
        return { status: 200, body: "<R><ETag>done</ETag></R>" };
      };

      await multipartClient().putFile("big.zip", filePath, content.length, sha256Hex(content));
      expect(receivedRequests.filter((r) => r.method === "PUT")).toHaveLength(4);
    });

    it("aborts the upload when a part is rejected", async () => {
      mockHandler = (req) => {
        if (req.url.endsWith("?uploads=")) {
          return { status: 200, body: "<R><UploadId>u1</UploadId></R>" };
        }
        if (req.method === "PUT") {
          return { status: 403, body: "<Error><Code>AccessDenied</Code></Error>" };
        }
        return { status: 204, body: "" };
      };

      await expect(
        multipartClient().putFile("big.zip", filePath, content.length, sha256Hex(content)),
      ).rejects.toThrow(/AccessDenied/);
      expect(receivedRequests.map((r) => r.method)).toEqual(["POST", "PUT", "DELETE"]);
    });

    it("treats an error body on a 200 completion as a failure", async () => {
      mockHandler = (req) => {
        if (req.url.endsWith("?uploads=")) {
          return { status: 200, body: "<R><UploadId>u1</UploadId></R>" };
        }
        if (req.method === "PUT") return { status: 200, headers: { etag: '"e"' }, body: "" };
        if (req.method === "POST") {
          return { status: 200, body: "<Error><Code>InternalError</Code></Error>" };
        }
        return { status: 204, body: "" };
      };

      await expect(
        multipartClient().putFile("big.zip", filePath, content.length, sha256Hex(content)),
      ).rejects.toThrow(/InternalError/);
      expect(receivedRequests.at(-1)?.method).toBe("DELETE");
    });
  });

  it("decodes XML entities in listed keys", () => {
    const listed = parseListObjectsV2Xml(
      "<R><Contents><Key>a &amp; b/db-1.zip</Key><LastModified>2026-09-01T00:00:00Z</LastModified><Size>5</Size></Contents></R>",
    );
    expect(listed.objects[0]?.key).toBe("a & b/db-1.zip");
  });

  it("signs keys that need percent-encoding", async () => {
    const client = new S3Client({
      endpoint: `http://127.0.0.1:${mockPort}`,
      bucket: "test-bucket",
      accessKeyId: "TESTKEY",
      secretAccessKey: "TESTSECRET",
    });
    await client.putObject("my backups/db #1.zip", Buffer.from("a"), 1, sha256Hex("a"));
    expect(receivedRequests[0]?.url).toBe("/test-bucket/my%20backups/db%20%231.zip");
  });
});
