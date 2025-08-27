# Async HTTP → S3 CSV Streamer (Lambda) — README

This Lambda downloads one or more CSV files **directly from HTTP(S)** and streams them to **Amazon S3** using **multipart upload**, with structured JSON logging and a built-in **S3 request counter**.

- **Language/runtime:** Python 3.11+  
- **I/O:** HTTP(S) → S3 (same bucket hosts the config)  
- **Concurrency:** Async (aiohttp) + configurable semaphore  
- **Resilience:** Safe aborts, zero-byte edge cases handled, progress logs  
- **Observability:** JSON logs + per-operation S3 counters

---

## How it works (high level)

1. On invoke, the function loads `s3://<BUCKET_NAME>/config/parameters.json` to get a list of `file_urls`.
2. For each URL, it chooses a filename via `SUFFIX_MAP` (or falls back to the URL’s basename).
3. It streams the response in **HTTP chunks** (configurable), buffers up to **S3 part size** (configurable), and uploads parts asynchronously via **multipart upload**.
4. If no bytes arrive, it **aborts the multipart** and writes an **empty object** instead.
5. It writes each file to:  
   `s3://<BUCKET_NAME>/<PATH_PREFIX>/<YYYY>/<MM>/<DD>/<suffix>.csv` (UTC date).
6. On completion, it returns a JSON summary and logs **S3 request counters**.

---

## Quick start

### 1) Create `parameters.json` in S3

```json
{
  "file_urls": [
    "https://example.com/datasets/rent_contracts.csv",
    "https://example.com/datasets/transactions.csv"
  ]
}
```

Upload to `s3://YOUR_BUCKET/config/parameters.json`.

### 2) Set environment variables

| Name           | Required | Default | Notes |
|----------------|----------|---------|-------|
| `BUCKET_NAME`  | ✅       | —       | Destination bucket AND where `config/parameters.json` lives. |
| `PATH_PREFIX`  | ❌       | `raw`   | Top-level S3 prefix for outputs. |
| `CONCURRENCY`  | ❌       | `2`     | Max simultaneous downloads (async semaphore). |
| `S3_PART_MB`   | ❌       | `50`    | Multipart **part** size in MB (min 5). |
| `HTTP_CHUNK_KB`| ❌       | `1024`  | Socket read chunk size (KB) fed into the part buffer. |
| `LOG_LEVEL`    | ❌       | `INFO`  | `DEBUG`/`INFO`/`WARNING`/`ERROR`. |

### 3) Required IAM permissions

Grant the Lambda **at least**:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadParametersJson",
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": ["arn:aws:s3:::YOUR_BUCKET/config/parameters.json"]
    },
    {
      "Sid": "WriteOutputs",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:AbortMultipartUpload",
        "s3:CreateMultipartUpload",
        "s3:CompleteMultipartUpload",
        "s3:UploadPart"
      ],
      "Resource": ["arn:aws:s3:::YOUR_BUCKET/*"]
    }
  ]
}
```

> Note: `boto3` is provided by Lambda. You must package **`aiohttp`** with your function code or via a Lambda layer.

---

## Deployment

### Option A — Zip (classic)
1. Create `requirements.txt`:
   ```
   aiohttp==3.9.5
   ```
2. Build in a manylinux-compatible env:
   ```bash
   mkdir -p package
   pip install --target package -r requirements.txt
   cp lambda_function.py package/  # rename your file to lambda_function.py or adjust handler
   (cd package && zip -r ../function.zip .)
   ```
3. Create/Update Lambda with handler `lambda_function.lambda_handler`, runtime Python 3.11, attach the IAM policy, and set Env Vars above.

### Option B — Container
- Use the AWS Python 3.11 base image, `pip install aiohttp`, copy code, set CMD to the handler.

### Recommended Lambda settings
- **Timeout:** Start with 10 minutes (600 s) for multi-file large streams.
- **Memory:** 1024–2048 MB (tunes network throughput).
- **Ephemeral storage:** Default (512 MB) is fine; buffering is in-memory.

> **VPC note:** If you place the Lambda in a VPC, make sure it has outbound internet (NAT gateway) to reach external HTTP sources.

---

## Configuration details

### URL → filename mapping (`SUFFIX_MAP`)
```python
SUFFIX_MAP = {
  'rent_contracts': 'rent_contracts.csv',
  'transactions':   'transactions.csv',
  'projects':       'projects.csv',
  'units':          'units.csv',
  'developers':     'developers.csv',
  'buildings':      'buildings.csv'
}
```
- If none of the keys appear in the URL (case-insensitive), the code uses the URL path’s basename or `data.csv`.

### S3 key layout
```
s3://BUCKET/{PATH_PREFIX}/{YYYY}/{MM}/{DD}/{suffix}.csv
```
- `YYYY/MM/DD` is **UTC** at ingestion time.

### Multipart strategy
- **Part size:** `S3_PART_MB` (default 50 MB); **must be ≥ 5 MB**.
- **HTTP chunk:** `HTTP_CHUNK_KB` (default 1024 KB). Chunks are appended to a buffer until it reaches a part.
- **Max parts:** S3 allows up to 10,000 parts; adjust `S3_PART_MB` for very large files.

### Metadata on each object
- `source_url`: the original URL
- `ingestion_time`: ISO 8601 UTC timestamp

---

## Observability

### Structured logs
Every log line is JSON, e.g.:

```json
{
  "event": "file_progress",
  "level": "INFO",
  "ts": "2025-08-26T13:45:01.123456+00:00",
  "aws_request_id": "…",
  "s3_key": "raw/2025/08/26/transactions.csv",
  "bytes": 104857600,
  "expected": 524288000,
  "percent": 20.0
}
```

Key events include:
- `lambda_start`, `lambda_complete`, `lambda_fatal`
- `ingestion_plan`, `no_file_urls`, `parameters_load_error`
- `file_download_start`, `file_progress`, `file_complete`, `file_complete_zero`, `file_error`
- `multipart_create_start/_complete/_done`, `part_upload_start/_complete`
- `multipart_abort_start/_done`
- `s3_request_counters`

### S3 request counter
The function tallies S3 calls and logs them once per invocation:

```json
{
  "event": "s3_request_counters",
  "create_multipart": 2,
  "upload_part": 20,
  "complete_multipart": 2,
  "abort_multipart": 0,
  "put_object": 0,
  "get_object": 1,
  "total": 25
}
```

### CloudWatch Logs Insights — sample queries

**Throughput per file**
```sql
fields @timestamp, event, s3_key, bytes, elapsed_ms, avg_mbps
| filter event = "file_complete"
| sort @timestamp desc
| display s3_key, bytes, elapsed_ms, avg_mbps
```

**Failures by URL**
```sql
fields @timestamp, event, url, error
| filter event = "file_error"
| sort @timestamp desc
```

**S3 request totals per invoke**
```sql
fields @timestamp, event, create_multipart, upload_part, complete_multipart, abort_multipart, put_object, get_object, total
| filter event = "s3_request_counters"
| sort @timestamp desc
```

---

## Example response (Lambda return body)

```json
{
  "message": "Streamed 2 ok, 0 failed",
  "elapsed_seconds": 18.42,
  "summary": {
    "total_files": 2,
    "successful": 2,
    "failed": 0,
    "total_bytes": 734003200
  },
  "s3_request_counts": {
    "create_multipart": 2,
    "upload_part": 16,
    "complete_multipart": 2,
    "abort_multipart": 0,
    "put_object": 0,
    "get_object": 1,
    "total": 21
  },
  "results": [
    {"url": "...", "s3_key": "raw/2025/08/26/transactions.csv", "bytes": 524288000, "parts": 10, "status": "success"},
    {"url": "...", "s3_key": "raw/2025/08/26/rent_contracts.csv", "bytes": 209715200, "parts": 6, "status": "success"}
  ]
}
```

---

## Tuning & best practices

- **Increase `CONCURRENCY`** to download more files in parallel. Watch outbound bandwidth and HTTP server limits.
- **Adjust `S3_PART_MB`** for optimal part counts (e.g., 32–128 MB is a good zone). Fewer, larger parts reduce S3 API calls.
- **Right-size memory**: more memory → more CPU/network → higher throughput.
- **Set `LOG_LEVEL=DEBUG`** during initial testing.
- **Retries/backoff**: The code will raise and abort the multipart on errors, then log `file_error`. Consider configuring *Lambda* retries or surrounding orchestration (e.g., Step Functions).

---

## Error handling

- **Zero-byte responses:** Aborts multipart (if started) and writes an **empty** object via `put_object`.
- **Any exception:** Aborts any in-flight multipart, logs `file_error`, and includes it in the returned `results`.
- **Missing config:** If `BUCKET_NAME` or `parameters.json` is missing/unreadable, the Lambda returns HTTP 500.

---

## Security notes

- URLs are fetched unauthenticated over HTTP(S). If targets require auth, extend the `session.get(...)` call with headers/tokens.
- If placing Lambda in a VPC, ensure egress via NAT. Otherwise, leave it out of VPC for simpler internet access.

---

## Testing locally

Use `parameters.json` with local/controllable URLs.

```bash
# Invoke via AWS SAM (example)
sam local invoke --event events/sample.json \
  --env-vars env.json
```

Example `env.json`:
```json
{
  "Parameters": {
    "BUCKET_NAME": "your-bucket",
    "PATH_PREFIX": "raw",
    "CONCURRENCY": "2",
    "S3_PART_MB": "50",
    "HTTP_CHUNK_KB": "1024",
    "LOG_LEVEL": "DEBUG"
  }
}
```

Upload a test `parameters.json`:
```bash
aws s3 cp parameters.json s3://your-bucket/config/parameters.json
```

---

## FAQ

**Q: Why both `HTTP_CHUNK_KB` and `S3_PART_MB`?**  
A: HTTP chunks stream from the socket into memory; once the buffer reaches `S3_PART_MB`, we flush a multipart part. This decouples network read granularity from S3 part sizing.

**Q: What if the server doesn’t send `Content-Length`?**  
A: Progress percentages won’t be logged (we still log absolute bytes and final throughput).

**Q: Can I write to different prefixes per file?**  
A: Yes—alter how `s3_key` is built in `stream_many` (e.g., per-URL logic).

**Q: CSV only?**  
A: The code sets `ContentType="text/csv"`. You can detect MIME type per URL or make it configurable.

---

## Minimal sequence (ASCII)

```
Lambda start
  └─ Get s3://BUCKET/config/parameters.json
     └─ For each URL (≤ CONCURRENCY in flight)
        ├─ create_multipart_upload
        ├─ [loop] HTTP read → buffer → upload_part
        ├─ complete_multipart_upload
        └─ log file_complete
  └─ log s3_request_counters
Return summary
```

---

## File layout & handler

- Keep the provided code as `lambda_function.py` (or set your handler accordingly).
- **Handler:** `lambda_function.lambda_handler`

---

### That’s it
Drop this README next to your Lambda code, wire env vars + IAM, and you’re ready to stream big CSVs into S3 with clear, queryable logs and precise S3 call accounting.

