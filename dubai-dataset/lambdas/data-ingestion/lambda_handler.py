import asyncio
import aiohttp
import os
import time
import json
import boto3
import io
import logging
import traceback
from datetime import datetime, timezone
from urllib.parse import urlparse

# ================= Logging =================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)

def log(event: str, level: str = "info", **fields):
    rec = {
        "event": event,
        "level": level.upper(),
        "ts": datetime.now(timezone.utc).isoformat(),
        **fields,
    }
    line = json.dumps(rec, ensure_ascii=False)
    if level.lower() == "error":
        logger.error(line)
    elif level.lower() == "warning":
        logger.warning(line)
    else:
        logger.info(line)
# ===========================================

# ========= S3 request counter ==============
class S3RequestCounter:
    def __init__(self):
        self.counts = {
            "create_multipart": 0,
            "upload_part": 0,
            "complete_multipart": 0,
            "abort_multipart": 0,
            "put_object": 0,
            "get_object": 0,
            "total": 0,
        }
        self._lock = asyncio.Lock()

    async def inc(self, name: str, n: int = 1):
        async with self._lock:
            self.counts[name] = self.counts.get(name, 0) + n
            self.counts["total"] += n

    # use ONLY before the event loop work starts (e.g., in lambda_handler before asyncio.run)
    def inc_sync(self, name: str, n: int = 1):
        self.counts[name] = self.counts.get(name, 0) + n
        self.counts["total"] += n

    def snapshot(self):
        return dict(self.counts)
# ===========================================

# ==== config via ENV ====
# Required: BUCKET_NAME
# Optional:
#   PATH_PREFIX    (default "raw")
#   CONCURRENCY    (default "2")
#   S3_PART_MB     (default "50")
#   HTTP_CHUNK_KB  (default "1024")
#   LOG_LEVEL      (default "INFO")
# ========================

SUFFIX_MAP = {
    'rent_contracts': 'rent_contracts.csv',
    'transactions': 'transactions.csv',
    'projects': 'projects.csv',
    'units': 'units.csv',
    'developers': 'developers.csv',
    'buildings': 'buildings.csv'
}

def _suffix_from_url(url: str) -> str:
    u = url.lower()
    for key, suffix in SUFFIX_MAP.items():
        if key in u:
            return suffix
    parsed = urlparse(url)
    name = os.path.basename(parsed.path)
    return name or 'data.csv'

async def _upload_part_to_s3(s3_client, bucket, key, upload_id, part_number, body_bytes, request_id, s3_counter: S3RequestCounter):
    started = time.monotonic()
    part_bytes = len(body_bytes)
    log("part_upload_start", aws_request_id=request_id, s3_key=key, part_number=part_number, bytes=part_bytes)

    await s3_counter.inc("upload_part")
    def _do():
        return s3_client.upload_part(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            PartNumber=part_number,
            Body=body_bytes
        )['ETag']
    etag = await asyncio.to_thread(_do)

    ms = (time.monotonic() - started) * 1000
    mbps = (part_bytes / (1024*1024)) / max(ms/1000, 1e-6)
    log("part_upload_complete",
        aws_request_id=request_id, s3_key=key, part_number=part_number,
        bytes=part_bytes, ms=round(ms, 2), mbps=round(mbps, 3), etag=etag)
    return etag

async def _create_multipart(s3_client, bucket, key, content_type, metadata, request_id, s3_counter: S3RequestCounter):
    log("multipart_create_start", aws_request_id=request_id, s3_key=key)
    await s3_counter.inc("create_multipart")
    def _do():
        return s3_client.create_multipart_upload(
            Bucket=bucket,
            Key=key,
            ContentType=content_type,
            Metadata=metadata
        )['UploadId']
    upload_id = await asyncio.to_thread(_do)
    log("multipart_create_complete", aws_request_id=request_id, s3_key=key, upload_id=upload_id)
    return upload_id

async def _complete_multipart(s3_client, bucket, key, upload_id, parts, request_id, s3_counter: S3RequestCounter):
    parts_sorted = sorted(parts, key=lambda p: p['PartNumber'])
    log("multipart_complete_start", aws_request_id=request_id, s3_key=key, upload_id=upload_id, parts=len(parts_sorted))
    await s3_counter.inc("complete_multipart")
    def _do():
        return s3_client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts_sorted}
        )
    resp = await asyncio.to_thread(_do)
    log("multipart_complete_done", aws_request_id=request_id, s3_key=key, upload_id=upload_id)
    return resp

async def _abort_multipart(s3_client, bucket, key, upload_id, request_id, s3_counter: S3RequestCounter):
    log("multipart_abort_start", level="warning", aws_request_id=request_id, s3_key=key, upload_id=upload_id)
    await s3_counter.inc("abort_multipart")
    def _do():
        try:
            s3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        except Exception:
            pass
    await asyncio.to_thread(_do)
    log("multipart_abort_done", level="warning", aws_request_id=request_id, s3_key=key, upload_id=upload_id)

async def _put_object_empty(s3_client, bucket, key, metadata, request_id, s3_counter: S3RequestCounter):
    # Edge case: server returned a truly empty file; do single put_object of 0 bytes
    await s3_counter.inc("put_object")
    def _do():
        return s3_client.put_object(Bucket=bucket, Key=key, Body=b"", ContentType="text/csv", Metadata=metadata)
    return await asyncio.to_thread(_do)

async def stream_one_csv_to_s3(session, url, sem, s3_client, bucket, s3_key,
                               s3_part_bytes: int, http_chunk_bytes: int, request_id: str,
                               s3_counter: S3RequestCounter):
    async with sem:
        upload_id = None
        parts = []
        part_number = 1
        total_bytes = 0
        buf = io.BytesIO()
        started = time.monotonic()

        metadata = {
            'source_url': url,
            'ingestion_time': datetime.now(timezone.utc).isoformat()
        }

        try:
            timeout = aiohttp.ClientTimeout(total=None, sock_read=300)
            async with session.get(url, timeout=timeout) as resp:
                resp.raise_for_status()

                expected = None
                cl = resp.headers.get("Content-Length")
                if cl and cl.isdigit():
                    expected = int(cl)

                log("file_download_start",
                    aws_request_id=request_id, url=url, s3_key=s3_key,
                    expected_bytes=expected)

                upload_id = await _create_multipart(
                    s3_client, bucket, s3_key, 'text/csv', metadata, request_id, s3_counter
                )

                async for chunk in resp.content.iter_chunked(http_chunk_bytes):
                    if not chunk:
                        continue
                    total_bytes += len(chunk)
                    buf.write(chunk)

                    if buf.tell() >= s3_part_bytes:
                        part_bytes = buf.getvalue()
                        buf.seek(0); buf.truncate(0)

                        await asyncio.sleep(0)  # cooperative yield
                        etag = await _upload_part_to_s3(
                            s3_client, bucket, s3_key, upload_id, part_number, part_bytes, request_id, s3_counter
                        )
                        parts.append({'ETag': etag, 'PartNumber': part_number})
                        part_number += 1

                        if expected:
                            pct = round((total_bytes / expected) * 100, 2)
                            log("file_progress", aws_request_id=request_id, s3_key=s3_key,
                                bytes=total_bytes, expected=expected, percent=pct)

                # Handle final part or empty file
                if total_bytes == 0:
                    # No bytes read at all; abort multipart and write empty object
                    await _abort_multipart(s3_client, bucket, s3_key, upload_id, request_id, s3_counter)
                    await _put_object_empty(s3_client, bucket, s3_key, metadata, request_id, s3_counter)
                    elapsed_ms = (time.monotonic() - started) * 1000
                    log("file_complete_zero",
                        aws_request_id=request_id, s3_key=s3_key, url=url,
                        bytes=0, parts=0, elapsed_ms=round(elapsed_ms, 2), avg_mbps=0.0)
                    return {'url': url, 's3_key': s3_key, 'bytes': 0, 'parts': 0, 'status': 'success'}

                if buf.tell() > 0:
                    part_bytes = buf.getvalue()
                    etag = await _upload_part_to_s3(
                        s3_client, bucket, s3_key, upload_id, part_number, part_bytes, request_id, s3_counter
                    )
                    parts.append({'ETag': etag, 'PartNumber': part_number})

                await _complete_multipart(s3_client, bucket, s3_key, upload_id, parts, request_id, s3_counter)

                elapsed_ms = (time.monotonic() - started) * 1000
                avg_mbps = (total_bytes / (1024*1024)) / max(elapsed_ms/1000, 1e-6)
                log("file_complete",
                    aws_request_id=request_id, s3_key=s3_key, url=url,
                    bytes=total_bytes, parts=len(parts),
                    elapsed_ms=round(elapsed_ms, 2), avg_mbps=round(avg_mbps, 3))

                return {
                    'url': url,
                    's3_key': s3_key,
                    'bytes': total_bytes,
                    'parts': len(parts),
                    'status': 'success'
                }

        except Exception as e:
            err = repr(e)
            tb = traceback.format_exc(limit=3)
            if upload_id is not None:
                await _abort_multipart(s3_client, bucket, s3_key, upload_id, request_id, s3_counter)
            log("file_error", level="error",
                aws_request_id=request_id, s3_key=s3_key, url=url,
                error=err, traceback=tb)
            return {
                'url': url,
                's3_key': s3_key,
                'status': 'error',
                'error': err
            }

async def stream_many(files, bucket_name, path_prefix, s3_client,
                      concurrency: int, s3_part_mb: int, http_chunk_kb: int,
                      request_id: str, s3_counter: S3RequestCounter):
    sem = asyncio.Semaphore(concurrency)
    s3_part_bytes = s3_part_mb * 1024 * 1024
    http_chunk_bytes = http_chunk_kb * 1024

    timestamp = datetime.now(timezone.utc).strftime('%Y/%m/%d')

    timeout = aiohttp.ClientTimeout(total=None, sock_read=300)
    connector = aiohttp.TCPConnector(limit=concurrency)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = []
        for url in files:
            suffix = _suffix_from_url(url)
            s3_key = f"{path_prefix}/{timestamp}/{suffix}"
            tasks.append(
                stream_one_csv_to_s3(
                    session, url, sem, s3_client, bucket_name, s3_key,
                    s3_part_bytes=s3_part_bytes,
                    http_chunk_bytes=http_chunk_bytes,
                    request_id=request_id,
                    s3_counter=s3_counter
                )
            )
        return await asyncio.gather(*tasks)

def lambda_handler(event, context):
    request_id = getattr(context, "aws_request_id", "unknown")
    log("lambda_start", aws_request_id=request_id, event_overview="stream_http_to_s3")

    bucket_name = os.environ.get('BUCKET_NAME')
    path_prefix = os.environ.get('PATH_PREFIX', 'raw')
    if not bucket_name:
        log("lambda_config_error", level="error", aws_request_id=request_id, reason="BUCKET_NAME not set")
        return {'statusCode': 500, 'body': json.dumps('BUCKET_NAME environment variable not set')}

    concurrency = int(os.environ.get('CONCURRENCY', '2'))
    s3_part_mb = int(os.environ.get('S3_PART_MB', '50'))
    http_chunk_kb = int(os.environ.get('HTTP_CHUNK_KB', '1024'))

    s3_client = boto3.client('s3')
    s3_counter = S3RequestCounter()

    # Load URL list from parameters.json in S3 (same bucket)
    try:
        s3_counter.inc_sync("get_object")
        s3_client.download_file(bucket_name, 'config/parameters.json', '/tmp/parameters.json')
        with open('/tmp/parameters.json', 'r') as f:
            params = json.load(f)
    except Exception as e:
        log("parameters_load_error", level="error", aws_request_id=request_id, error=repr(e))
        return {'statusCode': 500, 'body': json.dumps(f'Failed to load parameters.json: {str(e)}')}

    file_urls = params.get('file_urls', [])
    if not file_urls:
        log("no_file_urls", level="warning", aws_request_id=request_id)
        return {'statusCode': 400, 'body': json.dumps('No file_urls found in parameters.json')}

    log("ingestion_plan",
        aws_request_id=request_id, files=len(file_urls), concurrency=concurrency,
        s3_part_mb=s3_part_mb, http_chunk_kb=http_chunk_kb, bucket=bucket_name, prefix=path_prefix)

    try:
        t0 = time.monotonic()
        results = asyncio.run(
            stream_many(file_urls, bucket_name, path_prefix, s3_client,
                        concurrency=concurrency,
                        s3_part_mb=s3_part_mb,
                        http_chunk_kb=http_chunk_kb,
                        request_id=request_id,
                        s3_counter=s3_counter)
        )
        elapsed = time.monotonic() - t0

        success = [r for r in results if r.get('status') == 'success']
        failed  = [r for r in results if r.get('status') == 'error']

        # Log + return the S3 request counters
        s3_counts = s3_counter.snapshot()
        log("s3_request_counters", aws_request_id=request_id, **s3_counts)

        log("lambda_complete", aws_request_id=request_id,
            files_total=len(file_urls), files_ok=len(success), files_failed=len(failed),
            elapsed_ms=round(elapsed*1000, 2),
            total_bytes=sum(r.get('bytes', 0) for r in success))

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Streamed {len(success)} ok, {len(failed)} failed',
                'elapsed_seconds': elapsed,
                'summary': {
                    'total_files': len(file_urls),
                    'successful': len(success),
                    'failed': len(failed),
                    'total_bytes': sum(r.get('bytes', 0) for r in success)
                },
                's3_request_counts': s3_counts,
                'results': results
            })
        }
    except Exception as e:
        log("lambda_fatal", level="error", aws_request_id=request_id, error=repr(e),
            traceback=traceback.format_exc(limit=5))
        return {'statusCode': 500, 'body': json.dumps(f'Lambda execution failed: {str(e)}')}
