import subprocess
import os
import textwrap
import json
import boto3
from datetime import datetime

def lambda_handler(event, context):
    """
    Lambda function to download data files using async aiohttp and upload to S3
    """
    
    # Get environment variables
    bucket_name = os.environ.get('BUCKET_NAME')
    path_prefix = os.environ.get('PATH_PREFIX', 'raw')
    
    if not bucket_name:
        return {
            'statusCode': 500,
            'body': json.dumps('BUCKET_NAME environment variable not set')
        }
    
    # Load configuration from S3
    try:
        s3_client = boto3.client('s3')
        s3_client.download_file(bucket_name, 'config/parameters.json', '/tmp/parameters.json')
        with open('/tmp/parameters.json', 'r') as f:
            params = json.load(f)
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Failed to load parameters.json from S3: {str(e)}')
        }
    
    file_urls = params.get('file_urls', [])
    
    if not file_urls:
        return {
            'statusCode': 400,
            'body': json.dumps('No file_urls found in parameters.json')
        }
    
    # Create async download script
    download_src = textwrap.dedent(f"""
import asyncio
import aiohttp
import os
import time
import json
from urllib.parse import urlparse

# === configuration ===
FILE_URLS = {json.dumps(file_urls)}
OUTPUT_DIR = "/tmp/downloads"
CONCURRENCY = 5
CHUNK_SIZE = 1024 * 8
# =====================

os.makedirs(OUTPUT_DIR, exist_ok=True)

# URL suffix mapping based on filename patterns
SUFFIX_MAP = {{
    'rent_contracts': 'rent_contracts.csv',
    'transactions': 'transactions.csv', 
    'projects': 'projects.csv',
    'units': 'units.csv',
    'developers': 'developers.csv',
    'buildings': 'buildings.csv'
}}

def get_suffix_from_url(url):
    '''Extract appropriate suffix from URL'''
    for key, suffix in SUFFIX_MAP.items():
        if key in url.lower():
            return suffix
    # Default fallback
    parsed = urlparse(url)
    return os.path.basename(parsed.path) or 'data.csv'

async def download_one(session, url, sem):
    suffix = get_suffix_from_url(url)
    filename = os.path.join(OUTPUT_DIR, suffix)
    
    async with sem:
        try:
            async with session.get(url) as resp:
                resp.raise_for_status()
                with open(filename, "wb") as f:
                    async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                        f.write(chunk)
            
            file_size = os.path.getsize(filename)
            print(f"✔ Downloaded {{suffix}} ({{file_size}} bytes)")
            return {{
                'filename': filename,
                'suffix': suffix,
                'url': url,
                'size_bytes': file_size,
                'status': 'success'
            }}
            
        except Exception as e:
            print(f"✗ Failed to download {{suffix}}: {{str(e)}}")
            return {{
                'suffix': suffix,
                'url': url,
                'status': 'error',
                'error': str(e)
            }}

async def main():
    sem = asyncio.Semaphore(CONCURRENCY)
    timeout = aiohttp.ClientTimeout(total=300)  # 5 minute timeout
    
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [download_one(session, url, sem) for url in FILE_URLS]
        results = await asyncio.gather(*tasks)
        return results

if __name__ == "__main__":
    t0 = time.monotonic()
    results = asyncio.run(main())
    t1 = time.monotonic()
    
    download_time = t1 - t0
    print(f"Total download time: {{download_time:.2f}} seconds")
    
    # Write results to file for lambda to read
    with open("/tmp/download_results.json", "w") as f:
        json.dump({{"results": results, "download_time": download_time}}, f)
""")
    
    # Write the download script to /tmp
    script_path = '/tmp/download_script.py'
    with open(script_path, 'w') as f:
        f.write(download_src)
    
    try:
        # Execute the download script
        result = subprocess.run(['python', script_path], 
                              capture_output=True, text=True, timeout=600)
        
        if result.returncode != 0:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': 'Download script failed',
                    'stderr': result.stderr,
                    'stdout': result.stdout
                })
            }
        
        # Read download results
        with open('/tmp/download_results.json', 'r') as f:
            download_info = json.load(f)
        
        # Upload successful downloads to S3
        upload_results = []
        timestamp = datetime.now().strftime('%Y/%m/%d')
        
        for result_item in download_info['results']:
            if result_item['status'] == 'success':
                try:
                    filename = result_item['filename']
                    suffix = result_item['suffix']
                    s3_key = f"{path_prefix}/{timestamp}/{suffix}"
                    
                    with open(filename, 'rb') as f:
                        file_content = f.read()
                    
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=s3_key,
                        Body=file_content,
                        ContentType='text/csv',
                        Metadata={{
                            'source_url': result_item['url'],
                            'ingestion_time': datetime.now().isoformat(),
                            'file_size': str(len(file_content)),
                            'original_suffix': suffix
                        }}
                    )
                    
                    upload_results.append({{
                        'suffix': suffix,
                        's3_key': s3_key,
                        'size_bytes': len(file_content),
                        'source_url': result_item['url'],
                        'status': 'success'
                    }})
                    
                    print(f"✔ Uploaded {{suffix}} to s3://{{bucket_name}}/{{s3_key}}")
                    
                except Exception as e:
                    error_msg = f"Failed to upload {{result_item['suffix']}}: {{str(e)}}"
                    print(error_msg)
                    upload_results.append({{
                        'suffix': result_item['suffix'],
                        'status': 'error',
                        'error': error_msg
                    }})
            else:
                # Include download failures in results
                upload_results.append(result_item)
        
        # Cleanup temp files
        try:
            for result_item in download_info['results']:
                if result_item['status'] == 'success' and os.path.exists(result_item['filename']):
                    os.remove(result_item['filename'])
            if os.path.exists(script_path):
                os.remove(script_path)
            if os.path.exists('/tmp/download_results.json'):
                os.remove('/tmp/download_results.json')
        except Exception:
            pass  # Ignore cleanup errors
        
        successful = len([r for r in upload_results if r['status'] == 'success'])
        failed = len([r for r in upload_results if r['status'] == 'error'])
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Data ingestion completed: {successful} successful, {failed} failed',
                'download_time_seconds': download_info['download_time'],
                'summary': {
                    'total_files': len(file_urls),
                    'successful': successful,
                    'failed': failed
                },
                'results': upload_results
            })
        }
        
    except subprocess.TimeoutExpired:
        return {
            'statusCode': 500,
            'body': json.dumps('Download script timed out after 600 seconds')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Lambda execution failed: {str(e)}')
        }