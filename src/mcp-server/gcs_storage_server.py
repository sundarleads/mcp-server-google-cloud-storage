import asyncio
import os
import datetime
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
from google.cloud import storage
from google.oauth2 import service_account

# ── Credentials ───────────────────────────────────────────────────────────────
KEY_PATH   = os.environ.get("GCP_KEY_PATH") or \
             "/Users/yougasundarpanneerselvam/Desktop/py312_projects/creds/data-products-sdk-integration-4388c176a5b5.json"
PROJECT_ID = os.environ.get("GCP_PROJECT_ID") or \
             "data-products-sdk-integration"

credentials = service_account.Credentials.from_service_account_file(KEY_PATH)

# ── Server + Client ───────────────────────────────────────────────────────────
app        = Server("gcs-storage-server")
gcs_client = storage.Client(project=PROJECT_ID, credentials=credentials)

print(f"Connected to GCP project: {PROJECT_ID}", flush=True)


# ──────────────────────────────────────────────────────────────────────────────
# DECLARE ALL TOOLS
# ──────────────────────────────────────────────────────────────────────────────
@app.list_tools()
async def list_tools() -> list[Tool]:
    return [

        # ── BUCKET OPERATIONS ─────────────────────────────────────────────────
        Tool(
            name="list_buckets",
            description="List all GCS buckets in the GCP project",
            inputSchema={"type": "object", "properties": {}}
        ),
        Tool(
            name="create_bucket",
            description="Create a new GCS bucket in the project",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {"type": "string", "description": "Name of the bucket to create"},
                    "location":    {"type": "string", "description": "GCP region e.g. US, EU, asia-southeast1. Defaults to US"}
                },
                "required": ["bucket_name"]
            }
        ),
        Tool(
            name="delete_bucket",
            description="Delete a GCS bucket. Use force=true to delete even if it has files.",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {"type": "string", "description": "Name of the bucket to delete"},
                    "force":       {"type": "boolean", "description": "If true, deletes all files inside first. Defaults to false."}
                },
                "required": ["bucket_name"]
            }
        ),
        Tool(
            name="get_bucket_metadata",
            description="Get metadata of a GCS bucket — location, storage class, creation date",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {"type": "string", "description": "Name of the bucket"}
                },
                "required": ["bucket_name"]
            }
        ),

        # ── FILE OPERATIONS ───────────────────────────────────────────────────
        Tool(
            name="list_files",
            description="List all files in a GCS bucket with optional folder prefix filter",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {"type": "string", "description": "Name of the GCS bucket"},
                    "prefix":      {"type": "string", "description": "Optional folder prefix to filter, e.g. data/2024/"}
                },
                "required": ["bucket_name"]
            }
        ),
        Tool(
            name="upload_file",
            description="Upload a local file to a GCS bucket",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name":  {"type": "string", "description": "Name of the GCS bucket"},
                    "local_path":   {"type": "string", "description": "Full local path of the file to upload"},
                    "destination":  {"type": "string", "description": "Destination path inside the bucket"}
                },
                "required": ["bucket_name", "local_path", "destination"]
            }
        ),
        Tool(
            name="download_file",
            description="Download a file from GCS to local machine",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name":  {"type": "string", "description": "Name of the GCS bucket"},
                    "blob_name":    {"type": "string", "description": "Full path of file inside the bucket"},
                    "local_path":   {"type": "string", "description": "Local path to save the downloaded file"}
                },
                "required": ["bucket_name", "blob_name", "local_path"]
            }
        ),
        Tool(
            name="delete_file",
            description="Delete a specific file from a GCS bucket",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {"type": "string", "description": "Name of the GCS bucket"},
                    "file_name":   {"type": "string", "description": "Full path of the file to delete inside the bucket"}
                },
                "required": ["bucket_name", "file_name"]
            }
        ),
        Tool(
            name="copy_file",
            description="Copy a file within the same bucket or to a different bucket",
            inputSchema={
                "type": "object",
                "properties": {
                    "source_bucket":      {"type": "string", "description": "Source bucket name"},
                    "source_blob":        {"type": "string", "description": "Source file path inside bucket"},
                    "destination_bucket": {"type": "string", "description": "Destination bucket name"},
                    "destination_blob":   {"type": "string", "description": "Destination file path inside bucket"}
                },
                "required": ["source_bucket", "source_blob", "destination_bucket", "destination_blob"]
            }
        ),
        Tool(
            name="move_file",
            description="Move a file from one location to another — within same bucket or across buckets",
            inputSchema={
                "type": "object",
                "properties": {
                    "source_bucket":      {"type": "string", "description": "Source bucket name"},
                    "source_blob":        {"type": "string", "description": "Source file path"},
                    "destination_bucket": {"type": "string", "description": "Destination bucket name"},
                    "destination_blob":   {"type": "string", "description": "Destination file path"}
                },
                "required": ["source_bucket", "source_blob", "destination_bucket", "destination_blob"]
            }
        ),
        Tool(
            name="rename_file",
            description="Rename a file inside the same bucket",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {"type": "string", "description": "Name of the GCS bucket"},
                    "old_name":    {"type": "string", "description": "Current file path inside bucket"},
                    "new_name":    {"type": "string", "description": "New file path inside bucket"}
                },
                "required": ["bucket_name", "old_name", "new_name"]
            }
        ),
        Tool(
            name="get_file_metadata",
            description="Get metadata of a file — size, content type, MD5, created date, updated date",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {"type": "string", "description": "Name of the GCS bucket"},
                    "file_name":   {"type": "string", "description": "Full path of the file inside bucket"}
                },
                "required": ["bucket_name", "file_name"]
            }
        ),
        Tool(
            name="read_file_content",
            description="Read content of a small text file directly from GCS without downloading",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {"type": "string", "description": "Name of the GCS bucket"},
                    "file_name":   {"type": "string", "description": "Full path of the text file inside bucket"}
                },
                "required": ["bucket_name", "file_name"]
            }
        ),

        # ── FOLDER OPERATIONS ─────────────────────────────────────────────────
        Tool(
            name="list_folders",
            description="List all folders (prefixes) inside a GCS bucket",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {"type": "string", "description": "Name of the GCS bucket"},
                    "prefix":      {"type": "string", "description": "Optional parent prefix to list folders inside"}
                },
                "required": ["bucket_name"]
            }
        ),
        Tool(
            name="delete_folder",
            description="Delete all files under a folder prefix in a GCS bucket",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {"type": "string", "description": "Name of the GCS bucket"},
                    "folder_prefix": {"type": "string", "description": "Folder prefix to delete, e.g. data/2024/"}
                },
                "required": ["bucket_name", "folder_prefix"]
            }
        ),
        Tool(
            name="upload_folder",
            description="Upload an entire local folder recursively to a GCS bucket",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name":   {"type": "string", "description": "Name of the GCS bucket"},
                    "local_folder":  {"type": "string", "description": "Full local path of the folder to upload"},
                    "destination_prefix": {"type": "string", "description": "Destination prefix in bucket, e.g. data/uploads/"}
                },
                "required": ["bucket_name", "local_folder"]
            }
        ),
        Tool(
            name="download_folder",
            description="Download all files under a GCS prefix to a local folder",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name":  {"type": "string", "description": "Name of the GCS bucket"},
                    "prefix":       {"type": "string", "description": "GCS folder prefix to download"},
                    "local_folder": {"type": "string", "description": "Local folder path to save files into"}
                },
                "required": ["bucket_name", "prefix", "local_folder"]
            }
        ),

        # ── SEARCH & FILTER ───────────────────────────────────────────────────
        Tool(
            name="search_files",
            description="Search for files by name pattern inside a GCS bucket",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {"type": "string", "description": "Name of the GCS bucket"},
                    "pattern":     {"type": "string", "description": "Search pattern, e.g. .csv or report_2024"}
                },
                "required": ["bucket_name", "pattern"]
            }
        ),
        Tool(
            name="filter_by_date",
            description="List files in a bucket modified after a given date",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {"type": "string", "description": "Name of the GCS bucket"},
                    "after_date":  {"type": "string", "description": "Filter files modified after this date. Format: YYYY-MM-DD"}
                },
                "required": ["bucket_name", "after_date"]
            }
        ),

        # ── SIGNED URL ────────────────────────────────────────────────────────
        Tool(
            name="generate_signed_url",
            description="Generate a temporary public URL for a private GCS file",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name":   {"type": "string", "description": "Name of the GCS bucket"},
                    "file_name":     {"type": "string", "description": "Full path of the file inside bucket"},
                    "expiry_minutes": {"type": "integer", "description": "URL expiry in minutes. Defaults to 60."}
                },
                "required": ["bucket_name", "file_name"]
            }
        ),

        # ── MONITORING ────────────────────────────────────────────────────────
        Tool(
            name="get_bucket_size",
            description="Get total size of all files in a GCS bucket in MB",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {"type": "string", "description": "Name of the GCS bucket"},
                    "prefix":      {"type": "string", "description": "Optional prefix to calculate size for a specific folder"}
                },
                "required": ["bucket_name"]
            }
        ),
        Tool(
            name="get_file_count",
            description="Count total number of files in a GCS bucket or folder prefix",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {"type": "string", "description": "Name of the GCS bucket"},
                    "prefix":      {"type": "string", "description": "Optional prefix to count files in a specific folder"}
                },
                "required": ["bucket_name"]
            }
        ),
    ]


# ──────────────────────────────────────────────────────────────────────────────
# ROUTE ALL TOOL CALLS
# ──────────────────────────────────────────────────────────────────────────────
@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    handlers = {
        # Bucket
        "list_buckets":        handle_list_buckets,
        "create_bucket":       handle_create_bucket,
        "delete_bucket":       handle_delete_bucket,
        "get_bucket_metadata": handle_get_bucket_metadata,
        # File
        "list_files":          handle_list_files,
        "upload_file":         handle_upload_file,
        "download_file":       handle_download_file,
        "delete_file":         handle_delete_file,
        "copy_file":           handle_copy_file,
        "move_file":           handle_move_file,
        "rename_file":         handle_rename_file,
        "get_file_metadata":   handle_get_file_metadata,
        "read_file_content":   handle_read_file_content,
        # Folder
        "list_folders":        handle_list_folders,
        "delete_folder":       handle_delete_folder,
        "upload_folder":       handle_upload_folder,
        "download_folder":     handle_download_folder,
        # Search
        "search_files":        handle_search_files,
        "filter_by_date":      handle_filter_by_date,
        # Signed URL
        "generate_signed_url": handle_generate_signed_url,
        # Monitoring
        "get_bucket_size":     handle_get_bucket_size,
        "get_file_count":      handle_get_file_count,
    }
    handler = handlers.get(name)
    if handler:
        return await handler(arguments)
    return [TextContent(type="text", text=f"Unknown tool: {name}")]


# ──────────────────────────────────────────────────────────────────────────────
# BUCKET HANDLERS
# ──────────────────────────────────────────────────────────────────────────────
async def handle_list_buckets(arguments: dict) -> list[TextContent]:
    loop = asyncio.get_event_loop()
    try:
        buckets = await loop.run_in_executor(None, lambda: list(gcs_client.list_buckets()))
        if not buckets:
            return [TextContent(type="text", text="No buckets found in project")]
        lines = [f"Buckets in project '{PROJECT_ID}' ({len(buckets)} total):\n"]
        for b in buckets:
            lines.append(f"  • {b.name}")
        return [TextContent(type="text", text="\n".join(lines))]
    except Exception as e:
        return [TextContent(type="text", text=f"Error listing buckets: {str(e)}")]


async def handle_create_bucket(arguments: dict) -> list[TextContent]:
    loop     = asyncio.get_event_loop()
    name     = arguments["bucket_name"]
    location = arguments.get("location", "US")
    try:
        def do_create():
            bucket          = gcs_client.bucket(name)
            bucket.location = location
            return gcs_client.create_bucket(bucket)
        created = await loop.run_in_executor(None, do_create)
        return [TextContent(type="text", text=f"Bucket created: gs://{created.name}  Location: {location}")]
    except Exception as e:
        return [TextContent(type="text", text=f"Error creating bucket: {str(e)}")]


async def handle_delete_bucket(arguments: dict) -> list[TextContent]:
    loop  = asyncio.get_event_loop()
    name  = arguments["bucket_name"]
    force = arguments.get("force", False)
    try:
        def do_delete():
            bucket = gcs_client.bucket(name)
            if force:
                blobs = list(gcs_client.list_blobs(name))
                for blob in blobs:
                    blob.delete()
            bucket.delete()
        await loop.run_in_executor(None, do_delete)
        msg = f"Deleted bucket '{name}'"
        if force:
            msg += " including all files inside"
        return [TextContent(type="text", text=msg)]
    except Exception as e:
        return [TextContent(type="text", text=f"Error deleting bucket: {str(e)}\nTip: use force=true if bucket has files")]


async def handle_get_bucket_metadata(arguments: dict) -> list[TextContent]:
    loop = asyncio.get_event_loop()
    name = arguments["bucket_name"]
    try:
        def do_get():
            bucket = gcs_client.get_bucket(name)
            blobs  = list(gcs_client.list_blobs(name))
            return bucket, blobs
        bucket, blobs = await loop.run_in_executor(None, do_get)
        total_size = sum(b.size or 0 for b in blobs) / (1024 * 1024)
        return [TextContent(type="text", text=(
            f"Bucket Metadata: {name}\n"
            f"  Location      : {bucket.location}\n"
            f"  Storage Class : {bucket.storage_class}\n"
            f"  File Count    : {len(blobs)}\n"
            f"  Total Size    : {total_size:.2f} MB\n"
            f"  Created       : {bucket.time_created.strftime('%Y-%m-%d %H:%M:%S') if bucket.time_created else 'N/A'}"
        ))]
    except Exception as e:
        return [TextContent(type="text", text=f"Error getting bucket metadata: {str(e)}")]


# ──────────────────────────────────────────────────────────────────────────────
# FILE HANDLERS
# ──────────────────────────────────────────────────────────────────────────────
async def handle_list_files(arguments: dict) -> list[TextContent]:
    loop   = asyncio.get_event_loop()
    bucket = arguments["bucket_name"]
    prefix = arguments.get("prefix", "")
    try:
        blobs = await loop.run_in_executor(
            None, lambda: list(gcs_client.list_blobs(bucket, prefix=prefix))
        )
        if not blobs:
            return [TextContent(type="text", text=f"No files found in '{bucket}/{prefix}'")]
        lines = [f"Files in gs://{bucket}/{prefix}  ({len(blobs)} total):\n"]
        for b in blobs:
            lines.append(f"  • {b.name}  ({round((b.size or 0) / 1024, 1)} KB)")
        return [TextContent(type="text", text="\n".join(lines))]
    except Exception as e:
        return [TextContent(type="text", text=f"Error listing files: {str(e)}")]


async def handle_upload_file(arguments: dict) -> list[TextContent]:
    loop        = asyncio.get_event_loop()
    bucket_name = arguments["bucket_name"]
    local_path  = arguments["local_path"]
    destination = arguments["destination"]
    try:
        def do_upload():
            blob = gcs_client.bucket(bucket_name).blob(destination)
            blob.upload_from_filename(local_path)
        await asyncio.wait_for(loop.run_in_executor(None, do_upload), timeout=60.0)
        return [TextContent(type="text", text=f"Uploaded '{local_path}' → gs://{bucket_name}/{destination}")]
    except asyncio.TimeoutError:
        return [TextContent(type="text", text="Upload timed out after 60 seconds")]
    except Exception as e:
        return [TextContent(type="text", text=f"Error uploading file: {str(e)}")]


async def handle_download_file(arguments: dict) -> list[TextContent]:
    loop        = asyncio.get_event_loop()
    bucket_name = arguments["bucket_name"]
    blob_name   = arguments["blob_name"]
    local_path  = arguments["local_path"]
    try:
        def do_download():
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            blob = gcs_client.bucket(bucket_name).blob(blob_name)
            blob.download_to_filename(local_path)
        await asyncio.wait_for(loop.run_in_executor(None, do_download), timeout=60.0)
        return [TextContent(type="text", text=f"Downloaded gs://{bucket_name}/{blob_name} → '{local_path}'")]
    except asyncio.TimeoutError:
        return [TextContent(type="text", text="Download timed out after 60 seconds")]
    except Exception as e:
        return [TextContent(type="text", text=f"Error downloading file: {str(e)}")]


async def handle_delete_file(arguments: dict) -> list[TextContent]:
    loop        = asyncio.get_event_loop()
    bucket_name = arguments["bucket_name"]
    file_name   = arguments["file_name"]
    try:
        def do_delete():
            gcs_client.bucket(bucket_name).blob(file_name).delete()
        await loop.run_in_executor(None, do_delete)
        return [TextContent(type="text", text=f"Deleted gs://{bucket_name}/{file_name}")]
    except Exception as e:
        return [TextContent(type="text", text=f"Error deleting file: {str(e)}")]


async def handle_copy_file(arguments: dict) -> list[TextContent]:
    loop     = asyncio.get_event_loop()
    src_bkt  = arguments["source_bucket"]
    src_blob = arguments["source_blob"]
    dst_bkt  = arguments["destination_bucket"]
    dst_blob = arguments["destination_blob"]
    try:
        def do_copy():
            source      = gcs_client.bucket(src_bkt).blob(src_blob)
            dest_bucket = gcs_client.bucket(dst_bkt)
            gcs_client.copy_blob(source, dest_bucket, dst_blob)
        await loop.run_in_executor(None, do_copy)
        return [TextContent(type="text", text=(
            f"Copied:\n"
            f"  From : gs://{src_bkt}/{src_blob}\n"
            f"  To   : gs://{dst_bkt}/{dst_blob}"
        ))]
    except Exception as e:
        return [TextContent(type="text", text=f"Error copying file: {str(e)}")]


async def handle_move_file(arguments: dict) -> list[TextContent]:
    loop     = asyncio.get_event_loop()
    src_bkt  = arguments["source_bucket"]
    src_blob = arguments["source_blob"]
    dst_bkt  = arguments["destination_bucket"]
    dst_blob = arguments["destination_blob"]
    try:
        def do_move():
            source      = gcs_client.bucket(src_bkt).blob(src_blob)
            dest_bucket = gcs_client.bucket(dst_bkt)
            gcs_client.copy_blob(source, dest_bucket, dst_blob)
            source.delete()
        await loop.run_in_executor(None, do_move)
        return [TextContent(type="text", text=(
            f"Moved:\n"
            f"  From : gs://{src_bkt}/{src_blob}\n"
            f"  To   : gs://{dst_bkt}/{dst_blob}"
        ))]
    except Exception as e:
        return [TextContent(type="text", text=f"Error moving file: {str(e)}")]


async def handle_rename_file(arguments: dict) -> list[TextContent]:
    loop        = asyncio.get_event_loop()
    bucket_name = arguments["bucket_name"]
    old_name    = arguments["old_name"]
    new_name    = arguments["new_name"]
    try:
        def do_rename():
            bucket = gcs_client.bucket(bucket_name)
            source = bucket.blob(old_name)
            gcs_client.copy_blob(source, bucket, new_name)
            source.delete()
        await loop.run_in_executor(None, do_rename)
        return [TextContent(type="text", text=(
            f"Renamed in gs://{bucket_name}:\n"
            f"  From : {old_name}\n"
            f"  To   : {new_name}"
        ))]
    except Exception as e:
        return [TextContent(type="text", text=f"Error renaming file: {str(e)}")]


async def handle_get_file_metadata(arguments: dict) -> list[TextContent]:
    loop        = asyncio.get_event_loop()
    bucket_name = arguments["bucket_name"]
    file_name   = arguments["file_name"]
    try:
        def do_get():
            blob = gcs_client.bucket(bucket_name).blob(file_name)
            blob.reload()
            return blob
        blob = await loop.run_in_executor(None, do_get)
        return [TextContent(type="text", text=(
            f"File Metadata: {file_name}\n"
            f"  Bucket       : {bucket_name}\n"
            f"  Size         : {round((blob.size or 0) / 1024, 2)} KB\n"
            f"  Content Type : {blob.content_type}\n"
            f"  MD5          : {blob.md5_hash}\n"
            f"  Created      : {blob.time_created.strftime('%Y-%m-%d %H:%M:%S') if blob.time_created else 'N/A'}\n"
            f"  Updated      : {blob.updated.strftime('%Y-%m-%d %H:%M:%S') if blob.updated else 'N/A'}"
        ))]
    except Exception as e:
        return [TextContent(type="text", text=f"Error getting file metadata: {str(e)}")]


async def handle_read_file_content(arguments: dict) -> list[TextContent]:
    loop        = asyncio.get_event_loop()
    bucket_name = arguments["bucket_name"]
    file_name   = arguments["file_name"]
    try:
        def do_read():
            blob = gcs_client.bucket(bucket_name).blob(file_name)
            return blob.download_as_text()
        content = await loop.run_in_executor(None, do_read)
        # Limit output to first 3000 chars
        preview = content[:3000]
        truncated = " ... (truncated)" if len(content) > 3000 else ""
        return [TextContent(type="text", text=f"Content of gs://{bucket_name}/{file_name}:\n\n{preview}{truncated}")]
    except Exception as e:
        return [TextContent(type="text", text=f"Error reading file: {str(e)}")]


# ──────────────────────────────────────────────────────────────────────────────
# FOLDER HANDLERS
# ──────────────────────────────────────────────────────────────────────────────
async def handle_list_folders(arguments: dict) -> list[TextContent]:
    loop        = asyncio.get_event_loop()
    bucket_name = arguments["bucket_name"]
    prefix      = arguments.get("prefix", "")
    try:
        def do_list():
            iterator = gcs_client.list_blobs(bucket_name, prefix=prefix, delimiter="/")
            list(iterator)  # must consume iterator to populate prefixes
            return list(iterator.prefixes)
        folders = await loop.run_in_executor(None, do_list)
        if not folders:
            return [TextContent(type="text", text=f"No folders found in gs://{bucket_name}/{prefix}")]
        lines = [f"Folders in gs://{bucket_name}/{prefix}  ({len(folders)} total):\n"]
        for f in folders:
            lines.append(f"  📁 {f}")
        return [TextContent(type="text", text="\n".join(lines))]
    except Exception as e:
        return [TextContent(type="text", text=f"Error listing folders: {str(e)}")]


async def handle_delete_folder(arguments: dict) -> list[TextContent]:
    loop          = asyncio.get_event_loop()
    bucket_name   = arguments["bucket_name"]
    folder_prefix = arguments["folder_prefix"]
    try:
        def do_delete():
            blobs = list(gcs_client.list_blobs(bucket_name, prefix=folder_prefix))
            for blob in blobs:
                blob.delete()
            return len(blobs)
        count = await loop.run_in_executor(None, do_delete)
        return [TextContent(type="text", text=f"Deleted folder gs://{bucket_name}/{folder_prefix}  ({count} files removed)")]
    except Exception as e:
        return [TextContent(type="text", text=f"Error deleting folder: {str(e)}")]


async def handle_upload_folder(arguments: dict) -> list[TextContent]:
    loop         = asyncio.get_event_loop()
    bucket_name  = arguments["bucket_name"]
    local_folder = arguments["local_folder"]
    dest_prefix  = arguments.get("destination_prefix", "")
    try:
        def do_upload():
            uploaded = []
            for root, dirs, files in os.walk(local_folder):
                for file in files:
                    local_path  = os.path.join(root, file)
                    relative    = os.path.relpath(local_path, local_folder)
                    blob_name   = os.path.join(dest_prefix, relative).replace("\\", "/")
                    blob        = gcs_client.bucket(bucket_name).blob(blob_name)
                    blob.upload_from_filename(local_path)
                    uploaded.append(blob_name)
            return uploaded
        uploaded = await asyncio.wait_for(
            loop.run_in_executor(None, do_upload), timeout=300.0
        )
        lines = [f"Uploaded {len(uploaded)} files to gs://{bucket_name}/{dest_prefix}:\n"]
        for f in uploaded:
            lines.append(f"  ✓ {f}")
        return [TextContent(type="text", text="\n".join(lines))]
    except asyncio.TimeoutError:
        return [TextContent(type="text", text="Folder upload timed out after 5 minutes")]
    except Exception as e:
        return [TextContent(type="text", text=f"Error uploading folder: {str(e)}")]


async def handle_download_folder(arguments: dict) -> list[TextContent]:
    loop         = asyncio.get_event_loop()
    bucket_name  = arguments["bucket_name"]
    prefix       = arguments["prefix"]
    local_folder = arguments["local_folder"]
    try:
        def do_download():
            blobs      = list(gcs_client.list_blobs(bucket_name, prefix=prefix))
            downloaded = []
            for blob in blobs:
                relative   = blob.name[len(prefix):].lstrip("/")
                local_path = os.path.join(local_folder, relative)
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                blob.download_to_filename(local_path)
                downloaded.append(local_path)
            return downloaded
        downloaded = await asyncio.wait_for(
            loop.run_in_executor(None, do_download), timeout=300.0
        )
        lines = [f"Downloaded {len(downloaded)} files to '{local_folder}':\n"]
        for f in downloaded:
            lines.append(f"  ✓ {f}")
        return [TextContent(type="text", text="\n".join(lines))]
    except asyncio.TimeoutError:
        return [TextContent(type="text", text="Folder download timed out after 5 minutes")]
    except Exception as e:
        return [TextContent(type="text", text=f"Error downloading folder: {str(e)}")]


# ──────────────────────────────────────────────────────────────────────────────
# SEARCH & FILTER HANDLERS
# ──────────────────────────────────────────────────────────────────────────────
async def handle_search_files(arguments: dict) -> list[TextContent]:
    loop        = asyncio.get_event_loop()
    bucket_name = arguments["bucket_name"]
    pattern     = arguments["pattern"].lower()
    try:
        def do_search():
            blobs = list(gcs_client.list_blobs(bucket_name))
            return [b for b in blobs if pattern in b.name.lower()]
        matches = await loop.run_in_executor(None, do_search)
        if not matches:
            return [TextContent(type="text", text=f"No files matching '{pattern}' found in '{bucket_name}'")]
        lines = [f"Found {len(matches)} files matching '{pattern}':\n"]
        for b in matches:
            lines.append(f"  • {b.name}  ({round((b.size or 0) / 1024, 1)} KB)")
        return [TextContent(type="text", text="\n".join(lines))]
    except Exception as e:
        return [TextContent(type="text", text=f"Error searching files: {str(e)}")]


async def handle_filter_by_date(arguments: dict) -> list[TextContent]:
    loop        = asyncio.get_event_loop()
    bucket_name = arguments["bucket_name"]
    after_date  = arguments["after_date"]
    try:
        cutoff = datetime.datetime.strptime(after_date, "%Y-%m-%d").replace(
            tzinfo=datetime.timezone.utc
        )
        def do_filter():
            blobs = list(gcs_client.list_blobs(bucket_name))
            return [b for b in blobs if b.updated and b.updated > cutoff]
        matches = await loop.run_in_executor(None, do_filter)
        if not matches:
            return [TextContent(type="text", text=f"No files modified after {after_date}")]
        lines = [f"Files modified after {after_date}  ({len(matches)} total):\n"]
        for b in matches:
            lines.append(f"  • {b.name}  (updated: {b.updated.strftime('%Y-%m-%d')})")
        return [TextContent(type="text", text="\n".join(lines))]
    except Exception as e:
        return [TextContent(type="text", text=f"Error filtering files: {str(e)}")]


# ──────────────────────────────────────────────────────────────────────────────
# SIGNED URL HANDLER
# ──────────────────────────────────────────────────────────────────────────────
async def handle_generate_signed_url(arguments: dict) -> list[TextContent]:
    loop           = asyncio.get_event_loop()
    bucket_name    = arguments["bucket_name"]
    file_name      = arguments["file_name"]
    expiry_minutes = arguments.get("expiry_minutes", 60)
    try:
        def do_sign():
            blob = gcs_client.bucket(bucket_name).blob(file_name)
            return blob.generate_signed_url(
                expiration=datetime.timedelta(minutes=expiry_minutes),
                method="GET",
                version="v4"
            )
        url = await loop.run_in_executor(None, do_sign)
        return [TextContent(type="text", text=(
            f"Signed URL for gs://{bucket_name}/{file_name}:\n\n"
            f"{url}\n\n"
            f"Expires in {expiry_minutes} minutes."
        ))]
    except Exception as e:
        return [TextContent(type="text", text=f"Error generating signed URL: {str(e)}")]


# ──────────────────────────────────────────────────────────────────────────────
# MONITORING HANDLERS
# ──────────────────────────────────────────────────────────────────────────────
async def handle_get_bucket_size(arguments: dict) -> list[TextContent]:
    loop        = asyncio.get_event_loop()
    bucket_name = arguments["bucket_name"]
    prefix      = arguments.get("prefix", "")
    try:
        def do_size():
            blobs = list(gcs_client.list_blobs(bucket_name, prefix=prefix))
            total = sum(b.size or 0 for b in blobs)
            return total, len(blobs)
        total_bytes, count = await loop.run_in_executor(None, do_size)
        total_mb = total_bytes / (1024 * 1024)
        total_gb = total_mb / 1024
        return [TextContent(type="text", text=(
            f"Bucket Size: gs://{bucket_name}/{prefix}\n"
            f"  Files      : {count}\n"
            f"  Total Size : {total_mb:.2f} MB  ({total_gb:.4f} GB)"
        ))]
    except Exception as e:
        return [TextContent(type="text", text=f"Error getting bucket size: {str(e)}")]


async def handle_get_file_count(arguments: dict) -> list[TextContent]:
    loop        = asyncio.get_event_loop()
    bucket_name = arguments["bucket_name"]
    prefix      = arguments.get("prefix", "")
    try:
        def do_count():
            return len(list(gcs_client.list_blobs(bucket_name, prefix=prefix)))
        count = await loop.run_in_executor(None, do_count)
        location = f"gs://{bucket_name}/{prefix}" if prefix else f"gs://{bucket_name}"
        return [TextContent(type="text", text=f"File count in {location}: {count} files")]
    except Exception as e:
        return [TextContent(type="text", text=f"Error counting files: {str(e)}")]


# ──────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────
async def main():
    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())

asyncio.run(main())
