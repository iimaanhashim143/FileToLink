# KPS/server/stream_routes.py (FIXED & OPTIMIZED FOR HTML5 / video.js)

import re
import secrets
import time
import mimetypes
from urllib.parse import quote, unquote

from aiohttp import web

from KPS import __version__, StartTime
from KPS.bot import StreamBot, multi_clients, work_loads
from KPS.server.exceptions import FileNotFound, InvalidHash
from KPS.utils.custom_dl import ByteStreamer
from KPS.utils.logger import logger
from KPS.utils.render_template import render_page
from KPS.utils.time_format import get_readable_time

routes = web.RouteTableDef()

SECURE_HASH_LENGTH = 6
CHUNK_SIZE = 1024 * 1024
MAX_CONCURRENT_PER_CLIENT = 8
RANGE_REGEX = re.compile(r"bytes=(?P<start>\d*)-(?P<end>\d*)")
PATTERN_HASH_FIRST = re.compile(rf"^([a-zA-Z0-9_-]{{{SECURE_HASH_LENGTH}}})(\d+)(?:/.*)?$")
PATTERN_ID_FIRST = re.compile(r"^(\d+)(?:/.*)?$")
VALID_HASH_REGEX = re.compile(r'^[a-zA-Z0-9_-]+$')

streamers = {}
mimetypes.init()


def get_streamer(client_id: int) -> ByteStreamer:
    if client_id not in streamers:
        streamers[client_id] = ByteStreamer(multi_clients[client_id])
    return streamers[client_id]


def parse_media_request(path: str, query: dict) -> tuple[int, str]:
    clean_path = unquote(path).strip('/')

    match = PATTERN_HASH_FIRST.match(clean_path)
    if match:
        return int(match.group(2)), match.group(1)

    match = PATTERN_ID_FIRST.match(clean_path)
    if match:
        message_id = int(match.group(1))
        secure_hash = query.get("hash", "").strip()
        if len(secure_hash) == SECURE_HASH_LENGTH and VALID_HASH_REGEX.match(secure_hash):
            return message_id, secure_hash

    raise InvalidHash("Invalid URL or missing hash")


def select_optimal_client() -> tuple[int, ByteStreamer]:
    available = [(cid, load) for cid, load in work_loads.items() if load < MAX_CONCURRENT_PER_CLIENT]
    client_id = min(available, key=lambda x: x[1])[0] if available else min(work_loads, key=work_loads.get)
    return client_id, get_streamer(client_id)


def parse_range_header(range_header: str, file_size: int) -> tuple[int, int]:
    if not range_header:
        return 0, file_size - 1

    match = RANGE_REGEX.match(range_header)
    if not match:
        raise web.HTTPBadRequest(text="Invalid Range header")

    start = int(match.group('start') or 0)
    end = int(match.group('end') or file_size - 1)

    if start > end or end >= file_size:
        raise web.HTTPRequestRangeNotSatisfiable(headers={"Content-Range": f"bytes */{file_size}"})

    return start, end


@routes.get("/", allow_head=True)
async def root_redirect(request):
    raise web.HTTPFound("https://telegram.me/KPSBots")


@routes.get("/status", allow_head=True)
async def status_endpoint(request):
    uptime = time.time() - StartTime
    return web.json_response({
        "server": {"status": "operational", "version": __version__, "uptime": get_readable_time(uptime)},
        "telegram_bot": {"username": f"@{StreamBot.username}", "active_clients": len(multi_clients)},
        "resources": {"total_workload": sum(work_loads.values()), "distribution": work_loads}
    })


@routes.get(r"/watch/kpsbots-{path:.+}", allow_head=True)
async def media_preview(request: web.Request):
    path = request.match_info['path']
    message_id, secure_hash = parse_media_request(path, request.query)
    page = await render_page(message_id, secure_hash, requested_action='stream')
    return web.Response(text=page, content_type='text/html')


@routes.get(r"/kpsbots-{path:.+}", allow_head=True)
async def media_delivery(request: web.Request):
    path = request.match_info['path']
    message_id, secure_hash = parse_media_request(path, request.query)

    client_id, streamer = select_optimal_client()
    work_loads[client_id] += 1

    try:
        info = await streamer.get_file_info(message_id)
        if info['unique_id'][:SECURE_HASH_LENGTH] != secure_hash:
            raise InvalidHash("Hash mismatch")

        file_size = info['file_size']
        range_header = request.headers.get('Range')
        start, end = parse_range_header(range_header, file_size)
        content_length = end - start + 1

        filename = info.get('file_name') or f"video_{message_id}.mp4"
        mime_type = info.get('mime_type') or mimetypes.guess_type(filename)[0] or 'video/mp4'

        headers = {
            "Content-Type": mime_type,
            "Content-Length": str(content_length),
            "Content-Disposition": f"inline; filename*=UTF-8''{quote(filename)}",
            "Accept-Ranges": "bytes",
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Cache-Control": "public, max-age=31536000",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Range",
            "Access-Control-Expose-Headers": "Content-Range, Accept-Ranges, Content-Length",
            "X-Content-Type-Options": "nosniff"
        }

        if request.method == 'HEAD':
            return web.Response(status=206, headers=headers)

        async def stream():
            sent = 0
            async for chunk in streamer.stream_file(message_id, offset=start, limit=content_length):
                if sent + len(chunk) > content_length:
                    chunk = chunk[:content_length - sent]
                sent += len(chunk)
                yield chunk
                if sent >= content_length:
                    break

        return web.Response(status=206, body=stream(), headers=headers)

    finally:
        work_loads[client_id] -= 1
