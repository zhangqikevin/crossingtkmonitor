"""
TikTok Live Monitor — Web 工具后端

基于 https://github.com/isaackogan/TikTokLive
用户输入 TikTok 直播间 URL,后端连接直播间,把库能读取到的所有事件
通过 WebSocket 实时推送给前端页面。
"""

import asyncio
import contextlib
import dataclasses
import enum
import json
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

from urllib.parse import urlparse

import betterproto
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

import TikTokLive.events as tiktok_events
from TikTokLive import TikTokLiveClient
from TikTokLive.client.errors import (
    AgeRestrictedError,
    SignAPIError,
    SignatureRateLimitError,
    UserNotFoundError,
    UserOfflineError,
)
from TikTokLive.events import ConnectEvent
from TikTokLive.events.custom_events import BaseEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("tiktok-live-monitor")

STATIC_DIR = Path(__file__).parent / "static"

# WebsocketResponseEvent 对每条原始消息都会触发一次,内容与具体事件重复,排除掉
EXCLUDED_EVENTS = {"WebsocketResponseEvent", "Event", "BaseEvent", "CustomEvent", "ProtoEvent"}

MAX_STR_LEN = 4096

# 视频流代理只允许 TikTok 直播 CDN 的域名,防止被当成任意代理使用
ALLOWED_STREAM_HOST_SUFFIXES = (
    ".tiktokcdn.com",
    ".tiktokcdn-us.com",
    ".tiktokcdn-eu.com",
    ".ttlivecdn.com",
    ".tiktokv.com",
)


def collect_event_classes() -> dict:
    """收集 TikTokLive 库里所有可监听的事件类(共 200+ 种)"""
    classes = {}
    for name in dir(tiktok_events):
        if not name.endswith("Event") or name in EXCLUDED_EVENTS:
            continue
        obj = getattr(tiktok_events, name)
        if isinstance(obj, type) and (
            issubclass(obj, BaseEvent) or issubclass(obj, betterproto.Message)
        ):
            classes[name] = obj
    return classes


EVENT_CLASSES = collect_event_classes()
logger.info("已注册 %d 种 TikTokLive 事件类型", len(EVENT_CLASSES))


def _clean(value):
    """把事件数据清理成可安全发给前端的 JSON(截断超长字符串、转换 bytes)"""
    if isinstance(value, str):
        if len(value) > MAX_STR_LEN:
            return value[:MAX_STR_LEN] + f"…[截断,共{len(value)}字符]"
        return value
    if isinstance(value, bytes):
        return f"<{len(value)} bytes>"
    if isinstance(value, dict):
        return {k: _clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean(v) for v in value]
    return value


def _convert_value(v):
    if isinstance(v, betterproto.Message):
        return _message_to_dict(v)
    if isinstance(v, enum.Enum):
        return v.name if getattr(v, "name", None) else int(v)
    if isinstance(v, (list, tuple)):
        return [_convert_value(x) for x in v]
    if isinstance(v, dict):
        return {k: _convert_value(x) for k, x in v.items()}
    if isinstance(v, bytes):
        return f"<{len(v)} bytes>"
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, timedelta):
        return v.total_seconds()
    return v


def _message_to_dict(msg: betterproto.Message) -> dict:
    """手动遍历 proto 字段做序列化。

    betterproto 自带的 to_dict() 遇到 TikTok 新增、但库的 proto 定义里
    没有的枚举值会抛 ValueError,导致整条事件(连同用户名、评论内容)丢失。
    这里逐字段转换,未知枚举值原样保留为数字。
    """
    out = {}
    for f in dataclasses.fields(msg):
        try:
            v = getattr(msg, f.name)
        except Exception:
            continue
        try:
            converted = _convert_value(v)
        except Exception:
            continue
        if converted is None or converted == "" or converted == 0 \
                or converted is False or converted == [] or converted == {}:
            continue  # 与 to_dict(include_default_values=False) 行为保持一致
        out[f.name] = converted
    return out


def serialize_event(event) -> dict:
    try:
        if isinstance(event, betterproto.Message):
            data = _message_to_dict(event)
        elif dataclasses.is_dataclass(event):
            data = dataclasses.asdict(event)
        else:
            data = {k: v for k, v in vars(event).items() if not k.startswith("_")}
        return _clean(data)
    except Exception as e:  # 单个事件序列化失败不影响整体
        return {"_serialize_error": str(e), "_repr": repr(event)[:500]}


class Session:
    """一个浏览器 WebSocket 连接对应一个 TikTokLive 客户端"""

    def __init__(self, ws: WebSocket):
        self.ws = ws
        self.client: TikTokLiveClient | None = None
        self.task: asyncio.Task | None = None
        self.send_lock = asyncio.Lock()
        self.closed = False

    async def send(self, payload: dict):
        if self.closed:
            return
        async with self.send_lock:
            try:
                await self.ws.send_text(json.dumps(payload, ensure_ascii=False, default=str))
            except Exception:
                self.closed = True

    async def connect(self, url: str):
        await self.stop(notify=False)

        try:
            unique_id = TikTokLiveClient.parse_unique_id(url.strip())
        except Exception:
            await self.send({"type": "status", "state": "error",
                             "message": "Could not parse a username from the input. "
                                        "Try https://www.tiktok.com/@username/live or @username"})
            return

        client = TikTokLiveClient(unique_id=unique_id)
        self.client = client

        for name, cls in EVENT_CLASSES.items():
            client.add_listener(cls, self._make_handler(name, client))
        client.add_listener(ConnectEvent, self._make_connect_handler(client))

        await self.send({"type": "status", "state": "connecting", "unique_id": unique_id,
                         "message": f"Connecting to @{unique_id}'s LIVE…"})
        self.task = asyncio.create_task(self._run(client, unique_id))

    SIGN_RETRIES = 3  # 签名服务(eulerstream)瞬时故障/限流时的自动重试次数

    async def _run(self, client: TikTokLiveClient, unique_id: str):
        message = None
        for attempt in range(1, self.SIGN_RETRIES + 1):
            try:
                await client.connect(fetch_room_info=True, fetch_gift_info=True)
                message = {"type": "status", "state": "disconnected", "message": "Connection ended"}
            except asyncio.CancelledError:
                raise
            except UserOfflineError:
                message = {"type": "status", "state": "error",
                           "message": f"@{unique_id} is not live right now"}
            except UserNotFoundError:
                message = {"type": "status", "state": "error",
                           "message": f"User @{unique_id} not found — check the username/link"}
            except AgeRestrictedError:
                message = {"type": "status", "state": "error",
                           "message": "This LIVE is age-restricted and cannot be accessed anonymously"}
            except SignatureRateLimitError as e:
                try:
                    wait = max(int(e.retry_after), 5)
                except Exception:
                    wait = 30
                if attempt < self.SIGN_RETRIES and self.client is client:
                    logger.warning("Sign API rate limit, retrying in %ss (%d/%d)", wait, attempt, self.SIGN_RETRIES)
                    await self.send({"type": "status", "state": "connecting",
                                     "message": f"Signing service rate limit — retrying in {wait}s "
                                                f"({attempt}/{self.SIGN_RETRIES})…"})
                    await asyncio.sleep(wait)
                    continue
                message = {"type": "status", "state": "error",
                           "message": "Rate-limited by the TikTok signing service — wait a minute and try again"}
            except SignAPIError as e:
                wait = 3 * attempt
                if attempt < self.SIGN_RETRIES and self.client is client:
                    logger.warning("Sign API error (%s), retrying in %ss (%d/%d)", e, wait, attempt, self.SIGN_RETRIES)
                    await self.send({"type": "status", "state": "connecting",
                                     "message": f"Signing service error — retrying in {wait}s "
                                                f"({attempt}/{self.SIGN_RETRIES})…"})
                    await asyncio.sleep(wait)
                    continue
                message = {"type": "status", "state": "error",
                           "message": "The third-party TikTok signing service (eulerstream.com) is failing "
                                      "right now — this is not on your end, try again in a few minutes"}
            except Exception as e:
                logger.exception("Failed to connect to @%s", unique_id)
                message = {"type": "status", "state": "error",
                           "message": f"Connection failed: {type(e).__name__}: {e}"}
            break
        if self.client is client and message:  # 已被新的连接替换时不再发过期状态
            await self.send(message)

    def _make_connect_handler(self, client: TikTokLiveClient):
        async def handler(event: ConnectEvent):
            if self.client is not client:
                return
            await self.send({"type": "status", "state": "connected",
                             "unique_id": event.unique_id, "room_id": str(event.room_id),
                             "message": f"Connected to @{event.unique_id} (Room ID: {event.room_id})"})
            info = client.room_info
            if info:
                await self.send({"type": "room_info", "data": _clean(info)})
                stream = info.get("stream_url") or {}
                await self.send({"type": "stream", "data": {
                    "flv": stream.get("flv_pull_url") or {},
                    "hls": stream.get("hls_pull_url") or "",
                }})
        return handler

    def _make_handler(self, name: str, client: TikTokLiveClient):
        async def handler(event):
            if self.client is not client:
                return
            await self.send({"type": "event", "name": name,
                             "data": serialize_event(event), "ts": time.time()})
        return handler

    async def stop(self, notify: bool = True):
        client, task = self.client, self.task
        self.client, self.task = None, None
        if client is not None:
            with contextlib.suppress(Exception):
                await client.disconnect(close_client=True)
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        if notify and client is not None:
            await self.send({"type": "status", "state": "disconnected", "message": "Disconnected"})


app = FastAPI(title="TikTok Live Monitor")


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/mpegts.min.js")
async def mpegts_js():
    return FileResponse(STATIC_DIR / "mpegts.min.js", media_type="application/javascript")


@app.get("/card_front.png")
async def card_front():
    return FileResponse(STATIC_DIR / "card_front.png", media_type="image/png")


@app.get("/brain")
async def brain():
    return FileResponse(STATIC_DIR / "brain.html")


@app.get("/proxy/flv")
async def proxy_flv(url: str):
    """把 TikTok CDN 的 FLV 直播流转发给浏览器(绕过跨域限制)"""
    host = urlparse(url).hostname or ""
    if not url.startswith("https://") or not host.endswith(ALLOWED_STREAM_HOST_SUFFIXES):
        return JSONResponse({"error": "URL not allowed"}, status_code=403)

    async def stream_bytes():
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(15, read=None),
                                         follow_redirects=True) as hc:
                async with hc.stream("GET", url, headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                    "Referer": "https://www.tiktok.com/",
                }) as resp:
                    async for chunk in resp.aiter_bytes(65536):
                        yield chunk
        except (httpx.HTTPError, asyncio.CancelledError):
            return

    return StreamingResponse(stream_bytes(), media_type="video/x-flv",
                             headers={"Cache-Control": "no-store"})


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    session = Session(ws)
    try:
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            action = msg.get("action")
            if action == "connect":
                await session.connect(msg.get("url", ""))
            elif action == "disconnect":
                await session.stop()
    except WebSocketDisconnect:
        pass
    finally:
        session.closed = True
        await session.stop(notify=False)


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="127.0.0.1", port=port)
