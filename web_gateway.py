"""Serve the Web UI and forward its same-origin API to the local BLE server."""

import argparse
import ipaddress
import re
from pathlib import Path

from aiohttp import ClientError, ClientSession, ClientTimeout, DummyCookieJar, web
from multidict import CIMultiDict
from yarl import URL


WEB_ROOT = Path(__file__).resolve().parent / "webui"
MOBILE_UA = re.compile(r"Android|iPhone|iPod|webOS|BlackBerry|Windows Phone", re.I)
HOP_HEADERS = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "proxy-connection", "te", "trailer", "transfer-encoding", "upgrade",
}
BACKEND = web.AppKey("backend", URL)
SESSION = web.AppKey("backend_session", ClientSession)


def backend_origin(value: str) -> URL:
    try:
        url = URL(value)
        host = url.host
        loopback = host == "localhost" or ipaddress.ip_address(host).is_loopback
        valid = (
            loopback and url.scheme == "http" and url.user is None
            and url.path == "/" and not url.query_string and not url.fragment
        )
        if valid:
            return url.origin()
    except ValueError:
        pass
    raise argparse.ArgumentTypeError("expected a local HTTP origin, e.g. http://127.0.0.1:18199")


def forwarded_headers(headers, exclude=()):
    blocked = HOP_HEADERS.union(exclude)
    for value in headers.getall("Connection", ()):
        blocked.update(token.strip().lower() for token in value.split(","))
    return CIMultiDict((name, value) for name, value in headers.items() if name.lower() not in blocked)


async def backend_session(app):
    # The BLE service sends a keepalive every 15 seconds. Bound idle reads,
    # while allowing healthy event streams to stay open indefinitely.
    timeout = ClientTimeout(total=None, connect=5, sock_read=45)
    async with ClientSession(
        timeout=timeout,
        auto_decompress=False,
        cookie_jar=DummyCookieJar(),
        skip_auto_headers={"Accept-Encoding"},
        trust_env=False,
    ) as session:
        app[SESSION] = session
        yield


async def proxy_api(request):
    # Reject paths that could normalize outside the API prefix on the upstream.
    if (
        not request.rel_url.raw_path.startswith("/api/")
        or "\\" in request.path
        or any(part in {".", ".."} for part in request.path.split("/"))
    ):
        raise web.HTTPNotFound()

    origin = request.headers.get("Origin")
    if origin is not None and origin != f"{request.scheme}://{request.host}":
        raise web.HTTPForbidden(text="The BLE API requires a same-origin request.")

    target = URL(f"{request.app[BACKEND]}{request.rel_url.raw_path_qs}", encoded=True)
    body = await request.read() if request.can_read_body else None
    response = None
    try:
        async with request.app[SESSION].request(
            request.method,
            target,
            headers=forwarded_headers(request.headers, {"host"}),
            data=body,
            allow_redirects=False,
        ) as upstream:
            response = web.StreamResponse(
                status=upstream.status,
                headers=forwarded_headers(upstream.headers),
            )
            await response.prepare(request)
            async for chunk in upstream.content.iter_chunked(64 * 1024):
                await response.write(chunk)
            await response.write_eof()
            return response
    except ConnectionResetError:
        if response is not None:
            return response
    except (ClientError, TimeoutError):
        if response is not None:
            # Headers have already gone out. Close the truncated stream so
            # EventSource reconnects and a partial CSV is detected as incomplete.
            if request.transport is not None:
                request.transport.close()
            return response

    return web.json_response({"ok": False, "error": "无法连接本地 BLE 服务"}, status=502)


def create_app(web_root: Path, backend: URL) -> web.Application:
    app = web.Application(handler_args={"auto_decompress": False})
    app[BACKEND] = backend
    app.cleanup_ctx.append(backend_session)

    async def page(request):
        name = request.match_info.get("name")
        if name is None:
            ua = request.headers.get("User-Agent", "")
            name = "phone" if MOBILE_UA.search(ua) else "index"
        return web.FileResponse(web_root / f"{name}.html", headers={"Cache-Control": "no-cache"})

    app.router.add_get("/", page)
    app.router.add_get(r"/{name:index|phone|config}.html", page)
    app.router.add_static("/static/", web_root / "static")
    app.router.add_route("*", "/api/{tail:.*}", proxy_api)
    return app


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8199)
    parser.add_argument("--backend", type=backend_origin, default="http://127.0.0.1:18199")
    args = parser.parse_args()
    web.run_app(create_app(WEB_ROOT, args.backend), host=args.host, port=args.port, handler_cancellation=True)


if __name__ == "__main__":
    main()
