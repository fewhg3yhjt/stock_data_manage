from __future__ import annotations

import re
import socket
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Protocol, Sequence
from threading import Lock, BoundedSemaphore
from time import monotonic, sleep
from contextlib import contextmanager
from math import isfinite
import json
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit, parse_qsl
from urllib.request import Request, urlopen

from .contracts import EndpointContract, FailureClass, HttpResponse, ProviderContractError
from ..storage.raw import RawObjectStore, sanitized_url, sanitized_headers, sanitized_metadata


class HttpTransport(Protocol):
    def get(self, url: str, *, params: Mapping[str, str], timeout_seconds: float) -> HttpResponse: ...


@dataclass(slots=True)
class RequestsTransport:
    """The verified Tencent helper uses requests.request, its UA, Referer and tuple timeout."""
    headers: Mapping[str, str]
    use_session: bool = False
    _session: Any = field(default=None, init=False, repr=False)

    def get(self, url, *, params, timeout_seconds):
        import requests
        if self.use_session:
            # Create inside the existing capture scope so the saved probe retry policy applies.
            if self._session is None:
                self._session = requests.Session()
                self._session.headers.update(self.headers)
            response = self._session.get(url, params=params, timeout=timeout_seconds, allow_redirects=True)
        else:
            response = requests.request("GET", url, params=params, headers=dict(self.headers),
                                        timeout=timeout_seconds, allow_redirects=True)
        response.raise_for_status()
        return HttpResponse(response.status_code, dict(response.headers), response.content)

    def close(self):
        if self._session is not None:
            self._session.close()
            self._session = None


_REQUEST_CAPTURE_LOCK = Lock()


class HostPausedError(ConnectionError):
    """Same failure boundary as the successful rate-limited source probe."""


@contextmanager
def captured_requests(store, *, provider, endpoint, scope, code_version, pacer,
                      replay_manifest=None, evidence_roots=(), max_age_seconds=0, sdk_retry_policy=False,
                      probe_host_pause=False, require_empty_post_body=False, response_validator=None, cache_ignored_query_parameters=(),
                      native_transport=None, match_request_body=False):
    """Serialized single-input capture/replay. Session identity, proxies and request arguments are retained.

    SDK session policy matches the saved conservative probe. Internal urllib3 retries
    remain opaque: this scope must never be labeled physical_request enforcement.
    """
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    events = []
    replay_records = []
    if replay_manifest:
        replay_manifest = Path(replay_manifest)
        for number, line in enumerate(replay_manifest.read_text(encoding="utf-8").splitlines(), 1):
            record = json.loads(line)
            if record.get("event") == "http_response":
                replay_records.append((number, record))
    used = set()
    consecutive_errors, paused_hosts = {}, {}
    with _REQUEST_CAPTURE_LOCK:
        original_send, original_init = requests.Session.send, requests.Session.__init__
        scope = sanitized_metadata(scope)
        def make_response(request, record, body):
            response = requests.Response()
            response.status_code = int(record["status_code"])
            response.headers.update(record.get("response_headers", {}))
            response.encoding = record.get("encoding") or record.get("response_encoding") or "utf-8"
            response._content = body
            response._content_consumed = True
            response.url, response.request = request.url, request
            return response

        def send(session, request, **kwargs):
            native_call = kwargs.pop('_native_call', None)
            native_options = kwargs.pop('_native_options', None)
            request_scope = scope
            body_metadata = {}
            if match_request_body:
                import hashlib
                body = request.body or b""
                if isinstance(body, str):
                    body = body.encode("utf-8")
                if not isinstance(body, bytes):
                    raise ValueError("request-body matching requires a fixed byte or string body")
                digest = hashlib.sha256(body).hexdigest()
                body_metadata = {"request_body_bytes": len(body), "request_body_sha256": digest}
                if "application/x-www-form-urlencoded" in request.headers.get("Content-Type", ""):
                    body_metadata["request_form"] = sanitized_metadata(dict(parse_qsl(body.decode("utf-8"), keep_blank_values=True)))
                request_scope = {**scope, "request_body_sha256": digest}
            # The observed social-financing SDK uses a bodyless POST, not an arbitrary POST query.
            if require_empty_post_body and (request.method != "POST" or request.body not in (None, b"", "")):
                raise ValueError("source contract requires a bodyless POST")
            host = (urlsplit(request.url).hostname or "unknown").lower()
            if probe_host_pause and host in paused_hosts:
                store.append_event({"event": "host_paused", "provider": provider, "endpoint": endpoint,
                    "host": host, "reason": paused_hosts[host], "scope": scope, "code_version": code_version,
                    "fetched_at_utc": datetime.now(timezone.utc).isoformat()})
                raise HostPausedError(f"Probe host paused: {host}: {paused_hosts[host]}")
            def transport_error():
                if probe_host_pause:
                    consecutive_errors[host] = consecutive_errors.get(host, 0) + 1
                    if consecutive_errors[host] >= 2:
                        paused_hosts[host] = "two consecutive transport errors"
            source_ref = None
            mode = "live"
            if replay_manifest:
                matching = next(((n, r) for n, r in replay_records if n not in used
                                 and r.get("method") == request.method
                                 and sanitized_url(r["url"]) == sanitized_url(request.url)
                                 and (not match_request_body or
                                      (r.get("request_body_sha256") or r.get("request_options", {}).get("request_body_sha256")
                                       or r.get("scope", {}).get("request_body_sha256")) == digest)
                                 and (not require_empty_post_body or
                                      (r.get("request_body_sha256") or r.get("request_options", {}).get("request_body_sha256")) ==
                                      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")), None)
                if matching is None:
                    raise ValueError("no exact archived request match; replay never falls back to network")
                number, record = matching
                used.add(number)
                source_ref = {"manifest": str(replay_manifest.resolve()), "line": number,
                              "fetched_at_utc": record.get("fetched_at_utc")}
                if record.get('redacted'):
                    source_ref.update(redacted=True, original_body_sha256=record.get('original_body_sha256'),
                                      exact_original_bytes_retained=False)
                if record.get("outcome") == "transport_error":
                    transport_error()
                    store.append_event({"event": "http_response", "mode": "replay", "outcome": "transport_error",
                                        "url": sanitized_url(request.url), "method": request.method,
                                        "request_headers": sanitized_headers(request.headers),
                                        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
                                        "scope": request_scope, "code_version": code_version,
                                        "provider": provider, "endpoint": endpoint, "source_ref": source_ref,
                                        "error_type": record.get("error_type")})
                    raise requests.ConnectionError("archived transport failure")
                response = make_response(request, record, RawObjectStore.read_response(replay_manifest, record))
                mode = "replay"
            else:
                cached = RawObjectStore.find_cached_response(evidence_roots, url=request.url, method=request.method,
                    scope=request_scope, code_version=code_version, max_age_seconds=max_age_seconds,
                    ignored_query_parameters=cache_ignored_query_parameters)
                if cached:
                    manifest, record, body = cached
                    response = make_response(request, record, body)
                    source_ref, mode = {"manifest": str(manifest.resolve()), "sha256": record["body_sha256"],
                                        "fetched_at_utc": record.get("fetched_at_utc")}, "cached"
                else:
                    host = urlsplit(request.url).hostname or "unknown"
                    pacer.configure(host, 3, 1)
                    try:
                        with pacer.request(host):
                            response = native_call() if native_call is not None else original_send(session, request, **kwargs)
                    except Exception as exc:
                        transport_error()
                        store.append_event({"event": "http_response", "mode": "live", "outcome": "transport_error",
                            "url": sanitized_url(request.url), "method": request.method,
                            "request_headers": sanitized_headers(request.headers), "scope": request_scope,
                            "provider": provider, "endpoint": endpoint, "code_version": code_version,
                            "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
                            **({"request_options": body_metadata} if body_metadata else {}),
                            "error_type": type(exc).__name__})
                        raise
            event = store.record_response(response=response, url=request.url, method=request.method,
                request_headers=request.headers, scope=request_scope, provider=provider, endpoint=endpoint,
                code_version=code_version, mode=mode, source_ref=source_ref,
                request_options=native_options if native_options is not None else {"timeout": kwargs.get("timeout"), "allow_redirects": kwargs.get("allow_redirects", True),
                                 "trust_env": session.trust_env, "proxies": kwargs.get("proxies", {}),
                                 "sdk_retry_policy": sdk_retry_policy,
                                 "probe_host_pause": probe_host_pause,
                                 **body_metadata,
                                 **({"cache_ignored_query_parameters":list(cache_ignored_query_parameters)} if cache_ignored_query_parameters else {}),
                                 **({"request_body_bytes":0,"request_body_sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}
                                    if require_empty_post_body else {})})
            events.append(event)
            if probe_host_pause:
                status = int(response.status_code or 0)
                if status in {403, 429}:
                    paused_hosts[host] = f"HTTP {status}; no further requests to this host in this run"
                elif status >= 500:
                    consecutive_errors[host] = consecutive_errors.get(host, 0) + 1
                    if consecutive_errors[host] >= 2:
                        paused_hosts[host] = f"{consecutive_errors[host]} consecutive server errors"
                else:
                    consecutive_errors[host] = 0
            if response_validator is not None:
                response_validator(response)
            return response

        class ConservativeRetry(Retry):
            def get_backoff_time(self):
                return max(5.0, super().get_backoff_time()) if self.history else 0

            def is_retry(self, method, status_code, has_retry_after=False):
                return False if status_code in {403, 429} else super().is_retry(method, status_code, has_retry_after)

        def init(session, *args, **kwargs):
            original_init(session, *args, **kwargs)
            if sdk_retry_policy:
                policy = ConservativeRetry(total=2, connect=2, read=2, status=2, backoff_factor=5,
                    backoff_max=300, status_forcelist=(500, 502, 503, 504), allowed_methods=frozenset({"GET", "POST"}),
                    respect_retry_after_header=True, raise_on_status=False)
                session.mount("https://", HTTPAdapter(max_retries=policy))
                session.mount("http://", HTTPAdapter(max_retries=policy))
        native_restore = None
        if native_transport == 'curl_cffi':
            import curl_cffi.requests as curl_requests
            from types import SimpleNamespace
            original_native = curl_requests.Session.request
            def curl_request(session, method, url, *args, **kwargs):
                if args: raise ValueError('unexpected positional curl SDK arguments')
                prepared=requests.Request(method,url,params=kwargs.get('params'),headers=kwargs.get('headers')).prepare()
                return send(SimpleNamespace(trust_env=getattr(session,'trust_env',True)),prepared,
                    _native_call=lambda:original_native(session,method,url,**kwargs),
                    _native_options=dict(transport='original curl_cffi SDK request boundary',**sanitized_metadata(kwargs)))
            curl_requests.Session.request = curl_request
            native_restore = lambda: setattr(curl_requests.Session,'request',original_native)
        elif native_transport == 'pandas_urllib':
            import io
            import pandas.io.common as common
            from types import SimpleNamespace
            original_native = common.urlopen
            def excel_urlopen(request):
                prepared=SimpleNamespace(url=request.full_url,method=request.get_method(),headers=dict(request.header_items()),body=None)
                def delegate():
                    with original_native(request) as response:
                        return SimpleNamespace(content=response.read(),status_code=response.status,headers=dict(response.headers),encoding=None)
                response=send(SimpleNamespace(trust_env=True),prepared,_native_call=delegate,
                    _native_options={'transport':'original pandas urllib request boundary'})
                if response.status_code>=400:raise HTTPError(request.full_url,response.status_code,'archived HTTP failure',response.headers,None)
                buffered=io.BytesIO(response.content);buffered.headers=response.headers
                return buffered
            common.urlopen = excel_urlopen
            native_restore = lambda: setattr(common,'urlopen',original_native)
        elif native_transport is not None:
            raise ValueError('unsupported original SDK transport')
        requests.Session.send = send
        requests.Session.__init__ = init
        try:
            yield events
        finally:
            if native_restore is not None: native_restore()
            requests.Session.send, requests.Session.__init__ = original_send, original_init


class RequestPacer:
    """Pace physical calls without changing the delegated transport/session/retries."""

    def __init__(self, *, clock=monotonic, wait=sleep):
        self.clock = clock
        self.wait = wait
        self._groups = {}

    def configure(self, group: str, interval_seconds: float, concurrency: int = 1) -> None:
        if not group or not isfinite(interval_seconds) or interval_seconds < 0 or concurrency < 1:
            raise ValueError("invalid request pacing policy")
        if group in self._groups:
            current = self._groups[group]
            if current["started"]:
                if interval_seconds > current["interval"] or concurrency < current["concurrency"]:
                    raise ValueError("configure shared request policies before issuing requests")
                return
            interval_seconds = max(interval_seconds, current["interval"])
            concurrency = min(concurrency, current["concurrency"])
        self._groups[group] = {
            "interval": interval_seconds, "concurrency": concurrency,
            "lock": Lock(), "semaphore": BoundedSemaphore(concurrency), "last_start": None, "started": False,
        }

    @contextmanager
    def request(self, group: str):
        policy = self._groups[group]
        with policy["semaphore"]:
            with policy["lock"]:
                if policy["last_start"] is not None:
                    remaining = policy["interval"] - (self.clock() - policy["last_start"])
                    if remaining > 0:
                        self.wait(remaining)
                policy["last_start"] = self.clock()
                policy["started"] = True
            yield


@dataclass(slots=True)
class PacedTransport:
    transport: HttpTransport
    pacer: RequestPacer
    group: str

    def get(self, url: str, *, params: Mapping[str, str], timeout_seconds: float) -> HttpResponse:
        with self.pacer.request(self.group):
            return self.transport.get(url, params=params, timeout_seconds=timeout_seconds)


@dataclass(slots=True)
class UrlLibTransport:
    user_agent: str = "stock-data-manage/0.1"

    def get(self, url: str, *, params: Mapping[str, str], timeout_seconds: float) -> HttpResponse:
        request = Request(
            f"{url}?{urlencode(params)}",
            headers={"User-Agent": self.user_agent, "Accept": "application/json,text/plain,*/*"},
        )
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                return HttpResponse(int(response.status), dict(response.headers.items()), response.read())
        except HTTPError as exc:
            return HttpResponse(exc.code, dict(exc.headers.items()) if exc.headers else {}, exc.read())
        except (TimeoutError, socket.timeout) as exc:
            raise ProviderContractError("provider request timed out", FailureClass.TIMEOUT, retryable=True) from exc
        except URLError as exc:
            raise ProviderContractError(
                f"provider connection failed: {exc.reason}", FailureClass.CONNECTION, retryable=True
            ) from exc


@dataclass(frozen=True, slots=True)
class RealtimeFetchResponse:
    rows: tuple[Mapping[str, Any], ...]
    requested_symbols: tuple[str, ...]
    response_statuses: tuple[int, ...] = ()
    field_semantics: tuple[str, ...] = ()
    units: tuple[str, ...] = ()
    returned_row_count: int | None = None
    returned_first_key: str | None = None
    returned_last_key: str | None = None

    @property
    def returned_symbols(self) -> frozenset[str]:
        return frozenset(str(row["symbol"]) for row in self.rows)


@dataclass(frozen=True, slots=True)
class SnapshotFetchResponse:
    rows: tuple[Mapping[str, Any], ...]
    requested_symbols: tuple[str, ...]
    source_rows: tuple[Mapping[str, Any], ...] | None = None
    source_url: str | None = None

    @property
    def coverage_denominator(self) -> int:
        return len(self.requested_symbols)

    @property
    def missing_symbols(self) -> tuple[str, ...]:
        return tuple(symbol for symbol in self.requested_symbols if symbol not in self.returned_symbols)

    @property
    def returned_symbols(self) -> frozenset[str]:
        return frozenset(str(row["symbol"]) for row in self.rows)


def parse_provider_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    text = str(value)
    if len(text) == 12 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d%H%M")
    if len(text) == 14 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d%H%M%S")
    return datetime.fromisoformat(text.replace("/", "-"))


def parse_tencent_snapshot_line(line: str) -> dict[str, Any] | None:
    match = re.match(r"\s*v_([^=]+)=\"(.*?)\";?\s*$", line)
    if match is None:
        return None
    symbol, body = match.groups()
    parts = body.split("~")

    def item(index: int) -> str | None:
        return None if index >= len(parts) or parts[index] in {"", "-"} else parts[index]

    quote_time = item(30)
    if quote_time is None or len(quote_time) < 8:
        return None
    timestamp = parse_provider_datetime(quote_time)
    return {
        "symbol": symbol, "trade_date": timestamp.date().isoformat(), "bar_time": timestamp.isoformat(),
        "name": item(1), "open": item(5), "high": item(33), "low": item(34), "close": item(3),
        "pre_close": item(4), "volume": item(6), "amount": item(37), "volume_unit": "lot",
        "amount_unit": "wan_cny", "quote_time": quote_time,
    }


def parse_sina_snapshot_line(line: str) -> dict[str, Any] | None:
    match = re.match(r'\s*var\s+hq_str_([^=]+)="(.*?)";?\s*$', line)
    if match is None:
        return None
    symbol, body = match.groups()
    parts = body.split(",")
    if len(parts) < 32 or not parts[30] or not parts[31]:
        return None
    try:
        timestamp = datetime.strptime(f"{parts[30]} {parts[31]}", "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    return {
        "symbol": symbol, "trade_date": timestamp.date().isoformat(), "bar_time": timestamp.isoformat(),
        "name": parts[0] or None, "open": parts[1] or None, "high": parts[4] or None,
        "low": parts[5] or None, "close": parts[3] or None, "pre_close": parts[2] or None,
        "volume": parts[8] or None, "amount": parts[9] or None, "volume_unit": "share",
        "amount_unit": "cny", "quote_time": f"{parts[30]} {parts[31]}",
    }


def tdx_code(symbol: str) -> str:
    text = str(symbol).lower()
    return text[2:] if len(text) >= 8 and text[:2] in {"sh", "sz", "bj"} else text


def call_tdx_bars(client: object, code: str, category: int, count: int) -> object:
    bars = getattr(client, "bars", None)
    if not callable(bars):
        raise ProviderContractError("TDX client has no bars method", FailureClass.SCHEMA_CHANGED, retryable=False)
    try:
        return bars(symbol=code, frequency=category, offset=count)
    except TypeError:
        return bars(symbol=code, category=category, offset=count)


def tdx_records(value: object) -> list[object]:
    if value is None:
        return []
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        try:
            return list(to_dict("records") or [])
        except TypeError:
            return list(to_dict(orient="records") or [])
    if isinstance(value, Mapping):
        return [value]
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return list(value)
    return []


def tdx_value(item: object, *names: str) -> object:
    if isinstance(item, Mapping):
        for name in names:
            if name in item:
                return item[name]
        return None
    for name in names:
        if hasattr(item, name):
            return getattr(item, name)
    return None

V39_UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36'

def _v39_http(url, params=None, data=None, headers=None, method='GET', timeout=(10, 40), allow_status=(), allow_redirects=True):
    """非东财的 HTTP 请求：带浏览器 UA。网络错误、非 2xx 一律抛 RuntimeError（不把错误页当数据）；
    allow_status 里的状态码（源用 404 表示「当天没发布」时）原样返回，由调用方判断。"""
    import requests
    merged = {'User-Agent': V39_UA}
    merged.update(headers or {})
    try:
        response = requests.request(method, url, params=params, data=data, headers=merged, timeout=timeout, allow_redirects=allow_redirects)
        if response.status_code not in allow_status:
            response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f'请求 {url} 失败: {type(exc).__name__}: {exc}') from exc
    return response
