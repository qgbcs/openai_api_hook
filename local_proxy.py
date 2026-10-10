#!/usr/bin/env python3
"""本地聚合代理：端口 2255 同时提供
    - RPC 接口（复用 server_http.RPCRequestHandler，支持 /r=dir() 等）
    - /v1/* 反向代理到 Sapiens AI API（限流 + 429 自动重试）
    - 实时速率显示 + 动态限速调整
    - 请求/响应详细日志

用法:
    python local_proxy.py [--port 2255] [--upstream URL] [--rate 0.15] [--burst 10]
"""

import argparse
import json
import logging
import sys
import time
import threading
import urllib.request
import urllib.error
import os
import statistics
import socket
import ssl
from collections import deque
from datetime import datetime
from http.client import HTTPConnection, HTTPSConnection
from socketserver import ThreadingMixIn


# ─── 导入 server_http ─────────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'multi_mqtt'))
try:
    import server_http
    from server_http import RPCRequestHandler, ThreadedHTTPServer
except ImportError as e:
    print(f"错误：无法导入 server_http: {e}", file=sys.stderr)
    sys.exit(1)


# ─── 时间戳工具 ──────────────────────────────────────────────────────────────

def _ts():
    """返回带毫秒的时间戳字符串。"""
    return datetime.now().strftime('%H:%M:%S.%f')[:-3]


def set_console_title(text):
    """把实时状态写入控制台窗口标题，不占用 stdout。
    Windows 用 SetConsoleTitleW；其他平台用 ANSI OSC 转义。
    """
    if sys.platform == "win32":
        try:
            import ctypes
            ctypes.windll.kernel32.SetConsoleTitleW(str(text))
            return
        except Exception:
            pass
    sys.stdout.write(f"\033]0;{text}\007")
    sys.stdout.flush()


# ─── 详细日志 ────────────────────────────────────────────────────────────────

class DetailLogger:
    """请求/响应日志。默认紧凑单行：请求一行、回复一行。
    RPC 可设置 _detail_logger.verbose = True 强制展开全部 header 与 body。
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._active = {}  # request_id -> {start, ts, method, url}
        self.verbose = False

    def log_request(self, req_id, method, url, headers, body=None):
        with self._lock:
            self._active[req_id] = {
                'start': time.monotonic(), 'ts': _ts(),
                'method': method, 'url': url,
            }
            n = len(body) if body else 0
            print(f"[{_ts()}] >>> #{req_id} {method} {url} body={n}B")
            if self.verbose:
                for k, v in headers.items():
                    print(f"    {k}: {v}")
                if body:
                    self._print_body(body)

    def log_response(self, req_id, status, headers, body):
        with self._lock:
            info = self._active.pop(req_id, None)
            if not info:
                return
            elapsed = time.monotonic() - info['start']
            usage = extract_usage(body)
            tok = ""
            if usage:
                tok = (f" tok={usage.get('prompt_tokens')}+"
                       f"{usage.get('completion_tokens')}="
                       f"{usage.get('total_tokens')}")
            print(f"[{info['ts']}] <<< #{req_id} {status} {self._status_text(status)} "
                  f"{elapsed*1000:.0f}ms{tok}")
            if self.verbose:
                for k, v in headers:
                    if k.lower() not in ('transfer-encoding', 'connection'):
                        print(f"    {k}: {v}")
                if body:
                    self._print_body(body)

    def log_error(self, req_id, error, elapsed):
        with self._lock:
            # 不 pop：429 重试后还要记录最终响应，pop 会导致成功响应日志丢失
            info = self._active.get(req_id)
            ts = info['ts'] if info else _ts()
            print(f"[{ts}] !!! #{req_id} {elapsed*1000:.0f}ms {error}")

    @staticmethod
    def _print_body(body):
        try:
            print(f"    body: {body.decode('utf-8', errors='replace')[:500]}")
        except Exception:
            print(f"    body: [{len(body)}B binary]")

    @staticmethod
    def _status_text(code):
        texts = {
            200: 'OK', 201: 'Created', 204: 'No Content',
            400: 'Bad Request', 401: 'Unauthorized', 403: 'Forbidden',
            429: 'Too Many Requests',
            500: 'Internal Server Error', 502: 'Bad Gateway', 503: 'Service Unavailable',
        }
        return texts.get(code, 'Unknown')


# ─── Token 用量解析 ─────────────────────────────────────────────────────────

def extract_usage(body):
    """从响应体提取 OpenAI usage。兼容流式 SSE 与普通 JSON。

    返回 {'prompt_tokens', 'completion_tokens', 'total_tokens'} 或 None。
    """
    if not body:
        return None
    try:
        text = body.decode('utf-8', errors='replace')
    except Exception:
        return None

    # 流式：逐行扫描 "data: {...}"，取最后一个带 usage 的 chunk
    usage = None
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith('data:'):
            continue
        payload = line[5:].strip()
        if not payload or payload == '[DONE]':
            continue
        try:
            obj = json.loads(payload)
        except (ValueError, TypeError):
            continue
        if isinstance(obj, dict) and obj.get('usage'):
            usage = obj['usage']
    if usage:
        return usage

    # 非流式：整体 JSON
    try:
        obj = json.loads(text)
    except (ValueError, TypeError):
        return None
    return obj.get('usage') if isinstance(obj, dict) else None


# ─── 统计收集器 ──────────────────────────────────────────────────────────────

class StatsCollector:
    def __init__(self, window=60):
        self.window = window
        self._lock = threading.Lock()
        self._requests = deque()
        self._errors_429 = deque()
        self._error_count = 0
        self._success_count = 0
        # 累计 token 用量与最近一次请求的用量
        self._prompt_tokens = 0
        self._completion_tokens = 0
        self._total_tokens = 0
        self._last_usage = None

    def record(self, method, path, status, duration, usage=None):
        now = time.monotonic()
        with self._lock:
            self._requests.append((now, method, path, status, duration))
            while self._requests and self._requests[0][0] < now - self.window:
                self._requests.popleft()
            if status == 429:
                self._error_count += 1
            elif 200 <= status < 400:
                self._success_count += 1
            # 仅对带 usage 的成功响应累计（429/重试无 usage）
            if usage:
                self._last_usage = dict(usage)
                self._prompt_tokens += int(usage.get('prompt_tokens', 0) or 0)
                self._completion_tokens += int(usage.get('completion_tokens', 0) or 0)
                self._total_tokens += int(usage.get('total_tokens', 0) or 0)

    def record_429(self, path, headers):
        now = time.monotonic()
        with self._lock:
            self._errors_429.append((now, path, headers))
            while self._errors_429 and self._errors_429[0][0] < now - self.window:
                self._errors_429.popleft()

    def get_rate(self):
        with self._lock:
            now = time.monotonic()
            recent = [r for r in self._requests if r[0] >= now - self.window]
            if not recent:
                return 0.0
            span = recent[-1][0] - recent[0][0]
            return len(recent) / max(span, 1.0)

    def get_status(self, proxy_ref):
        with self._lock:
            now = time.monotonic()
            recent = list(self._requests)

            # 计算最近 1s/10s/60s 的请求数
            rates = {}
            for sec in [1, 10, 60]:
                count = len([r for r in recent if r[0] >= now - sec])
                rates[f"last_{sec}s"] = count / sec

            # 计算瞬时速率（最后两次请求的间隔）
            instant_rate = 0.0
            if len(recent) >= 2:
                last_ts = recent[-1][0]
                prev_ts = recent[-2][0]
                gap = last_ts - prev_ts
                if 0 < gap <= 300:  # 只考虑 5 分钟内的间隔
                    instant_rate = 1.0 / gap

            total = self._success_count + self._error_count
            error_rate = self._error_count / total if total > 0 else 0.0

            recent_429 = [(t, p, h) for t, p, h in self._errors_429
                         if t >= now - self.window]

            latencies = [r[4] for r in recent if r[4] > 0]
            latency_stats = {}
            if latencies:
                latencies.sort()
                latency_stats = {
                    "min": latencies[0],
                    "max": latencies[-1],
                    "avg": statistics.mean(latencies),
                    "p95": latencies[int(len(latencies) * 0.95)] if latencies else 0,
                }

            return {
                "instant_rate": instant_rate,
                "rate_per_sec": rates,
                "total_requests": len(recent),
                "success_count": self._success_count,
                "error_count": self._error_count,
                "error_rate": error_rate,
                "latency": latency_stats,
                "recent_429_count": len(recent_429),
                "recent_429": [
                    {"path": p, "headers": {k: v for k, v in h}, "timestamp": t}
                    for t, p, h in recent_429[-5:]
                ],
                "bucket": {
                    "tokens": proxy_ref.bucket._tokens if proxy_ref.bucket else None,
                    "rate": proxy_ref.bucket.rate if proxy_ref.bucket else None,
                    "burst": proxy_ref.bucket.burst if proxy_ref.bucket else None,
                } if proxy_ref.bucket else None,
                "last_usage": self._last_usage,
                "usage_total": {
                    "prompt_tokens": self._prompt_tokens,
                    "completion_tokens": self._completion_tokens,
                    "total_tokens": self._total_tokens,
                },
            }

    def reset(self):
        with self._lock:
            self._requests.clear()
            self._errors_429.clear()
            self._error_count = 0
            self._success_count = 0
            self._prompt_tokens = 0
            self._completion_tokens = 0
            self._total_tokens = 0
            self._last_usage = None


# ─── 令牌桶限速 ───────────────────────────────────────────────────────────────

class TokenBucket:
    def __init__(self, rate, burst):
        self.rate = float(rate)
        self.burst = float(burst)
        self._tokens = float(burst)
        self._last = time.monotonic()
        self._lock = threading.Lock()
        # 软限速：余量低于一半时按缺口比例渐进加延时，避免后半段瞬间耗尽触发 429
        # 0 表示关闭；可由 RPC 调整，如 bucket.soft_delay_max = 30
        self.soft_delay_max = 15.0

    def acquire(self):
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
            self._last = now
            if self._tokens < 1.0:
                wait = (1.0 - self._tokens) / self.rate
                time.sleep(wait)
                self._tokens = 0.0
                self._last = time.monotonic()
                return
            self._tokens -= 1.0
            # 取到令牌后检查余量：低于一半则按缺口比例额外 sleep（余量越少延时越长）
            half = self.burst / 2.0
            if self.soft_delay_max > 0 and self._tokens < half:
                deficit = 1.0 - (self._tokens / half)  # 0（刚好一半）→ 1（余量为 0）
                extra = deficit * self.soft_delay_max
                if extra > 0:
                    logging.warning(
                        "[%s] [soft] 令牌余量 %.1f/%.0f 不足一半，正在主动延迟 %.1fs",
                        _ts(), self._tokens, self.burst, extra
                    )
                    time.sleep(extra)
                    now = time.monotonic()
                    self._tokens = min(self.burst, self._tokens + (now - self._last) * self.rate)
                    self._last = now


# ─── 反向代理转发 ─────────────────────────────────────────────────────────────

MAX_RETRIES = 3
BASE_DELAY = 1.0
# 429 重试参数
RATE_LIMIT_MAX_DELAY = 60.0   # 单次最长等待秒数
RATE_LIMIT_BACKOFF_CAP = 8   # 指数退避最大倍数（BASE_DELAY * 2^8 = 256s，取 min 与 RATE_LIMIT_MAX_DELAY）
RATE_LIMIT_TOTAL_TIMEOUT = 90.0  # 单个请求 429 重试累计总时限（秒），超过则透传 429 让调用方快速失败，避免挂起超过调用方自身超时
_detail_logger = DetailLogger()
_req_counter = 0
_req_lock = threading.Lock()


def _next_req_id():
    global _req_counter
    with _req_lock:
        _req_counter += 1
        return _req_counter


def forward_request(bucket, upstream_url, method, headers, body, path, stats):
    """带限速 + 429 自动重试的转发，记录详细日志。

    - 429：挂起调用者指数退避重试；累计等待超过 RATE_LIMIT_TOTAL_TIMEOUT 则透传 429 让调用方快速失败。
    - 5xx / 网络错误：最多重试 MAX_RETRIES 次。
    """
    req_id = _next_req_id()
    attempt = 0              # 5xx / 网络错误重试计数（有上限 MAX_RETRIES）
    rate_limit_attempt = 0   # 429 重试计数
    rate_limit_start = None  # 首次收到 429 的时间点，用于累计总时限

    # 记录请求详情
    _detail_logger.log_request(req_id, method, upstream_url + path, headers, body)

    full_url = upstream_url.rstrip("/") + path
    parsed = urllib.parse.urlparse(full_url)

    while True:
        t0 = time.monotonic()
        bucket.acquire()

        conn = None
        try:
            # 使用 http.client 以便详细记录（而非 urllib）
            if parsed.scheme == "https":
                conn = HTTPSConnection(parsed.hostname, parsed.port or 443, timeout=120)
            else:
                conn = HTTPConnection(parsed.hostname, parsed.port or 80, timeout=120)

            conn.request(method, parsed.path, body=body, headers=headers)
            resp = conn.getresponse()
            resp_body = resp.read()
            resp_headers = [(k, v) for k, v in resp.getheaders()]
            duration = time.monotonic() - t0

            usage = extract_usage(resp_body)
            stats.record(method, path, resp.status, duration, usage)

            # 429：挂起重试（受 RATE_LIMIT_TOTAL_TIMEOUT 总时限约束，超时透传 429）
            if resp.status == 429:
                stats.record_429(path, resp_headers)
                if rate_limit_start is None:
                    rate_limit_start = time.monotonic()
                delay = min(
                    BASE_DELAY * (2 ** min(rate_limit_attempt, RATE_LIMIT_BACKOFF_CAP)),
                    RATE_LIMIT_MAX_DELAY,
                )
                # 优先读取上游 Retry-After 头
                ra = resp.headers.get("Retry-After")
                if ra:
                    try:
                        delay = max(delay, float(ra))
                    except ValueError:
                        pass
                waited = time.monotonic() - rate_limit_start
                if waited + delay > RATE_LIMIT_TOTAL_TIMEOUT:
                    # 已超过调用方可承受的总等待，透传 429 让其快速失败自行重试
                    logging.warning(
                        "[%s] [429] give-up attempt=%d path=%s waited=%.1fs > total_timeout=%.0fs，透传 429",
                        _ts(), rate_limit_attempt, path, waited, RATE_LIMIT_TOTAL_TIMEOUT
                    )
                    _detail_logger.log_response(req_id, 429, resp_headers, resp_body)
                    return 429, resp_headers, resp_body
                logging.warning(
                    "[%s] [429] auto-retry attempt=%d path=%s tokens=%.2f wait=%.1fs waited=%.1fs",
                    _ts(), rate_limit_attempt, path, bucket._tokens, delay, waited
                )
                _detail_logger.log_error(req_id, f"429 Too Many Requests (attempt {rate_limit_attempt})", duration)
                rate_limit_attempt += 1
                time.sleep(delay)
                continue

            _detail_logger.log_response(req_id, resp.status, resp_headers, resp_body)
            return resp.status, resp_headers, resp_body

        except urllib.error.HTTPError as e:
            duration = time.monotonic() - t0
            status = e.code

            # 429：挂起重试（受 RATE_LIMIT_TOTAL_TIMEOUT 总时限约束，超时透传 429）
            if status == 429:
                err_headers = list(e.headers.items())
                stats.record(method, path, 429, duration)
                stats.record_429(path, err_headers)
                if rate_limit_start is None:
                    rate_limit_start = time.monotonic()
                delay = min(
                    BASE_DELAY * (2 ** min(rate_limit_attempt, RATE_LIMIT_BACKOFF_CAP)),
                    RATE_LIMIT_MAX_DELAY,
                )
                ra = e.headers.get("Retry-After")
                if ra:
                    try:
                        delay = max(delay, float(ra))
                    except ValueError:
                        pass
                waited = time.monotonic() - rate_limit_start
                if waited + delay > RATE_LIMIT_TOTAL_TIMEOUT:
                    logging.warning(
                        "[%s] [429] give-up attempt=%d path=%s waited=%.1fs > total_timeout=%.0fs，透传 429",
                        _ts(), rate_limit_attempt, path, waited, RATE_LIMIT_TOTAL_TIMEOUT
                    )
                    err_body = e.read() if not e.fp.closed else b""
                    _detail_logger.log_response(req_id, 429, err_headers, err_body)
                    return 429, err_headers, err_body
                logging.warning(
                    "[%s] [429] auto-retry attempt=%d path=%s tokens=%.2f wait=%.1fs waited=%.1fs",
                    _ts(), rate_limit_attempt, path, bucket._tokens, delay, waited
                )
                _detail_logger.log_error(req_id, f"429 Too Many Requests (attempt {rate_limit_attempt})", duration)
                rate_limit_attempt += 1
                time.sleep(delay)
                continue

            if status >= 500 and attempt < MAX_RETRIES:
                delay = BASE_DELAY * (2 ** attempt)
                logging.warning("[%s] [%d] attempt=%d path=%s wait=%.1fs", _ts(), status, attempt, path, delay)
                attempt += 1
                time.sleep(delay)
                continue

            # 其他状态（含 4xx）直接返回调用者
            err_body = e.read() if not e.fp.closed else b""
            stats.record(method, path, status, duration)
            _detail_logger.log_response(req_id, status, list(e.headers.items()), err_body)
            return status, list(e.headers.items()), err_body

        except (urllib.error.URLError, OSError) as e:
            duration = time.monotonic() - t0
            if attempt < MAX_RETRIES:
                delay = BASE_DELAY * (2 ** attempt)
                stats.record(method, path, 0, duration)
                logging.warning("[%s] [conn] attempt=%d path=%s wait=%.1fs: %s", _ts(), attempt, path, delay, e)
                _detail_logger.log_error(req_id, str(e), duration)
                attempt += 1
                time.sleep(delay)
                continue
            stats.record(method, path, 0, duration)
            raise

        finally:
            # 长时 429 重试循环下及时归还连接，避免 fd 堆积
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass


# ─── 请求完整录制 ─────────────────────────────────────────────────────────────

def _encode_body(body):
    """把请求/响应 body 序列化为可 JSON 存储形式：优先 utf-8 文本，否则 base64。"""
    if body is None:
        return None, None
    if isinstance(body, str):
        return body, None
    try:
        return body.decode("utf-8"), None
    except UnicodeDecodeError:
        import base64
        return None, base64.b64encode(body).decode("ascii")


class RequestRecorder:
    """完整录制经过代理的请求与响应（不截断），落盘为 JSONL。

    完全由 RPC 控制：
        recorder.start(300)               # 录制 300 秒后自动停止
        recorder.start()                  # 一直录制到手动停止
        recorder.stop()                   # 随时停止
        recorder.status()                 # 查看状态 / 文件路径 / 已录条数
    """

    def __init__(self, dirpath=None):
        self.dirpath = dirpath or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "recordings")
        self.active = False
        self.path = None
        self.count = 0
        self.until = None          # monotonic 自动停止时刻，None 表示不限
        self._fh = None
        self._lock = threading.Lock()

    def start(self, duration=None, path=None):
        with self._lock:
            if self.active:
                return f"已在录制中：{self.path}（已录 {self.count} 条）"
            os.makedirs(self.dirpath, exist_ok=True)
            fname = path or f"rec_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
            self.path = fname if os.path.isabs(fname) else os.path.join(self.dirpath, fname)
            self._fh = open(self.path, "a", encoding="utf-8")
            self.count = 0
            self.active = True
            self.until = time.monotonic() + float(duration) if duration else None
            self._write({"event": "recording_start", "duration": duration,
                         "wall": time.time(), "time": datetime.now().isoformat()})
            auto = f"，{duration}s 后自动停止" if duration else "，直到手动 stop()"
            return f"开始录制 → {self.path}{auto}"

    def stop(self):
        with self._lock:
            if not self.active:
                return "当前未在录制"
            self._write({"event": "recording_stop", "count": self.count,
                         "wall": time.time(), "time": datetime.now().isoformat()})
            self._fh.close()
            self._fh = None
            self.active = False
            self.until = None
            return f"录制已停止：{self.path}，共 {self.count} 个请求"

    def status(self):
        self._maybe_auto_stop()
        return {
            "active": self.active, "path": self.path, "count": self.count,
            "remaining": round(self.until - time.monotonic(), 1) if self.until else None,
        }

    def _maybe_auto_stop(self):
        if self.active and self.until is not None and time.monotonic() >= self.until:
            self.stop()

    def _write(self, obj):
        # 调用方已持锁
        self._fh.write(json.dumps(obj, ensure_ascii=False) + "\n")
        self._fh.flush()

    def capture_request(self, seq, method, path, headers, body, client):
        self._maybe_auto_stop()
        if not self.active:
            return
        text, b64 = _encode_body(body)
        rec = {"kind": "request", "seq": seq, "wall": time.time(),
               "time": datetime.now().isoformat(), "method": method, "path": path,
               "client": client, "headers": dict(headers),
               "body_text": text, "body_b64": b64}
        with self._lock:
            if self.active:
                self._write(rec)

    def capture_response(self, seq, status, headers, body, note=None):
        self._maybe_auto_stop()
        if not self.active:
            return
        text, b64 = _encode_body(body)
        rec = {"kind": "response", "seq": seq, "wall": time.time(),
               "time": datetime.now().isoformat(), "status": status,
               "headers": dict(headers), "body_text": text, "body_b64": b64,
               "note": note}
        with self._lock:
            if self.active:
                self._write(rec)
                self.count += 1


# ─── 请求拦截（策略由 RPC 定义）───────────────────────────────────────────────

class Interceptor:
    """通用请求拦截机制。代理只负责在转发前调用 hook，所有判断逻辑由 RPC 定义。

    注册 hook（RPC 代码，非硬编码）::

        def toolcall_guard(req):
            # req: dict(method/path/headers/body_text/json/seq)
            if is_toolcall(req):
                decision = interceptor.review(req)   # 挂起，等待人工/外部 decide
                return decision                     # {'action': 'allow'|'block'|'modify'}
            return None                              # None = 放行到下一个 hook
        interceptor.add_hook(toolcall_guard)

    hook 返回值（action）：
        None                                   放行，继续下一个 hook
        {'action':'allow'}                     显式放行
        {'action':'block','status':403,'body':..., 'headers':{...}}  不转发，直接回 synthetic 响应
        {'action':'modify','headers':{...},'body': <str|bytes>}      修改后再转发

    挂起中的请求由 RPC decide() 放行/拦截/修改：
        interceptor.pending()                  查看等待中的请求
        interceptor.decide(seq_id, 'allow')
        interceptor.decide(seq_id, 'block', status=403, body='rejected')
        interceptor.decide(seq_id, 'modify', body='...')
    """

    def __init__(self):
        self._hooks = []                  # [(name, fn)]
        self._pending = {}                # key -> entry
        self._counter = 0
        self._lock = threading.Lock()
        self.notifier = None              # RPC 定义：挂起时如何"弹窗"通知
        self.default_timeout = 3600       # review 默认超时

    def add_hook(self, fn, name=None):
        name = name or getattr(fn, "__name__", f"hook{len(self._hooks)}")
        with self._lock:
            self._hooks = [(n, f) for n, f in self._hooks if n != name]
            self._hooks.append((name, fn))
        return f"已注册 hook: {name}"

    def remove_hook(self, name):
        with self._lock:
            before = len(self._hooks)
            self._hooks = [(n, f) for n, f in self._hooks if n != name]
        return f"已移除 hook: {name}" if len(self._hooks) < before else f"未找到 hook: {name}"

    def clear_hooks(self):
        with self._lock:
            n = len(self._hooks)
            self._hooks = []
        return f"已清空 {n} 个 hook"

    def list_hooks(self):
        return [n for n, _ in self._hooks]

    def run(self, req):
        """依次执行 hook，返回首个非 None action；异常的 hook 跳过不阻断请求。"""
        for name, fn in list(self._hooks):
            try:
                action = fn(req)
            except Exception:
                logging.exception("[interceptor] hook %s 异常，跳过", name)
                continue
            if action:
                action.setdefault("hook", name)
                return action
        return None

    # ---- 挂起 / 决策（供 RPC hook 调用） ----

    def review(self, req, timeout=None):
        """由 RPC hook 调用：登记请求并阻塞，直到 decide() 给出决策或超时。"""
        with self._lock:
            self._counter += 1
            key = self._counter
        event = threading.Event()
        entry = {"key": key, "req": req, "event": event, "decision": None,
                 "started": time.time(), "summary": self._summarize(req)}
        with self._lock:
            self._pending[key] = entry

        # 通知方式（弹窗/响铃/打印）完全由 RPC 定义
        if self.notifier:
            try:
                self.notifier(entry)
            except Exception:
                logging.exception("[interceptor] notifier 异常")

        event.wait(timeout if timeout is not None else self.default_timeout)
        with self._lock:
            self._pending.pop(key, None)

        decision = entry["decision"]
        if decision is None:
            return {"action": "block", "status": 504,
                    "body": f"interceptor review #{key} timed out"}
        return decision

    def decide(self, key, action, status=None, body=None, headers=None):
        with self._lock:
            entry = self._pending.get(int(key))
        if entry is None:
            return f"未找到等待中的请求 #{key}（可能已超时或被处理）"
        d = {"action": action}
        if status is not None:
            d["status"] = status
        if body is not None:
            d["body"] = body
        if headers is not None:
            d["headers"] = headers
        entry["decision"] = d
        entry["event"].set()
        return f"已对 #{key} 下达决策: {action}"

    def pending(self):
        with self._lock:
            return [{"key": k, "waiting": round(time.time() - e["started"], 1),
                     "summary": e["summary"]} for k, e in self._pending.items()]

    @staticmethod
    def _summarize(req):
        """供 pending 列表展示的精简摘要（不泄露完整大 body）。"""
        text = req.get("body_text") or ""
        j = req.get("json")
        tools = []
        if isinstance(j, dict):
            tools = [t.get("function", {}).get("name") for t in j.get("tools", [])
                     if isinstance(t, dict)]
        return {"path": req.get("path"), "method": req.get("method"),
                "tools": tools, "body_len": len(text)}


# ─── V1ProxyHandler ───────────────────────────────────────────────────────────

class V1ProxyHandler(RPCRequestHandler):
    upstream = ""
    bucket = None
    stats = None
    recorder = None
    interceptor = None
    _seq_lock = threading.Lock()
    _seq = 0

    @classmethod
    def _next_seq(cls):
        with cls._seq_lock:
            cls._seq += 1
            return cls._seq

    def _is_v1_path(self):
        return self.path.startswith("/v1")

    def log_message(self, fmt, *args):
        # 紧凑日志：默认访问日志已由 DetailLogger 的单行请求/回复替代
        pass

    def _send_synthetic(self, status, headers, body):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(status)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Type", (headers or {}).get(
            "Content-Type", "application/json"))
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def _proxy_v1(self, method):
        length = int(self.headers.get("Content-Length", "0") or 0)
        body = self.rfile.read(length) if length > 0 else None

        skip = {
            "host", "content-length", "transfer-encoding",
            "connection", "keep-alive", "proxy-authenticate",
            "proxy-authorization", "te", "trailers", "upgrade",
        }
        headers = {
            k: v for k, v in self.headers.items()
            if k.lower() not in skip
        }
        headers["X-Forwarded-For"] = self.client_address[0]
        headers["X-Real-URL"] = self.path

        seq = self._next_seq()
        body_text, body_b64 = _encode_body(body)
        parsed_json = None
        if body_text:
            try:
                parsed_json = json.loads(body_text)
            except (ValueError, TypeError):
                pass
        req_ctx = {
            "seq": seq, "method": method, "path": self.path,
            "headers": dict(headers), "body": body,
            "body_text": body_text, "body_b64": body_b64, "json": parsed_json,
            "client": self.client_address[0],
        }

        # 1) 录制请求（若开启）
        self.recorder.capture_request(
            seq, method, self.path, headers, body, self.client_address[0])

        # 2) 拦截 hook（策略完全由 RPC 定义）；拦截发生在取令牌之前，不消耗配额
        action = self.interceptor.run(req_ctx) if self.interceptor else None
        if action:
            act = action.get("action", "allow")
            if act in ("block", "respond"):
                s = int(action.get("status", 403))
                sh = action.get("headers", {})
                sb = action.get("body", b"")
                self.recorder.capture_response(seq, s, sh, sb, note="intercepted")
                self._send_synthetic(s, sh, sb)
                return
            if act == "modify":
                if action.get("headers"):
                    headers.update(action["headers"])
                if "body" in action:
                    nb = action["body"]
                    body = nb.encode("utf-8") if isinstance(nb, str) else nb
            # allow → 继续转发

        # 3) 转发到上游
        try:
            status, resp_headers, resp_body = forward_request(
                self.bucket, self.upstream, method, headers, body, self.path, self.stats
            )
        except Exception as e:
            logging.error("upstream error: %s", e)
            self.recorder.capture_response(seq, 502, {}, str(e), note="upstream error")
            self.send_response(502)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(f"502 Bad Gateway: {e}".encode())
            return

        # 4) 录制完整响应（不截断）
        self.recorder.capture_response(seq, status, dict(resp_headers), resp_body)

        # 5) 回写客户端
        self.send_response(status)
        skip_resp = {"transfer-encoding", "connection", "keep-alive", "trailer"}
        for k, v in resp_headers:
            if k.lower() in skip_resp:
                continue
            self.send_header(k, v)
        ct = dict(resp_headers).get("Content-Type", "application/json")
        self.send_header("Content-Type", ct)
        self.end_headers()
        if resp_body:
            self.wfile.write(resp_body)

    def do_GET(self):
        if self._is_v1_path():
            self._proxy_v1("GET")
        else:
            super().do_GET()

    def do_POST(self):
        if self._is_v1_path():
            self._proxy_v1("POST")
        else:
            super().do_POST()

    def do_PUT(self):
        if self._is_v1_path():
            self._proxy_v1("PUT")
        else:
            super().do_PUT()

    def do_DELETE(self):
        if self._is_v1_path():
            self._proxy_v1("DELETE")
        else:
            super().do_DELETE()

    def do_PATCH(self):
        if self._is_v1_path():
            self._proxy_v1("PATCH")
        else:
            super().do_PATCH()

    def do_HEAD(self):
        if self._is_v1_path():
            self._proxy_v1("HEAD")
        else:
            super().do_HEAD()

    def do_OPTIONS(self):
        if self._is_v1_path():
            self._proxy_v1("OPTIONS")
        else:
            super().do_OPTIONS()


# ─── 代理管理器（全局） ───────────────────────────────────────────────────────

class ProxyManager:
    def __init__(self, bucket, stats):
        self.bucket = bucket
        self.stats = stats

    def set_rate(self, rate):
        old_rate = self.bucket.rate
        self.bucket.rate = float(rate)
        logging.info("[%s] [rate] %.2f → %.2f req/s", _ts(), old_rate, rate)
        return f"速率已调整为 {rate} req/s（原 {old_rate}）"

    def set_burst(self, burst):
        old_burst = self.bucket.burst
        self.bucket.burst = int(burst)
        self.bucket._tokens = min(self.bucket._tokens, float(burst))
        logging.info("[%s] [burst] %d → %d", _ts(), old_burst, burst)
        return f"突发已调整为 {burst}（原 {old_burst}）"

    def get_status(self):
        return self.stats.get_status(self)

    def reset_stats(self):
        self.stats.reset()
        return "统计已重置"


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    # 重定向到文件时 stdout 默认块缓冲，会导致日志迟迟不落盘；强制行缓冲
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    parser = argparse.ArgumentParser(description="本地聚合代理：RPC + Sapiens AI 反向代理 + 动态限速")
    parser.add_argument("--port", type=int, default=2255, help="监听端口（默认 2255）")
    # 默认值来自对真实上游的压测（详见 probe_ratelimit.py 与压测日志）：
    # 上游为 60s 滑动窗口、窗口容量 ≈10 个请求，稳态上限 ≈0.167 rps。
    # 取 rate=0.15（60s 稳态占 9 个，留 1 个余量），burst=10（不超过窗口容量）。
    parser.add_argument("--rate", type=float, default=0.15, help="每秒请求数（默认 0.15，压测定值）")
    parser.add_argument("--burst", type=int, default=10, help="突发令牌上限（默认 10，压测定值）")
    parser.add_argument("--upstream", default="https://api.agnes-ai.cn", help="上游 API 地址（不含 /v1）")
    parser.add_argument("--log", default="info", choices=["debug", "info", "warning"])
    args = parser.parse_args()

    bucket = TokenBucket(rate=args.rate, burst=args.burst)
    stats = StatsCollector(window=60)
    proxy = ProxyManager(bucket, stats)
    recorder = RequestRecorder()
    interceptor = Interceptor()

    # executor 必须在 bucket/stats/proxy/recorder/interceptor 都创建好之后才初始化，
    # 否则 globals() 快照不包含这些变量，RPC 会报 NameError
    server_http.RPCRequestHandler.executor = server_http.rpc_executor.PythonExecutor(globals=globals())
    server_http.RPCRequestHandler.key = ''
    server_http.RPCRequestHandler.log_sink = None
    server_http.RPCRequestHandler.websocket_handler = None
    server_http.RPCRequestHandler.websocket_path = '/ws'
    server_http.RPCRequestHandler.websocket_handlers = {}
    server_http.RPCRequestHandler.redirect_root = None
    server_http.RPCRequestHandler.main_loop = None
    server_http.RPCRequestHandler.favicon_bytes = server_http.get_bmp_bytes(rgb=(22, 55, 0))

    V1ProxyHandler.bucket = bucket
    V1ProxyHandler.stats = stats
    V1ProxyHandler.upstream = args.upstream.rstrip("/")
    V1ProxyHandler.recorder = recorder
    V1ProxyHandler.interceptor = interceptor

    logging.basicConfig(
        level=getattr(logging, args.log.upper()),
        format="%(asctime)s [proxy] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    # 把局部变量写入模块 globals，让 RPC executor 可以直接访问
    globals()['bucket'] = bucket
    globals()['stats'] = stats
    globals()['proxy'] = proxy
    globals()['recorder'] = recorder
    globals()['interceptor'] = interceptor

    server = ThreadedHTTPServer(('0.0.0.0', args.port), V1ProxyHandler)
    thread = threading.Thread(target=server.serve_forever, name='Proxy_Server', daemon=True)
    thread.start()

    print("=" * 70)
    print("本地聚合代理已启动")
    print(f"  监听:   http://127.0.0.1:{args.port}")
    print(f"  RPC:    /r=...  (Python 代码执行)")
    print(f"  代理:   /v1/*   → {args.upstream}/v1")
    print(f"  限速:   {args.rate} req/s，突发 {args.burst}")
    print(f"  重试:   最多 {MAX_RETRIES} 次，指数退避 {BASE_DELAY}s 起")
    print("=" * 70)
    print()
    print("测试:")
    print(f"  curl http://127.0.0.1:{args.port}/r=dir()")
    print(f"  curl http://127.0.0.1:{args.port}/r=proxy.get_status()")
    print(f"  curl http://127.0.0.1:{args.port}/r=proxy.set_rate(2.0)")
    print(f"  curl http://127.0.0.1:{args.port}/v1/models")
    print()
    print("按 Ctrl+C 退出")
    print("-" * 70)

    # 实时状态写入控制台窗口标题（不占用 stdout，避免与请求/回复行交错）
    def rate_display():
        while True:
            time.sleep(2)
            status = proxy.get_status()
            rate = status['instant_rate'] or status['rate_per_sec'].get('last_1s', 0)
            tokens = bucket._tokens

            last_usage = status.get('last_usage')
            if last_usage:
                usage_str = (f" 最近tok {last_usage.get('prompt_tokens')}+"
                             f"{last_usage.get('completion_tokens')}="
                             f"{last_usage.get('total_tokens')}")
            else:
                usage_str = ""
            total_tok = status.get('usage_total', {}).get('total_tokens', 0)

            title = (f"[{datetime.now().strftime('%H:%M:%S')}] "
                     f"速率 {rate:.2f}/s "
                     f"令牌 {tokens:.1f}/{args.burst} "
                     f"成功 {status['success_count']} 失败 {status['error_count']} "
                     f"429 {status['recent_429_count']}"
                     f"{usage_str} 累计tok {total_tok}"
                     f"{' 限流触发' if status['recent_429_count'] > 0 else ''}")
            set_console_title(title)

    rate_thread = threading.Thread(target=rate_display, name='RateDisplay', daemon=True)
    rate_thread.start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n代理已停止。")
        sys.exit(0)


if __name__ == "__main__":
    main()
