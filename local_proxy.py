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
import urllib.parse
import os
import statistics
import socket
import ssl
from collections import deque
from datetime import datetime
from http.client import HTTPConnection, HTTPSConnection, HTTPException
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
    """把实时状态写入控制台窗口标题，不占用 stdout，避免与请求/回复行交错。
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


# ─── 文件同步日志 ──────────────────────────────────────────────────────────────

LIVE_LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "proxy.live.log")


class _Tee:
    """一次写多个流（控制台 + 文件），线程安全；任何一个流挂掉不影响其他。"""

    def __init__(self, *streams):
        self._streams = streams
        self._lock = threading.Lock()

    def write(self, data):
        with self._lock:
            for st in self._streams:
                try:
                    st.write(data)
                    st.flush()
                except Exception:
                    pass

    def flush(self):
        for st in self._streams:
            try:
                st.flush()
            except Exception:
                pass

    def reconfigure(self, **kwargs):
        for st in self._streams:
            try:
                st.reconfigure(**kwargs)
            except Exception:
                pass


def install_file_logging():
    """stdout/stderr 与 logging 全量同步到 proxy.live.log（追加模式）。

    无论是否重定向启动、有没有控制台窗口，现场日志都在文件里。
    """
    logf = open(LIVE_LOG_PATH, "a", encoding="utf-8", buffering=1)
    sys.stdout = _Tee(sys.stdout, logf)
    sys.stderr = _Tee(sys.stderr, logf)
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    print(f"\n================ 代理启动 {datetime.now():%Y-%m-%d %H:%M:%S} pid={os.getpid()} ================")
    return logf


# ─── 详细日志 ────────────────────────────────────────────────────────────────

class RecentRing:
    """内存中保留最近 N 个请求/上游回复的完整快照，供 RPC 排查。

    RPC 用法：
      recent.list()                  摘要（id/时刻/状态/大小/耗时/重试）
      recent.get(-1)                 最新一条完整内容（headers+body）
      recent.get(37)                 按请求 ID（日志 >>> #N 的 N）查询
      recent.get(-1)["resp_body"]    只看上游响应原文
    """

    def __init__(self, cap=3):
        self._items = deque(maxlen=cap)
        self._lock = threading.Lock()

    @staticmethod
    def _text(body):
        if body is None:
            return None
        if isinstance(body, str):
            return body
        try:
            return body.decode('utf-8', errors='replace')
        except Exception:
            return f"[{len(body)}B binary]"

    @staticmethod
    def _mask_headers(headers):
        if not headers:
            return {}
        items = headers.items() if isinstance(headers, dict) else headers
        out = {}
        for k, v in items:
            out[k] = "***" if k.lower() == "authorization" else v
        return out

    def add(self, item):
        with self._lock:
            self._items.append(item)
            return item

    def list(self):
        with self._lock:
            return [{
                'id': x.get('id'), 'arrival': x.get('arrival'),
                'method': x.get('method'), 'path': x.get('path'),
                'status': x.get('status'),
                'req_bytes': len(x.get('req_body') or b'') if x.get('req_body') else 0,
                'resp_bytes': len(x.get('resp_body') or b'') if x.get('resp_body') else 0,
                'wait_s': round(x.get('wait', 0.0), 2),
                'up_ms': round(x.get('up_ms', 0.0)) if x.get('up_ms') else None,
                'ttfb_ms': round(x.get('ttfb_ms')) if x.get('ttfb_ms') else None,
                'retries': len(x.get('retry_events', [])),
            } for x in self._items]

    def _resolve(self, key):
        """把 key 解析成环形缓冲中的一条记录。

        非负整数优先按请求 ID（日志 >>> #N）匹配；匹配不到再退回
        deque 下标。负数始终按下标（-1 最新）。
        """
        if isinstance(key, int) and key >= 0:
            for x in self._items:
                if x.get('id') == key:
                    return x
        try:
            return self._items[key]
        except IndexError:
            ids = [x.get('id') for x in self._items]
            if isinstance(key, int) and key >= 0:
                raise KeyError(
                    f"找不到请求 ID={key} 的快照（仅保留最近 {len(ids)} 条，"
                    f"可用 ID: {ids}）")
            raise

    def get(self, key=-1):
        """返回一条完整快照（文本化的 headers/body），默认最新一条。

        key 可以是请求 ID（如 recent.get(37)），也可以是下标
        （-1 最新、0 最旧）；非负整数优先按请求 ID 匹配。
        """
        with self._lock:
            x = self._resolve(key)
            return {
                'id': x.get('id'), 'arrival': x.get('arrival'),
                'method': x.get('method'), 'path': x.get('path'),
                'status': x.get('status'),
                'wait_s': x.get('wait', 0.0), 'up_ms': x.get('up_ms'),
                'ttfb_ms': x.get('ttfb_ms'),
                'req_headers': self._mask_headers(x.get('req_headers')),
                'req_body': self._text(x.get('req_body')),
                'resp_headers': x.get('resp_headers'),
                'resp_body': self._text(x.get('resp_body')),
                'retry_events': x.get('retry_events', []),
            }


class DetailLogger:
    """请求/响应日志。默认紧凑单行：请求一行、回复一行。
    RPC 可设置 _detail_logger.verbose = True 强制展开全部 header 与 body。
    最近 3 个请求/回复完整保存在 recent（RecentRing），RPC 可随时查看。
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._active = {}  # request_id -> 日志/快照条目
        self.verbose = False
        self.recent = RecentRing(cap=20)

    def log_request(self, req_id, method, url, headers, body=None, client=None):
        with self._lock:
            now = time.monotonic()
            item = {
                'id': req_id, 'arrival': _ts(), 'method': method,
                'path': url, 'req_headers': headers, 'req_body': body,
                'client': client,
                'start': now, 'wait': 0.0, 'retry_events': [],
            }
            self._active[req_id] = item
            self.recent.add(item)
            # 请求一到达（读完 body）立即留痕：即使后续在令牌桶排队，
            # 日志里也能看到未配对的 >>>，不会出现“调用方超时但日志空白”
            n = len(body) if body else 0
            who = f"{client[0]}:{client[1]} " if client else ""
            print(f"[{_ts()}] >>> #{req_id} {who}{method} {url} body={n}B")
            if self.verbose:
                for k, v in headers.items():
                    print(f"    {k}: {v}")
                if body:
                    self._print_body(body)

    def mark_sent(self, req_id, wait):
        """令牌等待结束、实际发送上游时调用：记录等待时长并重置计时起点。"""
        with self._lock:
            info = self._active.get(req_id)
            if info is not None:
                if info['wait'] == 0.0 and wait:
                    info['wait'] = wait          # 只记首次发送前的等待
                info['start'] = time.monotonic()  # 之后只统计纯上游耗时

    def log_response(self, req_id, status, headers, body, ttfb_ms=None):
        with self._lock:
            info = self._active.pop(req_id, None)
            if not info:
                return
            up_ms = (time.monotonic() - info['start']) * 1000
            info.update({'status': status, 'up_ms': up_ms,
                         'ttfb_ms': ttfb_ms,
                         'resp_headers': headers, 'resp_body': body})
            usage = extract_usage(body)
            wait_s = f" wait={info['wait']:.1f}s" if info['wait'] > 0.05 else ""
            ttfb_s = f" ttfb={ttfb_ms:.0f}ms" if ttfb_ms is not None else ""
            n_retry = len(info['retry_events'])
            retry_s = f" retry={n_retry}" if n_retry else ""
            tok = ""
            if usage:
                tok = (f" sent={usage.get('prompt_tokens')} "
                       f"recv={usage.get('completion_tokens')}")
            print(f"[{info['arrival']}] <<< #{req_id} {status} {self._status_text(status)} "
                  f"up={up_ms:.0f}ms{ttfb_s}{wait_s}{retry_s}{tok}")
            if self.verbose:
                for k, v in headers:
                    if k.lower() not in ('transfer-encoding', 'connection'):
                        print(f"    {k}: {v}")
                if body:
                    self._print_body(body)

    def log_error(self, req_id, error, elapsed):
        with self._lock:
            # 不 pop：429/网络重试后还要记录最终响应，pop 会导致成功响应日志丢失
            info = self._active.get(req_id)
            if info is not None:
                info['retry_events'].append(
                    {'ts': _ts(), 'attempt_ms': round(elapsed * 1000), 'error': str(error)})
                arrival = info['arrival']
            else:
                arrival = '?'
            # 时间戳必须用“此刻”而非请求到达时刻，否则长超时后看起来像错误发生在过去
            print(f"[{_ts()}] !!! #{req_id} (到达 {arrival}, 本次尝试 {elapsed*1000:.0f}ms) {error}")

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
        # 0 表示关闭；可由 RPC 调整，如 bucket.soft_delay_max = 15
        # 默认 8s：叠加令牌硬等最坏约 6.7s，单请求最坏等待约 15s，
        # 尽量控制在调用方（IDE）客户端超时之内
        self.soft_delay_max = 8.0

    def acquire(self):
        """取一个令牌，返回实际等待秒数（令牌硬等 + 软延迟之和）。"""
        delayed = 0.0
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
                return wait
            self._tokens -= 1.0
            # 取到令牌后检查余量：低于一半则按缺口比例额外 sleep（余量越少延时越长）
            half = self.burst / 2.0
            if self.soft_delay_max > 0 and self._tokens < half:
                deficit = 1.0 - (self._tokens / half)  # 0（刚好一半）→ 1（余量为 0）
                extra = deficit * self.soft_delay_max
                if extra > 0:
                    time.sleep(extra)
                    now = time.monotonic()
                    self._tokens = min(self.burst, self._tokens + (now - self._last) * self.rate)
                    self._last = now
                    delayed = extra
        return delayed


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


def forward_request(bucket, upstream_url, method, headers, body, path, stats, client=None):
    """带限速 + 429 自动重试的转发，记录详细日志。

    - 429：挂起调用者指数退避重试；累计等待超过 RATE_LIMIT_TOTAL_TIMEOUT 则透传 429 让调用方快速失败。
    - 5xx / 网络错误：最多重试 MAX_RETRIES 次。
    """
    req_id = _next_req_id()
    attempt = 0              # 5xx / 网络错误重试计数（有上限 MAX_RETRIES）
    rate_limit_attempt = 0   # 429 重试计数
    rate_limit_start = None  # 首次收到 429 的时间点，用于累计总时限

    # 请求到达立即留痕（排队等待期间也能在日志看到这条 >>>）
    _detail_logger.log_request(req_id, method, upstream_url + path, headers, body, client)

    full_url = upstream_url.rstrip("/") + path
    parsed = urllib.parse.urlparse(full_url)
    # http.client 的 request target 只给 path 会丢 query，必须显式拼回
    request_target = parsed.path + (("?" + parsed.query) if parsed.query else "")

    while True:
        wait = bucket.acquire()
        # 实际发送上游：记录代理内部等待时长，计时起点切到发送时刻
        _detail_logger.mark_sent(req_id, wait)
        t0 = time.monotonic()

        conn = None
        try:
            # 使用 http.client 以便详细记录（而非 urllib）
            if parsed.scheme == "https":
                conn = HTTPSConnection(parsed.hostname, parsed.port or 443, timeout=120)
            else:
                conn = HTTPConnection(parsed.hostname, parsed.port or 80, timeout=120)

            conn.request(method, request_target, body=body, headers=headers)
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

        except (OSError, HTTPException) as e:
            # http.client 不抛 HTTPError/URLError（那是 urllib.request 层的异常）：
            # 任何 HTTP 状态码都由 getresponse() 返回。这里只捕获连接层错误：
            # socket 超时/重置等 OSError，以及 BadStatusLine 等 HTTPException。
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


# ─── 流式透传 + SSE 心跳保活 ──────────────────────────────────────────────────

# 流式请求等待上游期间，按此间隔向客户端发心跳防断；RPC 可直接改。
# 实测 IDE 首事件窗口约 5s 且有调度抖动：5s 间隔曾在 5.3s 断连（帧差 0.3s
# 没赶上），2s 间隔保证窗口内至少到达 2 个有效事件。
HEARTBEAT_INTERVAL = 2.0
# 上游首字节（含响应头与第一个 SSE chunk）等待上限。
# 实测会遇到“TCP/TLS 连上但上游长时间零字节”的坏连接：120s 才超时重试时，
# 调用方（IDE 约 30s 首事件窗口）早已断开。15s 快速放弃坏连接并重试，
# 绝大多数重试在数秒内成功；模块全局，RPC 可实时调整。
FIRST_BYTE_TIMEOUT = 30
# 首个 chunk 透传之后，chunk 间间隔的等待上限（长回答允许较慢）
MID_STREAM_TIMEOUT = 120.0


def _is_stream_body(body):
    """hook 处理后的 body 是否为流式（stream=true）请求。"""
    if not body:
        return False
    try:
        return json.loads(body.decode("utf-8", errors="replace")).get("stream") is True
    except (ValueError, UnicodeDecodeError):
        return False


def _sse_error_frame(payload):
    """把上游错误体包装成单行 JSON 的 SSE data 帧。"""
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8", errors="replace")
    try:
        obj = json.loads(payload)
        data = json.dumps(obj, ensure_ascii=False)
    except (ValueError, TypeError):
        data = json.dumps({"error": {"message": payload[:2000]}}, ensure_ascii=False)
    return ("data: " + data + "\n\n").encode("utf-8")


# 主连接在该秒数内没返回响应头，就开第二条连接竞速（坏连接高发时避免白等 15s）
HEDGE_DELAY = 12


def _stream_race_connection(parsed, method, headers, body, bucket, hedge_enabled=True):
    """并行竞连接上游，返回首个可用响应 (conn, resp, wait_total, hedged)。

    实测上游坏连接率约 60%（TCP/TLS 连上但零字节），串行等 15s 超时会把请求
    总时长拖过调用方约 30s 的总 deadline。竞速：主连接 HEDGE_DELAY 秒内没出
    响应头就再取一个令牌开备用连接，谁先返回 200 用谁，另一条立即关闭。
    所有连接都失败时抛最后一个异常；返回非 200 响应（429/5xx）交给调用方。
    """
    import queue as _queue
    result_q = _queue.Queue()
    conns = {}
    conn_lock = threading.Lock()
    # http.client 的 request target 只给 path 会丢 query，必须显式拼回
    request_target = parsed.path + (("?" + parsed.query) if parsed.query else "")

    def worker(tag):
        try:
            if parsed.scheme == "https":
                c = HTTPSConnection(parsed.hostname, parsed.port or 443,
                                    timeout=FIRST_BYTE_TIMEOUT)
            else:
                c = HTTPConnection(parsed.hostname, parsed.port or 80,
                                   timeout=FIRST_BYTE_TIMEOUT)
            with conn_lock:
                conns[tag] = c
            c.request(method, request_target, body=body, headers=headers)
            r = c.getresponse()
            result_q.put((tag, True, c, r))
        except Exception as e:
            result_q.put((tag, False, None, e))

    wait_total = bucket.acquire()
    threads = {}
    th = threading.Thread(target=worker, args=("A",), name="upstream-A", daemon=True)
    th.start(); threads["A"] = th
    alive = {"A"}
    hedged = False
    bad = None       # 首个非 200 响应 (conn, resp)，另一条还在跑时留待兜底
    last_err = None

    def cleanup_losers():
        """winner 已定：关闭仍在阻塞的连接，排空并关闭已完成的备用连接。"""
        wasted = 0
        with conn_lock:
            for other in list(alive):
                oc = conns.get(other)
                if oc is not None:
                    try:
                        oc.close()
                    except Exception:
                        pass
        while True:
            try:
                _, ok2, c2, _r2 = result_q.get_nowait()
            except _queue.Empty:
                break
            if ok2 and c2 is not None:
                wasted += 1
                try:
                    c2.close()
                except Exception:
                    pass
        if bad is not None:
            try:
                bad[0].close()
            except Exception:
                pass
        for t in threads.values():
            t.join(timeout=1.0)
        return wasted

    while True:
        try:
            tag, ok, c, r = result_q.get(
                timeout=(None if (hedged or not hedge_enabled) else HEDGE_DELAY))
        except _queue.Empty:
            # 主连接迟迟不出响应头：取第二个令牌开备用连接竞速
            wait_total += bucket.acquire()
            th = threading.Thread(target=worker, args=("B",), name="upstream-B", daemon=True)
            th.start(); threads["B"] = th
            alive.add("B"); hedged = True
            logging.info("[%s] [hedge] 主连接 %.0fs 无响应头，启动备用连接竞速",
                         _ts(), HEDGE_DELAY)
            continue

        alive.discard(tag)
        if ok and r.status == 200:
            wasted = cleanup_losers()
            if hedged:
                logging.info("[%s] [hedge] 连接 %s 胜出%s",
                             _ts(), tag,
                             "，备用连接也已成功，多耗 1 次上游处理" if wasted else "")
            return c, r, wait_total, hedged
        if ok:
            # 429/5xx：另一条还在跑就给它机会，否则返回此响应由调用方处理
            if alive:
                if bad is None:
                    bad = (c, r)
                else:
                    try:
                        c.close()
                    except Exception:
                        pass
                continue
            return c, r, wait_total, hedged
        # 连接层异常
        last_err = r
        if alive:
            continue
        if bad is not None:
            return bad[0], bad[1], wait_total, hedged
        raise last_err


def forward_stream(handler, bucket, upstream_url, method, headers, body, path, stats,
                   client=None, stream_filter=None, hedge_enabled=True,
                   heartbeat_enabled=True):
    """流式透传：立即回 SSE 响应头，等待上游期间用心跳帧给客户端保活。

    令牌桶排队、429 退避、上游首字节前的空等，都按 HEARTBEAT_INTERVAL
    发送 SSE 注释帧（``: heartbeat``，SSE 客户端必须忽略），调用方读
    连接持续有字节，不会触发读空闲超时。上游 200 响应逐块透传；429
    仍在内部退避重试（不写客户端）。

    代价：响应头已提前按 200 发出，上游最终非 200（非 429）时错误只能
    以 SSE error data 帧传达。返回 (status, headers, body, already_sent)。
    """
    req_id = _next_req_id()
    attempt = 0
    rate_limit_attempt = 0
    rate_limit_start = None

    _detail_logger.log_request(req_id, method, upstream_url + path, headers, body, client)

    full_url = upstream_url.rstrip("/") + path
    parsed = urllib.parse.urlparse(full_url)

    write_lock = threading.Lock()

    def cwrite(data):
        """按 HTTP/1.1 chunked 编码写一个分块并立即 flush。"""
        if not data:
            return
        with write_lock:
            handler.wfile.write(f"{len(data):X}\r\n".encode("ascii"))
            handler.wfile.write(data)
            handler.wfile.write(b"\r\n")
            handler.wfile.flush()

    def cend():
        """chunked 结束块。"""
        with write_lock:
            handler.wfile.write(b"0\r\n\r\n")
            handler.wfile.flush()

    # 排队/等待前先发出 SSE 响应头（hook 已在调用处处理完 block）。
    # 必须用 HTTP/1.1 + chunked：HTTP/1.0+close 定界会被很多客户端整体
    # 缓冲到连接结束，心跳帧无法被 SSE 解析器增量消费。
    try:
        handler.protocol_version = "HTTP/1.1"
        handler.send_response(200)
        handler.send_header("Content-Type", "text/event-stream; charset=utf-8")
        handler.send_header("Cache-Control", "no-cache")
        handler.send_header("X-Accel-Buffering", "no")
        handler.send_header("Transfer-Encoding", "chunked")
        handler.send_header("Connection", "close")
        handler.end_headers()
        handler.wfile.flush()
        handler.close_connection = True
    except OSError as e:
        logging.warning("[%s] [client] 流式响应头未发出客户端已断开：%s", _ts(), e)
        return 499, [], b"", True

    stop_beat = threading.Event()
    # 首个真实上游 chunk 是否已透传：此前发“零内容 OpenAI chunk”充当有效
    # SSE 事件（实测 Trae 有首事件超时，注释帧不算有效事件），此后发注释帧
    stream_state = {"first_event": False}
    try:
        req_model = json.loads(body.decode("utf-8", errors="replace")).get("model") or ""
    except (ValueError, UnicodeDecodeError, AttributeError):
        req_model = ""

    def empty_event_frame():
        payload = json.dumps({
            "id": "chatcmpl-proxy-heartbeat",
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": req_model,
            "choices": [{"index": 0,
                         "delta": {"role": "assistant", "content": ""},
                         "finish_reason": None}],
        }, ensure_ascii=False)
        return ("data: " + payload + "\n\n").encode("utf-8")

    def heartbeat():
        while not stop_beat.wait(HEARTBEAT_INTERVAL):
            if not heartbeat_enabled:
                continue  # hook 按请求关闭：纯透传，不注入任何帧
            frame = b": heartbeat\n\n" if stream_state["first_event"] else empty_event_frame()
            try:
                cwrite(frame)
            except OSError:
                return  # 客户端断开，透传循环写时会感知并收尾

    beat_thread = threading.Thread(target=heartbeat, name="sse-heartbeat", daemon=True)
    beat_thread.start()

    final_status = 0
    final_headers = []
    final_ttfb = None
    resp_buf = bytearray()
    client_gone = False

    try:
        while True:
            t0 = time.monotonic()
            conn = None
            try:
                # 并行竞速建连（内部取令牌：主连接 1 个，触发 hedge 时再取 1 个）
                conn, resp, wait_total, hedged = _stream_race_connection(
                    parsed, method, headers, body, bucket, hedge_enabled=hedge_enabled)
                _detail_logger.mark_sent(req_id, wait_total)

                # 429：心跳继续，内部退避重试（受总时限约束）
                if resp.status == 429:
                    rb = resp.read()
                    duration = time.monotonic() - t0
                    stats.record(method, path, 429, duration)
                    stats.record_429(path, resp.getheaders())
                    if rate_limit_start is None:
                        rate_limit_start = time.monotonic()
                    delay = min(
                        BASE_DELAY * (2 ** min(rate_limit_attempt, RATE_LIMIT_BACKOFF_CAP)),
                        RATE_LIMIT_MAX_DELAY)
                    ra = resp.headers.get("Retry-After")
                    if ra:
                        try:
                            delay = max(delay, float(ra))
                        except ValueError:
                            pass
                    waited = time.monotonic() - rate_limit_start
                    if waited + delay > RATE_LIMIT_TOTAL_TIMEOUT:
                        logging.warning(
                            "[%s] [429] stream give-up waited=%.1fs，以 SSE 错误帧结束",
                            _ts(), waited)
                        try:
                            cwrite(_sse_error_frame(
                                '{"error":{"message":"upstream rate limited too long","code":429}}'))
                        except OSError:
                            client_gone = True
                        final_status = 429
                        resp_buf.extend(rb)
                        break
                    _detail_logger.log_error(
                        req_id, f"429 Too Many Requests (attempt {rate_limit_attempt})", duration)
                    rate_limit_attempt += 1
                    time.sleep(delay)
                    continue

                # 5xx：尚未透传任何上游字节，可安全重试
                if resp.status >= 500 and attempt < MAX_RETRIES:
                    resp.read()
                    duration = time.monotonic() - t0
                    stats.record(method, path, resp.status, duration)
                    _detail_logger.log_error(req_id, f"{resp.status} upstream error", duration)
                    attempt += 1
                    time.sleep(BASE_DELAY * (2 ** (attempt - 1)))
                    continue

                final_headers = resp.getheaders()

                # 非 200 终结响应（400/401/403/放弃重试的 5xx）：错误帧传达
                if resp.status != 200:
                    err = resp.read()
                    duration = time.monotonic() - t0
                    stats.record(method, path, resp.status, duration)
                    resp_buf.extend(err)
                    try:
                        cwrite(_sse_error_frame(err))
                    except OSError:
                        client_gone = True
                    final_status = resp.status
                    break

                # 200：逐块透传；心跳线程继续负责 chunk 间空闲保活
                # stream_filter（由 RPC hook 经 action 注入，代理核心不含策略）：
                # feed() 可重写/注入 SSE 事件；任何异常立即摘除过滤器退回原始
                # 透传（fail-open），绝不让修复逻辑本身拖垮正常响应。
                delivered = False
                ttfb_ms = None
                filter_dead = False

                def apply_filter(raw):
                    nonlocal filter_dead
                    if stream_filter is None or filter_dead:
                        return raw
                    try:
                        return stream_filter.feed(raw)
                    except Exception:
                        filter_dead = True
                        logging.exception("[%s] [stream] stream_filter 异常，摘除并回退原始透传",
                                          _ts())
                        return raw

                while True:
                    try:
                        chunk = resp.read(4096)
                    except (socket.timeout, TimeoutError):
                        if not delivered:
                            # 一个上游字节都没转发：抛出走外层连接重试
                            raise
                        # 已部分透传，不能重发：错误帧收尾
                        try:
                            cwrite(_sse_error_frame(
                                '{"error":{"message":"upstream read timeout mid-stream"}}'))
                        except OSError:
                            client_gone = True
                        logging.warning("[%s] [stream] 透传中途上游读取超时，已发错误帧", _ts())
                        break
                    if not chunk:
                        break
                    out = apply_filter(chunk)
                    if out:
                        resp_buf.extend(out)
                        try:
                            cwrite(out)
                            if not delivered:
                                delivered = True
                                ttfb_ms = (time.monotonic() - t0) * 1000
                                stream_state["first_event"] = True
                                # 已开始出字：放宽 chunk 间等待，允许长回答慢慢吐
                                try:
                                    conn.sock.settimeout(MID_STREAM_TIMEOUT)
                                except Exception:
                                    pass
                        except OSError:
                            client_gone = True
                            logging.warning(
                                "[%s] [client] 流式透传中调用方断开，已丢弃后续上游数据", _ts())
                            break
                # 上游正常结束：给过滤器补齐/收尾注入的机会（如补闭合标签）
                if delivered and stream_filter is not None and not filter_dead:
                    try:
                        tail = stream_filter.flush()
                    except Exception:
                        filter_dead = True
                        logging.exception("[%s] [stream] stream_filter flush 异常，摘除", _ts())
                        tail = None
                    if tail:
                        resp_buf.extend(tail)
                        try:
                            cwrite(tail)
                        except OSError:
                            client_gone = True
                duration = time.monotonic() - t0
                stats.record(method, path, 200, duration, extract_usage(bytes(resp_buf)))
                final_status = 200
                final_ttfb = ttfb_ms
                break

            except (OSError, HTTPException) as e:
                duration = time.monotonic() - t0
                if attempt < MAX_RETRIES:
                    stats.record(method, path, 0, duration)
                    logging.warning("[%s] [conn] stream attempt=%d wait: %s", _ts(), attempt, e)
                    _detail_logger.log_error(req_id, str(e), duration)
                    attempt += 1
                    time.sleep(BASE_DELAY * (2 ** (attempt - 1)))
                    continue
                stats.record(method, path, 502, duration)
                payload = json.dumps(
                    {"error": {"message": f"upstream connection failed: {e}"}},
                    ensure_ascii=False)
                try:
                    cwrite(_sse_error_frame(payload))
                except OSError:
                    client_gone = True
                final_status = 502
                resp_buf.extend(payload.encode("utf-8"))
                break
            finally:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
    except Exception as e:
        # 兜底：BadStatusLine 等非 OSError 异常绝不能穿透——否则调用方收到
        # 裸 502，recent 里状态永远停在 None，异常堆栈只出现在 stderr 容易丢
        import traceback
        logging.error("[%s] [stream] 未预期异常: %s\n%s", _ts(), e, traceback.format_exc())
        _detail_logger.log_error(req_id, f"unexpected: {e!r}", 0.0)
        stats.record(method, path, 502, 0.0)
        try:
            cwrite(_sse_error_frame(json.dumps(
                {"error": {"message": f"proxy stream error: {e}"}}, ensure_ascii=False)))
        except OSError:
            client_gone = True
        final_status = 502
    finally:
        stop_beat.set()
        if not client_gone:
            try:
                cend()
            except OSError:
                client_gone = True

    _detail_logger.log_response(req_id, final_status, final_headers,
                                bytes(resp_buf), ttfb_ms=final_ttfb)
    if client_gone:
        logging.warning(
            "[%s] [client] 调用方流式接收中断，上游实际返回 %d，%dB 未送达",
            _ts(), final_status, len(resp_buf))
    return final_status, final_headers, bytes(resp_buf), True


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
        # 加载说明从函数自身动态提取（docstring 首行），不由调用方硬编码
        doc_lines = (getattr(fn, "__doc__", "") or "").strip().splitlines()
        desc = doc_lines[0].strip() if doc_lines else ""
        print(f"  内置hook: {name} 已开启{('（' + desc + '）') if desc else ''}")
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
        """依次执行 hook。

        - block/respond/allow：终结 hook 链立即返回（allow 会带上前序 modify
          已累积的 body/过滤器）；
        - modify：链式叠加——改写结果写回 req 供后续 hook 继续判定，body 以
          最后一个 modify 为准，headers 合并，stream_filter 透传保留。
        异常的 hook 跳过不阻断请求。
        """
        merged = None
        for name, fn in list(self._hooks):
            try:
                action = fn(req)
            except Exception:
                logging.exception("[interceptor] hook %s 异常，跳过", name)
                continue
            if not action:
                continue
            action.setdefault("hook", name)
            kind = action.get("action", "allow")
            if kind != "modify":
                if kind == "allow" and merged is not None:
                    return merged     # 前序 modify 已累积，放行其改写结果
                return action
            # 改写结果回写 req_ctx，让后续 hook 基于最新 body 判定
            if "body" in action:
                nb = action["body"]
                body = nb.encode("utf-8") if isinstance(nb, str) else nb
                req["body"] = body
                req["body_text"] = body.decode("utf-8", errors="replace")
                try:
                    req["json"] = json.loads(req["body_text"])
                except (ValueError, TypeError):
                    req["json"] = None
            if action.get("headers"):
                req.setdefault("headers", {}).update(action["headers"])
            if merged is None:
                merged = dict(action)
            else:
                if action.get("headers"):
                    merged.setdefault("headers", {}).update(action["headers"])
                if "body" in action:
                    merged["body"] = action["body"]
                if action.get("stream_filter"):
                    merged["stream_filter"] = action["stream_filter"]
                # hedge/heartbeat 关闭标志粘性：任一 hook 要求关即保持关
                if action.get("hedge") is False:
                    merged["hedge"] = False
                if action.get("heartbeat") is False:
                    merged["heartbeat"] = False
        return merged

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
        sh = headers or {}
        sh_lower = {k.lower() for k in sh}
        # 逐跳/定界头不能由调用方决定：CL 按本地 body 重算，TE 不支持；
        # server/date 由 send_response() 自动生成
        hop = {"content-length", "transfer-encoding", "connection",
               "keep-alive", "server", "date"}
        for k, v in sh.items():
            if k.lower() not in hop:
                self.send_header(k, v)
        if "content-type" not in sh_lower:
            self.send_header("Content-Type", "application/json")
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
        stream_filter = None      # hook 可经 modify action 注入流式响应过滤器
        hedge_enabled = True      # hook 可经 modify action 的 hedge=False 关闭并发竞速
        heartbeat_enabled = True  # hook 可经 modify action 的 heartbeat=False 纯透传
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
            flt = action.get("stream_filter")
            if flt is not None and hasattr(flt, "feed") and hasattr(flt, "flush"):
                stream_filter = flt
            if action.get("hedge") is False:
                hedge_enabled = False
            if action.get("heartbeat") is False:
                heartbeat_enabled = False
            # allow → 继续转发

        # 3) 转发到上游（流式请求走透传+心跳，其余走缓冲转发）
        already_sent = False
        try:
            if _is_stream_body(body):
                status, resp_headers, resp_body, already_sent = forward_stream(
                    self, self.bucket, self.upstream, method, headers, body,
                    self.path, self.stats, client=self.client_address,
                    stream_filter=stream_filter, hedge_enabled=hedge_enabled,
                    heartbeat_enabled=heartbeat_enabled)
            else:
                status, resp_headers, resp_body = forward_request(
                    self.bucket, self.upstream, method, headers, body, self.path,
                    self.stats, client=self.client_address)
        except Exception as e:
            logging.error("upstream error: %s", e)
            self.recorder.capture_response(seq, 502, {}, str(e), note="upstream error")
            if already_sent:
                # 流式路径响应头已发，只能补一个 chunked SSE 错误帧 + 结束块
                try:
                    frame = _sse_error_frame(
                        json.dumps({"error": {"message": f"proxy error: {e}"}},
                                   ensure_ascii=False))
                    self.wfile.write(f"{len(frame):X}\r\n".encode("ascii"))
                    self.wfile.write(frame + b"\r\n0\r\n\r\n")
                    self.wfile.flush()
                except OSError:
                    pass
                return
            self.send_response(502)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(f"502 Bad Gateway: {e}".encode())
            return

        # 4) 录制完整响应（不截断）
        self.recorder.capture_response(seq, status, dict(resp_headers), resp_body)

        # 5) 回写客户端（流式路径已在 forward_stream 中透传完毕，跳过）
        if already_sent:
            return
        try:
            self.send_response(status)
            # content-length：上游 chunked 响应经 resp.read() 解码后无 CL，
            # 必须按本地 body 重算，否则客户端拿不到 body 边界；
            # content-type：循环只跳过不重发，统一在下面发一次，避免重复头；
            # server/date：send_response() 已自动生成，不透传上游的值
            skip_resp = {"transfer-encoding", "connection", "keep-alive",
                         "trailer", "content-length", "content-type",
                         "server", "date"}
            for k, v in resp_headers:
                if k.lower() in skip_resp:
                    continue
                self.send_header(k, v)
            self.send_header(
                "Content-Type", dict(resp_headers).get("Content-Type", "application/json"))
            if method != "HEAD":
                self.send_header("Content-Length", str(len(resp_body or b"")))
            self.end_headers()
            if method != "HEAD" and resp_body:
                self.wfile.write(resp_body)
        except (BrokenPipeError, ConnectionResetError, OSError) as e:
            # 上游已正常返回，但调用方（如 IDE）等不及提前断开——
            # 典型表现：客户端报 Connection timeout，代理侧却是 200
            logging.warning(
                "[%s] [client] 调用方在收到响应前断开（多为客户端超时），上游实际返回 %d，%dB 已丢弃：%s",
                _ts(), status, len(resp_body or b""), e)

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
    # 所有 print 同步落盘 proxy.live.log（控制台窗口丢失也能排查）
    install_file_logging()

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
    # basicConfig 的 StreamHandler 绑定的是已被 Tee 替换的 stderr，logging 输出
    # 自然同步进 proxy.live.log，无需再加文件 handler（否则每条记录落盘两遍）

    # 把局部变量写入模块 globals，让 RPC executor 可以直接访问
    globals()['bucket'] = bucket
    globals()['stats'] = stats
    globals()['proxy'] = proxy
    globals()['recorder'] = recorder
    globals()['interceptor'] = interceptor
    globals()['recent'] = _detail_logger.recent  # 最近 3 个请求/回复内存快照，RPC 排查用

    
    print(f"  监听:   http://127.0.0.1:{args.port}")
    print(f"  RPC:    /r=...  (Python 代码执行)")
    print(f"  代理:   /v1/*   → {args.upstream}/v1")
    print(f"  限速:   {args.rate} req/s，突发 {args.burst}")
    print(f"  重试:   最多 {MAX_RETRIES} 次，指数退避 {BASE_DELAY}s 起")
    import builtin_hooks; builtin_hooks.register_defaults(interceptor)
    print("=" * 70)
    print()
    print("测试:")
    print(f" !curl 127.0.0.1:{args.port}/r=recorder.start(300)")
    print(f"  curl http://127.0.0.1:{args.port}/r=proxy.get_status()")
    print(f"  curl http://127.0.0.1:{args.port}/r=proxy.set_rate(2.0)")
    print(f"  curl http://127.0.0.1:{args.port}/v1/models")
    print()
    print("按 Ctrl+C 退出")
    print("-" * 70)

    # hook 已注册完毕，此时才绑定端口开始服务，杜绝启动瞬间请求漏过 hook
    server = ThreadedHTTPServer(('0.0.0.0', args.port), V1ProxyHandler)

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
