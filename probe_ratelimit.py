#!/usr/bin/env python3
"""Agnes API 真实限流参数压测脚本。

直接打上游（绕过 local_proxy 的 429 无限重试），记录原始状态码 /
限流响应头 / Retry-After，推断 burst、窗口、恢复速率。

用法:
    python probe_ratelimit.py burst [N]          # 瞬时并发 N 个（默认 15），测突发容量
    python probe_ratelimit.py recover           # 持续探测直到从 429 恢复，测恢复耗时
    python probe_ratelimit.py rate RPS DURATION # 以 RPS 固定速率压 DURATION 秒，测稳态
    python probe_ratelimit.py sweep             # 自动分档速率扫描
"""

import json
import sys
import time
import threading
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

URL = "https://api.agnes-ai.cn/v1/chat/completions"
KEY = "sk-gFk0KXBwghyZrgWshFbqvYVABFLtamRCZTvygRCtJjYEkOwR"
BODY = json.dumps({
    "model": "agnes-2.5-flash",
    "messages": [{"role": "user", "content": "hi"}],
    "max_tokens": 1,
    "stream": True,
}).encode()

LOG_LOCK = threading.Lock()
LOG_PATH = r"C:\Users\Administrator\AppData\Local\Temp\ratelimit_probe.jsonl"

# 关心的响应头关键字
def interesting_headers(headers):
    return {k: v for k, v in headers.items()
            if any(w in k.lower() for w in ("rate", "limit", "retry", "remaining", "reset", "quota"))}


def log_record(rec):
    with LOG_LOCK:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def one_request(i, t_start=None):
    """发一个最小流式请求，读到首个字节即关闭（最短占用）。"""
    req = urllib.request.Request(URL, data=BODY, headers={
        "Authorization": f"Bearer {KEY}",
        "Content-Type": "application/json",
    })
    t0 = time.monotonic()
    wall = time.time()
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            r.read(100)  # 流式读一点即断开，缩短请求占用
            status = r.status
            headers = interesting_headers(dict(r.headers))
        rec = {"i": i, "status": status, "dt": round(time.monotonic() - t0, 3),
               "wall": wall, "rel": round(time.monotonic() - t_start, 3) if t_start else 0,
               "headers": headers}
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        rec = {"i": i, "status": e.code, "dt": round(time.monotonic() - t0, 3),
               "wall": wall, "rel": round(time.monotonic() - t_start, 3) if t_start else 0,
               "headers": interesting_headers(dict(e.headers)), "body": body[:400]}
    except Exception as e:
        rec = {"i": i, "status": 0, "dt": round(time.monotonic() - t0, 3),
               "wall": wall, "rel": round(time.monotonic() - t_start, 3) if t_start else 0,
               "error": repr(e)[:200]}
    log_record(rec)
    return rec


def summarize(results, label):
    ok = [r for r in results if 200 <= r["status"] < 300]
    r429 = [r for r in results if r["status"] == 429]
    other = [r for r in results if r not in ok and r not in r429]
    print(f"\n=== {label} ===")
    print(f"  total={len(results)} ok={len(ok)} 429={len(r429)} other={len(other)}")
    if ok:
        dts = sorted(r["dt"] for r in ok)
        print(f"  ok latency min/median/max = {dts[0]:.2f}/{dts[len(dts)//2]:.2f}/{dts[-1]:.2f}s")
    if other:
        from collections import Counter
        print(f"  other status: {dict(Counter(r['status'] for r in other))}")
    return ok, r429, other


# ─── 模式 1：突发并发 ─────────────────────────────────────────────────────────

def mode_burst(n):
    print(f"[burst] 瞬时并发 {n} 个请求 ...")
    t_start = time.monotonic()
    with ThreadPoolExecutor(max_workers=n) as ex:
        results = list(ex.map(lambda i: one_request(i, t_start), range(n)))
    results.sort(key=lambda r: r["i"])
    ok, r429, other = summarize(results, f"BURST x{n}")
    # 打印每个请求的状态序列（按发起顺序近似）
    seq = "".join("O" if 200 <= r["status"] < 300 else ("X" if r["status"] == 429 else "?") for r in results)
    print(f"  sequence: {seq}")
    for r in results:
        tag = "OK " if 200 <= r["status"] < 300 else str(r["status"])
        print(f"    #{r['i']:>2} {tag} dt={r['dt']:.2f}s "
              f"h={r.get('headers', {})} "
              f"{('body=' + r['body'][:120]) if r['status']==429 else ''}")


# ─── 模式 2：恢复探测 ─────────────────────────────────────────────────────────

def mode_recover():
    print("[recover] 每 5s 探测一次，直到连续 2 次成功 ...")
    i = 0
    t_start = time.monotonic()
    consecutive_ok = 0
    while True:
        i += 1
        r = one_request(i, t_start)
        tag = "OK " if 200 <= r["status"] < 300 else str(r["status"])
        print(f"  [{r['rel']:>7.1f}s] probe #{i} {tag} dt={r['dt']:.2f}s "
              f"{r.get('headers', {})}")
        if 200 <= r["status"] < 300:
            consecutive_ok += 1
            if consecutive_ok >= 2:
                print(f"[recover] 已恢复，限流持续约 {r['rel']:.0f}s")
                break
        else:
            consecutive_ok = 0
        time.sleep(5)


# ─── 模式 3：固定速率压测 ─────────────────────────────────────────────────────

def mode_rate(rps, duration):
    interval = 1.0 / rps
    n = int(rps * duration)
    print(f"[rate] {rps} rps 压测 {duration}s（约 {n} 个请求，间隔 {interval:.3f}s）")
    t_start = time.monotonic()
    results = []
    threads = []

    def worker(idx):
        results.append(one_request(idx, t_start))

    for i in range(n):
        target = t_start + i * interval
        wait = target - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        th = threading.Thread(target=worker, args=(i,))
        th.start()
        threads.append(th)
        # 实时进度
        if i % max(1, int(rps * 5)) == 0:
            ok_now = len([r for r in results if 200 <= r["status"] < 300])
            x_now = len([r for r in results if r["status"] == 429])
            print(f"  [{time.monotonic()-t_start:>6.1f}s] sent={i} returned ok={ok_now} 429={x_now}")

    for th in threads:
        th.join(timeout=90)

    summarize(results, f"RATE {rps} rps x {duration}s")
    # 按 10s 分桶看何时开始 429
    buckets = {}
    for r in results:
        b = int(r["rel"] // 10) * 10
        buckets.setdefault(b, {"ok": 0, "429": 0, "other": 0})
        if 200 <= r["status"] < 300:
            buckets[b]["ok"] += 1
        elif r["status"] == 429:
            buckets[b]["429"] += 1
        else:
            buckets[b]["other"] += 1
    print("  10s 分桶:")
    for b in sorted(buckets):
        c = buckets[b]
        print(f"    {b:>4}-{b+10:>4}s: ok={c['ok']:>3} 429={c['429']:>3} other={c['other']}")


# ─── 模式 4：自动扫描（档间自动等恢复）──────────────────────────────────────

def wait_recovered(label=""):
    """轮询直到连续 2 次成功，再额外等 35s 让探测请求也滑出窗口（恢复满 burst）。"""
    print(f"[cooldown {label}] 等待限流恢复 ...")
    t_start = time.monotonic()
    consecutive = 0
    i = 0
    while True:
        i += 1
        r = one_request(9000 + i, t_start)
        ok = 200 <= r["status"] < 300
        print(f"  [{r['rel']:>6.1f}s] {'OK' if ok else r['status']}")
        if ok:
            consecutive += 1
            if consecutive >= 2:
                break
        else:
            consecutive = 0
        time.sleep(5)
    print(f"[cooldown {label}] 已恢复，再等 35s 清空探测请求 ...")
    time.sleep(35)


def mode_sweep():
    # 聚焦 0.25 rps 疑似临界区：0.2(应全成功) / 0.3(临界) / 0.5(半429) / 1.0(大量429)
    results_by_rate = {}
    for rps in [0.2, 0.3, 0.5, 1.0]:
        wait_recovered(f"before {rps}rps")
        mode_rate(rps, 100)
    print("\n[SWEEP DONE]")


if __name__ == "__main__":
    # 清空/新建日志标记
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps({"event": "run_start", "argv": sys.argv,
                            "wall": time.time(),
                            "time": datetime.now().isoformat()}, ensure_ascii=False) + "\n")

    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    mode = sys.argv[1]
    if mode == "burst":
        mode_burst(int(sys.argv[2]) if len(sys.argv) > 2 else 15)
    elif mode == "recover":
        mode_recover()
    elif mode == "rate":
        mode_rate(float(sys.argv[2]), float(sys.argv[3]))
    elif mode == "sweep":
        mode_sweep()
    else:
        print(__doc__)
