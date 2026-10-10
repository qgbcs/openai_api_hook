# -*- coding: utf-8 -*-
"""验证：1) 软限速渐进延时 2) 429 累计超总时限后透传"""
import sys, os, time, threading, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.chdir(os.path.dirname(os.path.abspath(__file__)))
import local_proxy as lp
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ── 1. 软限速 ──
b = lp.TokenBucket(rate=0.1, burst=4)  # 低速率避免 sleep 期间回充干扰断言
b.soft_delay_max = 2.0
ts = []
for i in range(4):
    t0 = time.monotonic()
    b.acquire()
    ts.append(time.monotonic() - t0)
print("soft acquire 耗时:", [f"{x:.2f}" for x in ts])
assert ts[0] < 0.1 and ts[1] < 0.1, "前半段不应延时"
assert 0.7 < ts[2] < 1.4, f"余量约1应延时约1s，实际 {ts[2]:.2f}"
assert 1.5 < ts[3] < 2.5, f"余量约0应延时约2s，实际 {ts[3]:.2f}"
print("软限速 OK：余量低于一半后延时渐进增大")

# ── 2. 429 总时限透传 ──
class H429(BaseHTTPRequestHandler):
    def do_POST(self):
        self.rfile.read(int(self.headers.get('Content-Length', 0)))
        body = json.dumps({"error": "rate limited"}).encode()
        self.send_response(429)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a): pass

srv = ThreadingHTTPServer(('127.0.0.1', 0), H429)
threading.Thread(target=srv.serve_forever, daemon=True).start()
port = srv.server_address[1]

lp.RATE_LIMIT_TOTAL_TIMEOUT = 6.0   # 测试用小总时限
lp.RATE_LIMIT_MAX_DELAY = 2.0
bucket = lp.TokenBucket(rate=1000, burst=100)
bucket.soft_delay_max = 0
stats = lp.StatsCollector()
t0 = time.monotonic()
status, headers, body = lp.forward_request(
    bucket, f"http://127.0.0.1:{port}", "POST",
    {"Content-Type": "application/json"}, b"{}", "/v1/chat/completions", stats)
elapsed = time.monotonic() - t0
print(f"429 透传: status={status} elapsed={elapsed:.1f}s")
assert status == 429, f"应透传 429，实际 {status}"
assert 5.0 < elapsed < 9.0, f"应在总时限 6s 附近透传，实际 {elapsed:.1f}s"
print("429 总时限 OK：超过总时限后快速失败返回调用方")
srv.shutdown()
print("ALL PASS")
