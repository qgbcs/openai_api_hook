# local_proxy RPC 使用手册

本地聚合代理（`local_proxy.py`）在同一个端口（默认 **2255**）上提供两类服务：

| 路径 | 行为 |
|---|---|
| `/v1/*` | 反向代理到 `https://api.agnes-ai.cn/v1`（令牌桶限速、429 重试、流式竞速/心跳） |
| **其他任意路径** | 全部当作 **RPC**：路径本身就是一段可执行的 Python 代码 |

本文档只讲 RPC。

---

## 1. 快速开始
PowerShell 把 curl 映射成了 Invoke-WebRequest，改用 curl.exe。

```bash
# 查询代理状态
curl.exe http://127.0.0.1:2255/r=proxy.get_status()

# 调整限速
curl.exe http://127.0.0.1:2255/r=proxy.set_rate(0.2)
curl.exe http://127.0.0.1:2255/r=proxy.set_burst(8)

# 查看最近请求 / 取最新一条完整快照
curl.exe http://127.0.0.1:2255/r=recent.list()
curl.exe http://127.0.0.1:2255/r=recent.get(-1)
```

> **Windows PowerShell 注意**：`curl` 是 `Invoke-WebRequest` 的别名，必须用 **`curl.exe`**。
> 含空格、引号、中文的代码要做 URL 编码（见 §3）。

---

## 2. 执行模型（必读）

### 2.1 URL 即代码

服务器把去掉首个 `/` 后的路径做一次 percent-decode，然后交给一个**持久化 Python REPL** 执行：

- 代码以**裸表达式结尾** → 该表达式的值就是 HTTP 响应体（REPL 语义）；
- `r=...` 是一种约定写法：`r=expr` 本身是赋值语句，执行器最后回退返回命名空间里的 `r`；
- 多语句代码（赋值、`import`、`def`、`for` 等）正常执行，状态（变量、函数、已导入模块）**在后续 RPC 请求之间持续保留**；
- `print(...)` 的输出被捕获，表达式结果优先于 print 输出返回；
- 代码抛异常 → HTTP **500**，响应体是完整 traceback（同时写入代理控制台和 `proxy.live.log`）。

```bash
curl.exe -s "http://127.0.0.1:2255/r=1+2"                 # → 3（裸表达式直接求值）
curl.exe -s "http://127.0.0.1:2255/r=r=bucket.rate"       # → 0.15（结果赋给 r 并返回）
curl.exe -s "http://127.0.0.1:2255/r=r=[i*i for i in range(5)]"
curl.exe -s "http://127.0.0.1:2255/r=x=42"                # 既无结尾表达式也无 r：
                                                          # → no 'r' variable, locals keys: [...]
```

GET、POST 都走同一个处理器；**代码只从 URL 路径取，POST body 会被忽略**。

### 2.2 请求级上下文变量

每次 RPC 执行期间临时注入四个变量，执行完自动还原，不会污染持久命名空间：

| 变量 | 说明 |
|---|---|
| `request` / `q` | 本次 HTTP 请求 handler（可读 `request.headers`、`client_address` 等） |
| `response` / `p` | 响应包装器，用于自定义状态码、响应头、**流式输出** |

`response`（别名 `p`）的方法：

- `p.set_data(obj)`：一次性返回任意对象（执行结束后统一写回）；
- `p.set_status(code)` / `p.set_header(k, v)`：自定义状态码与头；
- `p.write(data)`：**流式分块写回**，首次调用立即提交响应头，适合边算边输出的大文本。

```python
# 示例：流式返回（多行代码建议用 §3 的方式发送）
p.set_status(200)
p.set_header("Content-Type", "text/plain; charset=utf-8")
for i in range(3):
    p.write(f"chunk {i}\n".encode())
```

### 2.3 安全警告

代理默认 `key=''`（无鉴权）且监听 `0.0.0.0:2255`，即**同一网络内任何人都能通过 RPC 执行任意 Python 代码**（等价于本机 shell 权限）。仅可在可信内网运行；需要公网暴露时必须加鉴权 key 或绑回 `127.0.0.1`。

---

## 3. 复杂/多行代码怎么发

代码里有空格、引号、中文或换行时，用 URL 编码。最省事的是用一小段 Python 发起：

```python
import urllib.parse, urllib.request

code = r'''
import json
x = recent.get(-1)
b = json.loads(x["req_body"])
r = [m.get("role") for m in b.get("messages", [])]
'''

url = "http://127.0.0.1:2255/" + urllib.parse.quote(code, safe="")
print(urllib.request.urlopen(url, timeout=30).read().decode("utf-8"))
```

单行内联写法（PowerShell 里注意双引号转义）：

```powershell
curl.exe -s "http://127.0.0.1:2255/r=r=%5Bi*i%20for%20i%20in%20range(5)%5D"
```

---

## 4. RPC 全局对象一览

执行器直接持有 `local_proxy` 模块的 globals，启动后这些对象均可直接访问：

| 对象 | 类型 | 用途 |
|---|---|---|
| `proxy` | `ProxyManager` | 代理总控：查状态、动态调速、重置统计 |
| `bucket` | `TokenBucket` | 令牌桶：rate/burst/当前余量/软延迟 |
| `stats` | `StatsCollector` | 请求计数、错误率、延迟、token 用量 |
| `recent` | `RecentRing` | 最近 **3** 个请求/响应的完整内存快照 |
| `recorder` | `RequestRecorder` | 不截断地录制请求/响应到 JSONL 文件 |
| `interceptor` | `Interceptor` | 运行时增删拦截 hook、人工审核决策 |
| `_detail_logger` | `DetailLogger` | `.verbose=True` 展开全部请求头/body 日志 |

此外模块级函数（如 `extract_usage(body)`、转发常量 `MAX_RETRIES` 等）也都在命名空间里，可用于自定义脚本，但**不要从 RPC 调用 `bucket.acquire()`**——它会真正睡眠并消耗一个配额。

---

## 5. 状态查询与动态调速：`proxy` / `bucket` / `stats`

### 5.1 `proxy.get_status()`

返回字段说明：

```jsonc
{
  "instant_rate": 0.10,            // 按最后两次请求间隔算出的瞬时速率
  "rate_per_sec": {"last_1s": 0.0, "last_10s": 0.0, "last_60s": 0.0},
  "total_requests": 7,             // 最近 60s 窗口内请求数
  "success_count": 40,             // 累计成功（含 2xx）
  "error_count": 0,                // 累计 429 次数（注意：只统计 429）
  "error_rate": 0.0,
  "latency": {"min": ..., "max": ..., "avg": ..., "p95": ...},
  "recent_429_count": 0,
  "recent_429": [],                // 最近 5 次 429 的路径/响应头
  "bucket": {"tokens": 9.0, "rate": 0.15, "burst": 10.0},
  "last_usage": {"prompt_tokens": 74354, "completion_tokens": 148, ...},
  "usage_total": {"prompt_tokens": 2054706, "completion_tokens": 18212, ...}
}
```

### 5.2 常用命令

```bash
curl.exe -s "http://127.0.0.1:2255/r=proxy.get_status()"
curl.exe -s "http://127.0.0.1:2255/r=proxy.set_rate(0.2)"     # 稳态速率 req/s
curl.exe -s "http://127.0.0.1:2255/r=proxy.set_burst(8)"      # 突发容量
curl.exe -s "http://127.0.0.1:2255/r=proxy.reset_stats()"     # 清空计数与 token 累计
```

直接改桶参数也可以（立即生效）：

```python
bucket.rate = 0.2
bucket.burst = 10
bucket.soft_delay_max = 8     # 余量低于一半时的软延迟上限（秒）；0 关闭
r = {"rate": bucket.rate, "burst": bucket.burst, "tokens": bucket._tokens}
```

> 默认值来自压测：`rate=0.15 req/s`、`burst=10`（上游为 60s 滑动窗口、容量约 10）。
> `soft_delay_max` 默认 8s，用于令牌余量过半时渐进加延迟，避免集中触发上游 429。

---

## 6. 请求快照排查：`recent`

环形缓冲只保留**最近 3 条**（容量在代码里写死），含请求头、请求体、响应头、响应体全文。

### 6.1 `recent.list()`

摘要列表，每项字段：`id / arrival / method / path / status / req_bytes / resp_bytes / wait_s / up_ms / ttfb_ms / retries`。

- `id`：日志里 `>>> #N` 的请求序号；
- `wait_s`：发往上游前在令牌桶的等待秒数；
- `up_ms`：纯上游处理耗时；`ttfb_ms`：首字节到达耗时。

### 6.2 `recent.get(key)`

`key` 同时支持**请求 ID** 和**环形下标**（非负整数优先按 ID 匹配，匹配不到再按下标；负数永远是下标）：

```bash
curl.exe -s "http://127.0.0.1:2255/r=recent.get(-1)"     # 最新一条
curl.exe -s "http://127.0.0.1:2255/r=recent.get(40)"     # 按请求 ID=#40 查
curl.exe -s "http://127.0.0.1:2255/r=recent.get(0)"      # 缓冲中最旧一条（下标）
```

ID 已被挤出缓冲时返回明确错误，而不是裸 `IndexError`：

```text
KeyError: '找不到请求 ID=37 的快照（仅保留最近 3 条，可用 ID: [38, 39, 40]）'
```

返回结构（敏感头 `Authorization` 已打码）：

```jsonc
{
  "id": 40, "arrival": "19:44:01.305", "method": "POST",
  "path": "https://api.agnes-ai.cn/v1/chat/completions",
  "status": 200, "wait_s": 0.0, "up_ms": 1256.0, "ttfb_ms": 6312.0,
  "req_headers": {"authorization": "***", ...},
  "req_body": "<文本化后的请求体字符串>",
  "resp_headers": [["Content-Type", "text/event-stream"], ...],
  "resp_body": "data: {...}\n\ndata: [DONE]\n",
  "retry_events": []
}
```

### 6.3 实战：解析请求体

**坑提示**：`get()` 返回的 `req_body` / `resp_body` 已经是 **`str`**（内部做过 decode），不要再调 `.decode()`，否则报 `'str' object has no attribute 'decode'`。

```python
import json
x = recent.get(-1)
b = json.loads(x["req_body"])
r = {
    "model": b.get("model"),
    "stream": b.get("stream"),
    "messages": len(b.get("messages", [])),
    "roles": [m.get("role") for m in b.get("messages", [])],
    "tools": [t.get("function", {}).get("name")
              for t in b.get("tools", []) if isinstance(t, dict)][:15],
}
```

只看上游响应开头/结尾：

```python
x = recent.get(-1)
rb = x.get("resp_body") or ""
r = rb[:300] + "\n...\n" + rb[-300:]
```

### 6.4 展开实时详细日志

```bash
curl.exe -s "http://127.0.0.1:2255/r=_detail_logger.verbose=True"
curl.exe -s "http://127.0.0.1:2255/r=_detail_logger.verbose=False"
```

开启后控制台/`proxy.live.log` 会打印每个请求的全部头与 body。

---

## 7. 完整录制：`recorder`

`recent` 只留 3 条且重启即失；`recorder` 把经过代理的请求与响应**不截断**落盘为 JSONL，默认目录为脚本旁的 `recordings/`。

```bash
# 录制 300 秒后自动停止
curl.exe -s "http://127.0.0.1:2255/r=recorder.start(300)"
# 一直录到手动停止；可自定义文件名（相对路径落在 recordings/，也可给绝对路径）
curl.exe -s "http://127.0.0.1:2255/r=recorder.start()"
curl.exe -s "http://127.0.0.1:2255/r=recorder.start(path='case1.jsonl')"
# 查看状态 / 停止
curl.exe -s "http://127.0.0.1:2255/r=recorder.status()"
curl.exe -s "http://127.0.0.1:2255/r=recorder.stop()"
```

`status()` 返回 `{active, path, count, remaining}`。

JSONL 每行一个对象：

- `{"event": "recording_start" / "recording_stop", ...}`：起止标记；
- `{"kind": "request", "seq", "time", "method", "path", "client", "headers", "body_text", "body_b64"}`；
- `{"kind": "response", "seq", "status", "headers", "body_text", "body_b64", "note"}`；
  - 文本走 `body_text`，二进制走 `body_b64`（base64）；
  - 被拦截的合成响应 `note="intercepted"`，上游异常 `note="upstream error"`。
- 请求与响应靠同一个 `seq` 配对（注意 `seq` 与日志里的 `#id` 是两个独立计数）。

---

## 8. 请求拦截：`interceptor`（策略全部由 RPC 定义）

代理核心不含任何业务判断：转发前它只是依次调用 hook 链，**所有拦截逻辑通过 RPC 动态注册**。hook 在取令牌**之前**执行，拦截不消耗上游配额。

### 8.1 hook 入参与返回值

hook 是一个普通 Python 函数，参数 `req` 结构：

```python
{
    "seq": 12,                 # 代理内部序号
    "method": "POST",
    "path": "/v1/chat/completions",
    "headers": {...},          # 已剔除 hop-by-hop 头，附带 X-Forwarded-For / X-Real-URL
    "body": b"...",            # 原始字节
    "body_text": "...",        # 文本化结果（与 body_b64 二选一有值）
    "body_b64": None,
    "json": {...} or None,     # body 是合法 JSON 时已解析好的 dict
    "client": "127.0.0.1",
}
```

返回值约定：

| 返回 | 含义 |
|---|---|
| `None` | 放行，继续执行下一个 hook |
| `{"action": "allow"}` | 显式放行（等价于 None，但短路后续 hook） |
| `{"action": "block", "status": 403, "body": "...", "headers": {...}}` | 不转发，直接回合成响应（`"respond"` 是同义别名） |
| `{"action": "modify", "headers": {...}, "body": str\|bytes}` | 修改请求后再转发（改 body 时记得同步 `Content-Length`） |

hook 自身抛异常会被捕获记日志并跳过，不会阻断请求。

### 8.2 管理 hook

```python
# 1) 定义并注册（函数在持久命名空间里，后续请求都会经过它）
def block_big_model(req):
    j = req.get("json")
    if isinstance(j, dict) and "pro" in str(j.get("model", "")):
        return {"action": "block", "status": 403, "body": "pro model disabled"}

interceptor.add_hook(block_big_model)        # 同名重注册会覆盖旧的
interceptor.add_hook(block_big_model, name="block_pro")

# 2) 查看 / 移除 / 清空
interceptor.list_hooks()                     # ['sanitize_chat_messages', 'block_big_model']
interceptor.remove_hook("block_big_model")
interceptor.clear_hooks()                    # 注意：会连默认 hook 一起清掉
```

运行时注册的 hook 在代理重启后丢失（重启只自动注册内置 hook）。

### 8.3 人工审核：挂起请求 + 异步决策

hook 里调用 `interceptor.review(req)` 可把请求挂起，等另一条 RPC 用 `decide()` 放行/拦截/修改：

```python
def toolcall_guard(req):
    """带 tools 定义的请求先挂起，等人在另一条 RPC 里 decide"""
    j = req.get("json")
    if isinstance(j, dict) and j.get("tools"):
        return interceptor.review(req)       # 阻塞转发线程，等待决策
    return None

interceptor.add_hook(toolcall_guard)
```

请求挂起后，在任意时刻用另一个 RPC 会话决策：

```bash
# 查看等待中的请求（key / 已等待秒数 / 路径与工具摘要）
curl.exe -s "http://127.0.0.1:2255/r=interceptor.pending()"

# 放行
curl.exe -s "http://127.0.0.1:2255/r=interceptor.decide(1,%20%27allow%27)"
# 拦截：自定义状态码和响应体
curl.exe -s "http://127.0.0.1:2255/r=interceptor.decide(1,%20%27block%27,%20status=403,%20body=%27rejected%27)"
# 修改请求体后转发
curl.exe -s "http://127.0.0.1:2255/r=interceptor.decide(1,%20%27modify%27,%20body=%27...%27)"
```

决策超时（默认 `interceptor.default_timeout = 3600` 秒，`review(req, timeout=N)` 可按请求覆盖）后自动以 **504** 拦截。

### 8.4 自定义挂起通知

挂起时如何"通知人"也由 RPC 定义，代理不内置任何方式：

```python
def ding(entry):
    # entry 含 key/req/summary/started；这里可发响铃、弹窗、webhook、MQTT 消息等
    print(">>> 待审核请求 #%s: %s" % (entry["key"], entry["summary"]))

interceptor.notifier = ding
interceptor.notifier = None     # 关闭通知
```

### 8.5 内置 hook（`builtin_hooks`）

启动时自动注册 `sanitize_chat_messages`，模块挂在 `interceptor.builtin_hooks` 上：

- **`sanitize_chat_messages`**（默认开启）：
  1. 删除 content 为空串/纯空白/空数组的 user 消息；
  2. 全请求图片 part 超过 **4** 张时，从最早的图片开始移除，移除后变空的纯图消息整条删除。
  规避上游网关"每请求最多 4 图"和 vllm 空 parts 校验导致的 400。
- **`fix_empty_user_parts`**（默认不开启）：只删空 user 消息，不管图片数量。

运行时管理：

```python
import builtin_hooks
builtin_hooks.MAX_CHAT_IMAGES = 6                       # RPC 调阈值（对后续请求生效）
interceptor.add_hook(builtin_hooks.fix_empty_user_parts)
interceptor.remove_hook("sanitize_chat_messages")       # 关闭默认 hook
```

---

## 9. 常用排查套路速查

```bash
# 1. 代理是否健康 / 有没有 429
curl.exe -s "http://127.0.0.1:2255/r=proxy.get_status()"

# 2. 最近请求的状态、ttfb、重试次数
curl.exe -s "http://127.0.0.1:2255/r=recent.list()"

# 3. 失败请求的上游响应原文（用日志里的 #ID）
curl.exe -s "http://127.0.0.1:2255/r=recent.get(39)"

# 4. 开启全量详细日志，复现后关闭
curl.exe -s "http://127.0.0.1:2255/r=_detail_logger.verbose=True"

# 5. 录一段完整现场（300 秒）
curl.exe -s "http://127.0.0.1:2255/r=recorder.start(300)"

# 6. 运行时临时放宽/收紧限速
curl.exe -s "http://127.0.0.1:2255/r=proxy.set_rate(0.25)"
curl.exe -s "http://127.0.0.1:2255/r=proxy.set_burst(12)"

# 7. 查看 / 清空当前所有拦截 hook
curl.exe -s "http://127.0.0.1:2255/r=interceptor.list_hooks()"
```

现场日志还会持续同步写入脚本同目录的 **`proxy.live.log`**（控制台窗口丢失也不影响排查），
其中包含：启动参数、每个请求的 `>>>` / `<<<` 单行日志、429 与连接重试、
hedge 竞速（主连接 12s 无响应头时启动备用连接）、客户端提前断开、RPC 异常堆栈。

---

## 10. 注意事项汇总

1. **PowerShell 用 `curl.exe`**；复杂代码走 URL 编码或用 Python `urllib` 发送（§3）。
2. 代码在**持久 REPL**中执行：可定义函数长期驻留，也可能被后来的 RPC 覆盖；代理重启全部归零。
3. `recent` 只有 3 条，需要更长现场用 `recorder`；`get()` 的 body 字段是 **str 不是 bytes**。
4. 不要在 RPC 里调 `bucket.acquire()`、不要在 hook 里做长时间阻塞（除有意的 `review()` 外）。
5. hook 链运行在转发线程中：`clear_hooks()` 会同时移除默认的 `sanitize_chat_messages`。
6. 500 响应体是 traceback；改代码语法错误不会影响在跑的代理，只有重启才加载新代码。
7. RPC 无鉴权且默认监听全部网卡，**切勿暴露到不可信网络**。
