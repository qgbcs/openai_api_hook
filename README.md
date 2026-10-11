# 本地限流可靠代理

一个纯标准库实现的本地 AI API 聚合代理。同一个端口同时提供：

| 路径 | 功能 |
|------|------|
| `/r=...` | Python RPC：在进程内执行任意代码，用于探测状态、动态调参、定义拦截策略 |
| `/v1/*` | 反向代理到上游 AI API（默认 `https://api.agnes-ai.cn`） |

**核心能力**

- 令牌桶限流，参数可通过 RPC 实时调整
- 上游返回 `429` 时**不向调用方透传**，挂起并指数退避无限重试，直到成功
- 完整请求 / 响应录制，RPC 随时开启、定时自动停止、随时停止
- 通用请求拦截机制：所有判断与处理策略由 RPC 定义（如 toolcall 弹窗审核），核心代码不硬编码任何业务规则
- 终端实时显示速率、令牌余量、当前请求与累计 token 数、成功 / 失败计数
- 详细请求 / 响应日志（类似 `curl -v`）
- 附带真实限流参数压测脚本

---

## 目录结构

```
trae_auto/
├── local_proxy.py        # 代理主程序
├── probe_ratelimit.py    # 上游限流参数压测脚本
├── start_local_proxy.bat # Windows 启动脚本
├── start_proxy.sh        # Linux / macOS / WSL 启动脚本
├── recordings/          # 请求录制产物（gitignore，含敏感头信息）
└── .trae/skills/         # 相关技能定义
```

---

## 依赖

- Python 3.8+
- **零第三方依赖**，全部使用标准库
- 依赖同机的 [`multi_mqtt`](../multi_mqtt) 项目提供 `server_http`（HTTP / RPC 基础层）。
  `local_proxy.py` 会自动把上级目录的 `multi_mqtt` 加入 `sys.path`。

---

## 快速开始

Windows：

```bat
start_local_proxy.bat
```

或直接运行：

```bash
python local_proxy.py
```

启动后监听 `http://127.0.0.1:2255`。

### 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--port` | `2255` | 监听端口 |
| `--rate` | `0.15` | 令牌补充速率（请求 / 秒），来自真实压测 |
| `--burst` | `10` | 令牌桶容量（突发上限），来自真实压测 |
| `--upstream` | `https://api.agnes-ai.cn` | 上游 API 地址（不含 `/v1`） |
| `--log` | `info` | 日志级别：`debug` / `info` / `warning` |

示例：自定义端口与上游

```bash
python local_proxy.py --port 3000 --upstream http://127.0.0.1:9911 --rate 5 --burst 5
```

---

## 上游限流模型（真实压测结论）

使用 `probe_ratelimit.py` 对真实上游长时间压测，结论为：

> **60 秒滑动窗口，窗口容量约 10 个请求，可持续稳态吞吐 ≈ 0.167 req/s（约 10 次 / 分钟）。**

压测证据：

- 冷启动首次突发宽限约允许 15 个请求；随后的请求全部 `429`
- 以 1.0 rps 压测时，0–10s 用掉约 8 个配额，之后持续 `429`；**恰好在 60–70s 最早的请求滑出窗口时立即恢复成功** —— 典型滑动窗口特征
- 0.2 rps 偶发 `429`；0.3 / 0.5 / 1.0 rps 均周期性触发 `429`
- `429` 响应**不携带** `Retry-After` 或任何 ratelimit 头，只能依据时间窗口退避

因此默认值取 `rate=0.15`（60 秒稳态占 9 个，留 1 个余量）、`burst=10`（不超过窗口容量）。
在含周期性突发的 361 秒端到端压测中，48/48 请求最终成功，调用方视角零失败。

---

## RPC 接口

RPC 直接在 URL 中携带 Python 代码，在代理进程内执行，可访问运行时的全部对象。

### 查看状态

```bash
curl "http://127.0.0.1:2255/r=proxy.get_status()"
```

返回字段：

```python
{
  'instant_rate': 0.0,                 # 瞬时速率
  'rate_per_sec': {'last_1s': ..., 'last_10s': ..., 'last_60s': ...},
  'total_requests': 0,
  'success_count': 0,
  'error_count': 0,
  'error_rate': 0.0,
  'latency': {'min': ..., 'max': ..., 'avg': ..., 'p95': ...},
  'recent_429_count': 0,
  'recent_429': [...],
  'bucket': {'tokens': 10.0, 'rate': 0.15, 'burst': 10.0},
  'last_usage': {'prompt_tokens': 51, 'completion_tokens': 2, 'total_tokens': 53},
  'usage_total': {'prompt_tokens': 102, 'completion_tokens': 4, 'total_tokens': 106},
}
```

### 动态调整限流

```bash
curl "http://127.0.0.1:2255/r=proxy.set_rate(0.2)"    # 调整速率
curl "http://127.0.0.1:2255/r=proxy.set_burst(10)"    # 调整突发容量
curl "http://127.0.0.1:2255/r=proxy.reset_stats()"    # 清空统计
```

### 直接访问运行时对象

`bucket` / `stats` / `proxy` / `recorder` / `interceptor` 均暴露在 RPC 全局命名空间：

```bash
curl "http://127.0.0.1:2255/r=bucket.rate"
curl "http://127.0.0.1:2255/r=dir()"
```

> 含引号 / 括号的复杂表达式请先做 URL 编码。

---

## 请求完整录制

将经过代理的请求与响应**不截断**地落盘为 JSONL，完全由 RPC 控制。

```bash
curl "http://127.0.0.1:2255/r=recorder.start(300)"   # 录制 300 秒后自动停止
curl "http://127.0.0.1:2255/r=recorder.start()"      # 一直录制到手动停止
curl "http://127.0.0.1:2255/r=recorder.stop()"       # 随时停止
curl "http://127.0.0.1:2255/r=recorder.status()"     # 状态 / 文件 / 条数 / 剩余秒
```

文件默认保存在 `recordings/rec_YYYYMMDD_HHMMSS.jsonl`，每条记录包含：

- **请求**：方法、路径、客户端、全部请求头、完整 body
- **响应**：状态码、全部响应头、完整 body
- body 优先按 UTF-8 文本存储，非文本自动 base64

> 录制文件包含 `Authorization` 等敏感头，已在 `.gitignore` 中忽略，请勿提交或外传。

---

## 请求拦截（策略由 RPC 定义）

代理只提供**机制**：在转发前调用已注册的 hook，并支持挂起等待外部决策。
所有判断逻辑（如何识别 toolcall、如何弹窗、如何处理）都在 RPC 侧，核心代码不含任何硬编码规则。

### hook 协议

hook 接收请求上下文 `req`：

```python
{
  'seq': 1, 'method': 'POST', 'path': '/v1/chat/completions',
  'headers': {...},
  'body': b'...', 'body_text': '...', 'body_b64': None,
  'json': {...},          # 已解析的 JSON（解析失败为 None）
  'client': '127.0.0.1',
}
```

返回值：

| 返回 | 含义 |
|------|------|
| `None` | 放行，继续下一个 hook |
| `{'action': 'allow'}` | 显式放行 |
| `{'action': 'block', 'status': 403, 'body': ..., 'headers': {...}}` | 不转发，直接返回合成响应 |
| `{'action': 'modify', 'headers': {...}, 'body': ...}` | 修改请求后再转发 |

拦截发生在取令牌**之前**，被拦截的请求不消耗限流配额。

### 示例：toolcall 弹窗审核

通过 RPC 注册（无需重启代理）：

```python
def popup(entry):
    # “弹窗”方式由你定义：打印、响铃、推送到 IM、写 UI 等
    print("REVIEW", entry["key"], entry["summary"])

interceptor.notifier = popup

def toolcall_guard(req):
    j = req.get("json")
    if isinstance(j, dict) and j.get("tools"):
        return interceptor.review(req)   # 挂起，等待人工 decide
    return None

interceptor.add_hook(toolcall_guard)
```

当带 `tools` 的请求到来时会被挂起，通过另一个 RPC 随时查看并决策：

```bash
curl "http://127.0.0.1:2255/r=interceptor.pending()"
# [{'key': 1, 'waiting': 11.1, 'summary': {'tools': ['get_weather'], ...}}]

curl "http://127.0.0.1:2255/r=interceptor.decide(1, 'allow')"
curl "http://127.0.0.1:2255/r=interceptor.decide(1, 'block', status=403, body='rejected')"
curl "http://127.0.0.1:2255/r=interceptor.decide(1, 'modify', body='...')"
```

管理 hook：

```bash
curl "http://127.0.0.1:2255/r=interceptor.list_hooks()"
curl "http://127.0.0.1:2255/r=interceptor.remove_hook('toolcall_guard')"
curl "http://127.0.0.1:2255/r=interceptor.clear_hooks()"
```

`review()` 默认超时 1 小时，超时后返回 504，避免请求永久挂起。异常的 hook 会被跳过，不会阻断正常请求。

---

## 429 处理策略

- 上游 `429` **不会返回给调用方**，代理内部挂起重试
- 退避序列：`1s → 2s → 4s → ...`，单次封顶 60s
- 若上游提供 `Retry-After` 头则优先采用
- 5xx 与网络错误仍有上限（默认重试 3 次，指数退避）

---

## 压测脚本

`probe_ratelimit.py` 直接打上游（绕过代理的 429 重试），记录原始状态码与响应头，
数据落盘到 `%TEMP%/ratelimit_probe.jsonl`。

```bash
python probe_ratelimit.py burst 15              # 瞬时并发 15 个，测突发容量
python probe_ratelimit.py recover               # 每 5s 探测，测限流恢复耗时
python probe_ratelimit.py rate 0.3 100          # 以 0.3 rps 固定速率压 100 秒
python probe_ratelimit.py sweep                 # 自动分档扫描（0.2/0.3/0.5/1.0 rps）
```

> 脚本内的 API key 仅用于压测，请勿提交到公开仓库；如需共享请改为从环境变量读取。

---

## 终端实时显示

```
[03:14:14] 速率: 0.15/s [██████████░░░░░░░░░░] 令牌: 9.0/10 成功: 48 失败: 0 429: 0 最近tok: 51+2=53 累计tok: 106
```

详细响应日志中每个带用量的响应还会单独打印：

```
TOKENS: prompt=51 completion=2 total=53
```

按 `Ctrl+C` 退出。








新错误定位 ：这是另一个独立问题。Search Agent 是 Task 工具派出的子代理（system prompt: "file search specialist"），录制铁证：

- 同一个 36102B 请求被 字节不变地重发 12 次 （MD5 全部相同），每次 agnes 都在 2-4 秒内返回完整合法的 4 个 Grep 工具调用
- 每次重发云端都换 IDC 集群（HL→LF→LQ）和入口 IPv6——是 云端调用链在拒并重试 ，约 70 秒后放弃回 502
- 主会话同样的请求机制全程正常，仅子代理路径异常；且 TTFB 越长（代理注入的保活帧越多）云端判定耗时越长——高度怀疑子代理链路不容忍代理注入的心跳帧
实验性修复已上线 ：识别子代理请求后 纯透传 （关闭心跳注入 + hedge 竞速，不改请求体），策略仍在 hook 中、核心只提供通用开关通道。
