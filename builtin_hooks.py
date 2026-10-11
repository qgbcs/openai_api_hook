# -*- coding: utf-8 -*-
"""代理内置 hook 集合。

主文件只需一行::

    from builtin_hooks import register_defaults
    register_defaults(interceptor)

默认开启 sanitize_chat_messages；fix_empty_user_parts 保留在此文件但默认
不开启，需要时可由 RPC 注册::

    interceptor.add_hook(interceptor.builtin_hooks.fix_empty_user_parts)
"""
import json
import logging
import sys
import time

# 上游每个请求只保留最后 4 个图片 part（按 image part 计数，与图片
# 大小、请求总大小、含图消息条数均无关：实测 1x1 迷你图第 5 个照样
# 触发，同一条消息放 5 图则 200）。更早的图片 part 会被其网关移除；
# 若被移除图片所在 user 消息因此变空（纯图消息），content 变成空
# 数组 []，后端 vllm 校验 User 消息 parts 为空即 400。RPC 可改此值。
MAX_CHAT_IMAGES = 4


def sanitize_chat_messages(req):
    """清理空 user 消息；图片超 4 张时从最早移除，纯图消息整条删除

    转发前修正两类必被上游 400 的消息结构：

    1. user 消息 content 为空字符串、纯空白或空数组：整条移除；
    2. 图片总数超过 MAX_CHAT_IMAGES：从最早出现的图片开始移除，
       移除后若该消息变空（纯图消息），整条消息删除——不能把空
       parts 留给上游。assistant 纯空白 content 合法，不动。
    """
    data = req.get("json")
    if not isinstance(data, dict):
        return None
    messages = data.get("messages")
    if not isinstance(messages, list):
        return None

    def is_empty_user(m):
        if not isinstance(m, dict) or m.get("role") != "user":
            return False
        c = m.get("content")
        if isinstance(c, list):
            return len(c) == 0
        if isinstance(c, str):
            return not c.strip()
        return False

    notes = []

    # 1) 空 user 消息整条移除
    n_empty = sum(1 for m in messages if is_empty_user(m))
    if n_empty:
        messages = [m for m in messages if not is_empty_user(m)]
        notes.append(f"移除空 user 消息 {n_empty} 条")

    # 2) 图片超额：收集全部图片位置（消息序号, part 序号），按出现顺序
    img_positions = []
    for mi, m in enumerate(messages):
        c = m.get("content")
        if isinstance(m, dict) and isinstance(c, list):
            for pi, p in enumerate(c):
                if isinstance(p, dict) and p.get("type") == "image_url":
                    img_positions.append((mi, pi))

    n_drop = len(img_positions) - MAX_CHAT_IMAGES
    if n_drop > 0:
        drop_parts = {}        # mi -> set(pi)
        drop_msgs = set()
        for mi, pi in img_positions[:n_drop]:
            drop_parts.setdefault(mi, set()).add(pi)
        new_messages = []
        for mi, m in enumerate(messages):
            if mi in drop_parts:
                c = m.get("content")
                kept_parts = [p for pi, p in enumerate(c) if pi not in drop_parts[mi]]
                if not kept_parts:
                    drop_msgs.add(mi)   # 纯图消息：整条删，不留空 parts
                    continue
                m = dict(m); m["content"] = kept_parts
            new_messages.append(m)
        messages = new_messages
        notes.append(f"图片 {len(img_positions)} 张超上限 {MAX_CHAT_IMAGES}，"
                     f"移除最早 {n_drop} 张（其中整条删除纯图消息 {len(drop_msgs)} 条）")

    if not notes:
        return None
    if not messages:
        return None  # 删空了整个对话，放行让上游自己报错更合理

    data = dict(data)
    data["messages"] = messages
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    logging.warning("[sanitize_chat_messages] %s", "；".join(notes))
    return {"action": "modify", "body": body,
            "headers": {"Content-Length": str(len(body))}}


def fix_empty_user_parts(req):
    """只移除 content 为空的 user 消息，不处理图片数量

    与 sanitize_chat_messages 的区别：不管图片数量，只处理空 content。
    空数组 [] 或纯空白字符串的 user 消息整条移除；assistant 纯空白
    content 合法，不动。
    """
    data = req.get("json")
    if not isinstance(data, dict):
        return None
    messages = data.get("messages")
    if not isinstance(messages, list):
        return None

    def is_empty_user(m):
        if not isinstance(m, dict) or m.get("role") != "user":
            return False
        c = m.get("content")
        if isinstance(c, list):
            return len(c) == 0
        if isinstance(c, str):
            return not c.strip()
        return False

    n_bad = sum(1 for m in messages if is_empty_user(m))
    if n_bad == 0:
        return None
    kept = [m for m in messages if not is_empty_user(m)]
    if not kept:
        return None  # 删空了整个对话，放行让上游自己报错更合理
    data = dict(data)
    data["messages"] = kept
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    logging.warning("[fix_empty_user_parts] 移除 %d 条空 user 消息（%d→%d）",
                    n_bad, len(messages), len(kept))
    return {"action": "modify", "body": body,
            "headers": {"Content-Length": str(len(body))}}


# ─── 对话压缩（compact）摘要修复 ─────────────────────────────────────────────
#
# 背景（2026-10-11 实测，录制见 recordings/）：Trae 的“压缩历史”由 ai-agent
# 把 /compact 转成一个普通流式 OpenAI 请求，最后一条 user 消息是固定的摘要
# 指令（见下方 SUMMARY_PROMPT_MARK），要求模型把摘要包在 <summary></summary>
# 里。该请求经“ai-agent → Trae 云端中转 → 本地隧道 → 本代理 → agnes”，
# 云端中转对整轮响应有约 60s 总时限，超时直接回 "upstream failed to respond
# (HTTP Status: 502)"（4028 CUSTOM_MODEL_ORIGIN_ERROR）。
#
# 实测失败链：
#   1) 首次请求带全量历史（120k tokens）+ 27 个工具定义，模型 13s 就出完，
#      但漏掉 <summary> 包裹标签 → ai-agent 判格式不符，丢弃并重发；
#   2) 截断重试（98k/66k/6k tokens）虽然格式合格，但 agnes 大上下文 prefill
#      慢，整轮 33~69s，越过云端 60s deadline → 502，压缩失败。
#
# 修复（两层，全部在本 hook 内，核心代理只提供通用 feed/flush 机制）：
#   请求侧：识别摘要请求后移除 tools/tool_choice（ai-agent 自己重试时也会
#           去掉），并在指令末尾强化标签要求；
#   响应侧：SummaryTagGuard 逐 SSE 事件保证正文以 <summary> 开头、
#           finish 前补 </summary>。首次请求本来就快（~13s），格式一保
#           证，压缩在云端时限内即可完成，不会进入截断重试。

SUMMARY_PROMPT_MARK = "Your task is to create a detailed summary of the conversation so far"
SUMMARY_OPEN = "<summary>"
SUMMARY_CLOSE = "</summary>"
_FORMAT_HINT = (
    "\n\nIMPORTANT OUTPUT FORMAT: Output ONLY the summary itself, wrapped exactly in "
    "<summary></summary> tags as shown in the example above. Your response MUST begin "
    f"with {SUMMARY_OPEN} and end with {SUMMARY_CLOSE}, with no text before or after the tags."
)


def _last_user_text(messages):
    """返回最后一条 user 消息的文本（str 内容或 list parts 拼接）及是否可改写。"""
    for m in reversed(messages):
        if not isinstance(m, dict) or m.get("role") != "user":
            continue
        c = m.get("content")
        if isinstance(c, str):
            return c, "str"
        if isinstance(c, list):
            parts = [p.get("text", "") for p in c
                     if isinstance(p, dict) and p.get("type") == "text"]
            if parts:
                return "\n".join(parts), "list"
    return None, None


class SummaryTagGuard:
    """流式 SSE 标签守卫（feed/flush 协议由 local_proxy 调用）。

    仅做确定性的标签补齐，不改写模型正文：
      - 首个正文片段若不是 <summary> 开头，先注入一个开标签事件；
      - 全程扫描正文，正文已含 </summary> 则不补；
      - finish_reason 到达（或流结束）时仍缺闭标签，注入一个闭标签事件。
    任何上游非 data 帧、心跳注释帧、[DONE]、无 content 的帧原样透传。
    """

    def __init__(self, model=""):
        self._model = model or ""
        self._buf = b""              # 半条 SSE 事件的残余字节
        self._hold = b""             # 开标签判定前缓存的完整事件
        self._lead = ""              # 判定前累计的开头正文
        self._decided = False        # 是否已确定开标签处理
        self._has_open = False
        self._has_close = False
        self._seen_content = False
        self.injected = {"open": False, "close": False}
        # 诊断时序（毫秒，相对守卫创建=请求即将转发上游）：定位云端
        # stream_retry_policy 到底卡在首 token、流中停顿还是完成确认
        self._t0 = time.monotonic()
        self._t_first_frame = None
        self._t_first_content = None
        self._t_last_content = None
        self._t_finish = None
        self._t_done = None
        self._max_gap = 0.0
        self._text_tail = ""          # 已转发正文末尾滚动窗口：跨帧检测闭标签

    def _ms(self):
        return (time.monotonic() - self._t0) * 1000.0

    def _note_text(self, content):
        """记录一段正文，滚动窗口跨帧检测闭标签。

        模型常把 ``</summary>`` 拆成两个 delta（如 ``</summary`` + ``>``），
        只在单帧内 ``in`` 会漏检，导致结尾重复注入闭标签。
        """
        tail = self._text_tail + content
        if SUMMARY_CLOSE in tail:
            self._has_close = True
        self._text_tail = tail[-(len(SUMMARY_CLOSE) - 1):]

    def _timing_log(self):
        logging.warning(
            "[fix_compact_summary] 响应时序 first_frame=%sms first_content=%sms "
            "last_content=%sms finish=%sms done=%sms 最大帧间隔=%.0fms 补标签=%s",
            None if self._t_first_frame is None else int(self._t_first_frame),
            None if self._t_first_content is None else int(self._t_first_content),
            None if self._t_last_content is None else int(self._t_last_content),
            None if self._t_finish is None else int(self._t_finish),
            None if self._t_done is None else int(self._t_done),
            self._max_gap, self.injected)

    def _inject(self, text):
        obj = {
            "id": "chatcmpl-proxy-compactfix",
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": self._model,
            "choices": [{"index": 0, "delta": {"content": text},
                         "finish_reason": None}],
        }
        return b"data: " + json.dumps(obj, ensure_ascii=False).encode("utf-8") + b"\n\n"

    def _decide_open(self):
        """根据累计开头正文判定：自然开标签 / 需注入开标签。返回 (done, inject?)。"""
        stripped = self._lead.lstrip()
        if not stripped:
            return False, None          # 还只是空白，继续等
        if SUMMARY_OPEN.startswith(stripped) and len(stripped) < len(SUMMARY_OPEN):
            return False, None          # 正在形成标签前缀，再等几个字符
        if stripped.startswith(SUMMARY_OPEN):
            self._has_open = True
            return True, False
        return True, True               # 首字符不可能形成标签 → 需要注入

    def _on_event(self, raw):
        """处理一条完整的 SSE 事件（含结尾 \\n\\n），返回应转发的字节。"""
        if not raw.startswith(b"data:"):
            return raw                  # 注释帧等原样透传
        now = self._ms()
        if self._t_first_frame is None:
            self._t_first_frame = now
        payload = raw[5:].lstrip(b" ")
        if payload.rstrip(b"\r\n") == b"[DONE]":
            self._t_done = now
            return self._finish(raw)
        try:
            ev = json.loads(payload)
        except (ValueError, TypeError):
            return self._release(raw)
        choices = ev.get("choices")
        if not isinstance(choices, list) or not choices:
            return self._release(raw)
        delta = choices[0].get("delta") or {}
        content = delta.get("content")
        finish = choices[0].get("finish_reason")
        if finish:
            self._t_finish = now

        if isinstance(content, str) and content:
            self._seen_content = True
            if self._t_first_content is None:
                self._t_first_content = now
            elif self._t_last_content is not None:
                self._max_gap = max(self._max_gap, now - self._t_last_content)
            self._t_last_content = now
            self._note_text(content)
            if not self._decided:
                self._hold += raw
                self._lead += content
                done, do_inject = self._decide_open()
                if not done:
                    return b""
                self._decided = True
                held, self._hold = self._hold, b""
                self._lead = ""
                if do_inject:
                    self.injected["open"] = True
                    return self._inject(SUMMARY_OPEN + "\n") + held
                return held
            return raw

        if finish and not (isinstance(content, str) and content):
            return self._finish(raw)
        return self._release(raw)

    def _release(self, raw):
        """判定期间外的帧直接放行；若还有 hold 先吐出。"""
        if self._hold:
            held, self._hold = self._hold, b""
            return held + raw
        return raw

    def _finish(self, raw):
        """终结帧到达前补闭标签（finish 帧与 [DONE] 都会进入，靠 _has_close 去重）。"""
        out = self._release(b"")
        if self._seen_content and not self._has_close:
            self.injected["close"] = True
            self._has_close = True
            out += self._inject("\n" + SUMMARY_CLOSE)
        return out + raw

    def feed(self, data):
        out = []
        self._buf += data
        while b"\n\n" in self._buf:
            raw, self._buf = self._buf.split(b"\n\n", 1)
            out.append(self._on_event(raw + b"\n\n"))
        return b"".join(out)

    def flush(self):
        out = []
        # 残余半个事件：正常不会有（上游均以 \n\n 结尾），兜底原样发出
        if self._buf:
            tail, self._buf = self._buf, b""
            if not self._decided:
                self._decided = True
                if self._hold:
                    held, self._hold = self._hold, b""
                    stripped = self._lead.lstrip()
                    if not stripped.startswith(SUMMARY_OPEN):
                        self.injected["open"] = True
                        out.append(self._inject(SUMMARY_OPEN + "\n"))
                    out.append(held)
            out.append(tail)
        if self._hold:
            held, self._hold = self._hold, b""
            stripped = self._lead.lstrip()
            if not self._decided and not stripped.startswith(SUMMARY_OPEN):
                self.injected["open"] = True
                out.append(self._inject(SUMMARY_OPEN + "\n"))
            out.append(held)
        if self._seen_content and not self._has_close:
            self.injected["close"] = True
            self._has_close = True
            out.append(self._inject("\n" + SUMMARY_CLOSE))
        self._timing_log()
        if self.injected["open"] or self.injected["close"]:
            logging.warning("[fix_compact_summary] 摘要标签补齐 open=%s close=%s",
                            self.injected["open"], self.injected["close"])
        return b"".join(out)


def fix_compact_summary(req):
    """压缩摘要请求：移除工具定义、强化标签格式指令、挂响应标签守卫

    识别条件：最后一条 user 消息以 ai-agent 固定摘要指令开头。只对这类
    请求生效，普通对话完全不受影响。
    """
    data = req.get("json")
    if not isinstance(data, dict):
        return None
    messages = data.get("messages")
    if not isinstance(messages, list) or not messages:
        return None
    text, _kind = _last_user_text(messages)
    # 真实流量中该 user 消息前面常带数百字 IDE system-reminder（打开文件
    # 通知、语言设置等），摘要指令在消息中部，必须全消息匹配
    if not text or SUMMARY_PROMPT_MARK not in text:
        return None

    data = dict(data)
    messages = [dict(m) for m in messages]
    data["messages"] = messages
    notes = []

    # 1) 摘要调用不需要任何工具（ai-agent 自身的截断重试也不带工具）
    if "tools" in data:
        data.pop("tools", None)
        data.pop("tool_choice", None)
        notes.append("移除工具定义")

    # 2) 在摘要指令末尾强化标签要求
    last = messages[-1]
    for m in reversed(messages):
        if isinstance(m, dict) and m.get("role") == "user":
            last = m
            break
    c = last.get("content")
    if isinstance(c, str):
        last["content"] = c + _FORMAT_HINT
        notes.append("追加格式指令")
    elif isinstance(c, list):
        for p in reversed(c):
            if isinstance(p, dict) and p.get("type") == "text":
                p["text"] = p.get("text", "") + _FORMAT_HINT
                notes.append("追加格式指令")
                break

    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    guard = SummaryTagGuard(model=data.get("model", ""))
    # 压缩请求 prefill 本来就慢，hedge 会同时打两条相同大请求抢占上游算力，
    # 反而拖慢首字；此通道由 hook 按请求关闭，核心默认行为不变
    logging.warning("[fix_compact_summary] 命中压缩摘要请求：%s", "；".join(notes))
    return {"action": "modify",
            "body": body,
            "headers": {"Content-Length": str(len(body))},
            "stream_filter": guard,
            "hedge": False}


# ─── 子代理（Task/search）纯透传 ────────────────────────────────────────────
#
# 实测（2026-10-12，recordings/rec_20261012_001539.jsonl #60-#71）：Trae 的
# Task 工具派出的 file-search 子代理，其请求被字节不变地重发 12 次，每次
# 上游都 200 完整返回合法 tool_calls，云端调用链仍判失败换 IDC（HL/LF/LQ）
# 重发，约 70s 后给客户端 502。主会话同样的心跳帧机制全程正常，仅子代理
# 路径受影响——该链路对代理注入帧（首字节前零内容 assistant 帧 / 流中
# 注释帧）不容忍。对这类请求关闭心跳与 hedge，纯透传上游 SSE。

SUBAGENT_SYSTEM_MARKS = (
    "You are a file search specialist for Trae IDE",
)


def subagent_passthrough(req):
    """识别 Trae 子代理请求：不改 body，仅关闭心跳注入与 hedge 竞速。"""
    data = req.get("json")
    if not isinstance(data, dict):
        return None
    messages = data.get("messages")
    if not isinstance(messages, list) or not messages:
        return None
    sys_text = ""
    for m in messages[:2]:
        if isinstance(m, dict) and m.get("role") == "system":
            c = m.get("content")
            if isinstance(c, str):
                sys_text += c
    if not any(mark in sys_text for mark in SUBAGENT_SYSTEM_MARKS):
        return None
    logging.warning("[subagent_passthrough] 命中子代理请求：关闭心跳与竞速，纯透传")
    return {"action": "modify", "hedge": False, "heartbeat": False}


def register_defaults(interceptor):
    """注册默认开启的内置 hook；加载说明由 Interceptor.add_hook 动态打印。

    主文件启动时调用这一行即可，本函数不含任何硬编码提示文案。
    未开启的 fix_empty_user_parts 可由 RPC 注册::

        interceptor.add_hook(interceptor.builtin_hooks.fix_empty_user_parts)
    """
    interceptor.builtin_hooks = sys.modules[__name__]
    # 压缩摘要修复先注册：modify 可链式叠加，其后 sanitize 仍会作用于改写体
    interceptor.add_hook(fix_compact_summary)
    interceptor.add_hook(subagent_passthrough)
    interceptor.add_hook(sanitize_chat_messages)

