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


def register_defaults(interceptor):
    """注册默认开启的内置 hook；加载说明由 Interceptor.add_hook 动态打印。

    主文件启动时调用这一行即可，本函数不含任何硬编码提示文案。
    未开启的 fix_empty_user_parts 可由 RPC 注册::

        interceptor.add_hook(interceptor.builtin_hooks.fix_empty_user_parts)
    """
    interceptor.builtin_hooks = sys.modules[__name__]
    interceptor.add_hook(sanitize_chat_messages)

