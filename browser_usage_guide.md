# Trae Browser 工具使用心得

## 问题回顾

在调用 `run_mcp` 工具打开 Hugging Face 和百度时，出现了一些失败：

### 失败原因 1：参数顺序错误

**错误写法**（参数写在 code 字段之后）：
```javascript
// ❌ 错误：server_name 和 tool_name 漏掉了
{"code": "await tools.browser_navigate(...)", "server_name": "integrated_code_mode", "tool_name": "Exec"}
```

**正确写法**（`args` 是唯一的顶层字段）：
```javascript
// ✅ 正确：所有参数都放在 args 里
run_mcp(server_name="integrated_code_mode", tool_name="Exec", args={"code": "..."})
```

**关键规则**：`run_mcp` 的 `args` 必须是一个 **JSON 对象**，里面只有 `code` 字段。不能把 `server_name`、`tool_name` 放在 `args` 外面再嵌套。

---

### 失败原因 2：Hugging Face 网络不通

即使参数正确，浏览器也无法访问 `https://huggingface.co/`，报错：
```
ERR_CONNECTION_RESET (-101)
```

这说明**当前环境对 Hugging Face 有网络限制**（可能是防火墙、DNS 污染或公司网络策略）。这不是工具调用问题，而是网络环境问题。

---

## 正确调用模板

```javascript
// 1. 打开网页
await tools.browser_navigate({ url: "https://www.baidu.com" });
await tools.browser_wait_for({ time: 2 });
const snap = await tools.browser_snapshot();
text(snap);

// 2. 提取数据
const data = await tools.browser_evaluate({
  script: `JSON.stringify({ title: document.title })`
});
text(data);
```

**调用格式**：
```
run_mcp(
  server_name="integrated_code_mode",
  tool_name="Exec",
  args={"code": "<上面的 JavaScript 代码>"}
)
```

---

## 核心要点总结

| 要点 | 说明 |
|------|------|
| **args 必须是 JSON 对象** | 所有浏览器工具调用必须通过 `await tools.xxx()` 在 code 字段里执行 |
| **不要在外面传 server_name** | server_name 和 tool_name 是 run_mcp 的顶层参数，不是 args 的一部分 |
| **先 snapshot 再交互** | 每次 navigate 或点击后，用 snapshot 获取最新元素 ref |
| **网络限制注意** | 国内可能无法访问 Hugging Face、GitHub 等站点，需确认网络连通性 |
| **等待页面加载** | navigate 后用 `browser_wait_for({ time: 2 })` 等待页面渲染完成 |

---

## 完整流程示例

```javascript
// Step 1: 导航到目标页面
await tools.browser_navigate({ url: "https://目标网站" });
await tools.browser_wait_for({ time: 2 });
const snap = await tools.browser_snapshot();
text(snap);  // 查看页面结构

// Step 2: 根据 snapshot 中的 ref 进行交互
await tools.browser_click({ ref: "e11" });  // 点击按钮
await tools.browser_wait_for({ time: 1 });

// Step 3: 提取结果
const result = await tools.browser_evaluate({
  script: `JSON.stringify(document.querySelector('.result')?.textContent ?? '')`
});
text(result);
```
