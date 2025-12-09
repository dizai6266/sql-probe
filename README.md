# SQL-Probe

**基于 SQL 的轻量级数据质量探针与告警工具**

专为 Databricks/Spark 环境设计，通过一段 SQL 即可实现数据质量检查、告警级别判断、多渠道通知以及 ETL 流程阻断。让数据监控像写 SQL 一样简单。

> 📘 **完整使用示例**：请参考 [`notebooks/sql_probe_demo.py`](./notebooks/sql_probe_demo.py)，包含所有功能的交互式演示。

---

## ✨ 核心功能

| 功能 | 说明 |
|------|------|
| **SQL 驱动** | 无需编写复杂的 Python 逻辑，一段 SQL 搞定检查与告警规则 |
| **智能分级** | 根据 SQL 返回的 `is_warning` 和 `status` 自动推断 INFO/WARNING/ERROR/CRITICAL 级别 |
| **流程控制** | 支持基于告警级别自动阻断 ETL 任务，防止脏数据扩散 |
| **多渠道路由** | 内置多 Webhook 支持，轻松将不同告警分发到不同飞书群 |
| **恢复通知** | 当告警从异常恢复到正常时，自动发送"已恢复"通知 |
| **聚合条件** | 支持 Python 风格的条件定义，无需在 SQL 中写告警逻辑 |
| **SQL 验证** | 执行前验证 SQL 格式是否符合规范（Dry Run） |
| **批量执行** | 一次性执行多个检查，汇总报告 |

---

## 🚀 快速开始

### 1. 部署
将 `sql-probe` 目录上传到 Databricks Workspace：
```bash
# 推荐路径
/Workspace/Shared/libs/sql-probe
```

### 2. 最小化示例
```python
import sys
sys.path.append("/Workspace/Shared/libs")

from sql_probe import SQLProbeNotifier

# 初始化探针
probe = SQLProbeNotifier(spark)

# 执行检查：空值超过 100 条报红，否则报黄
probe.execute('''
    SELECT
        '空值检查' as alert_name,
        CASE WHEN cnt > 0 THEN 1 ELSE 0 END as is_warning,
        concat('发现 ', cnt, ' 条异常数据') as alert_info,
        CASE WHEN cnt > 100 THEN 'AbnormalRed' ELSE 'AbnormalYellow' END as status
    FROM (SELECT count(*) as cnt FROM my_table WHERE id IS NULL)
''')
```

---

## 📜 SQL 契约 (SQL Contract)

SQL-Probe 依赖 SQL 返回的特定字段来决定行为：

| 字段名 | 必填 | 类型 | 说明 |
| :--- | :---: | :--- | :--- |
| **`is_warning`** | ✅ | `int` | **主开关**。`0`: 正常，`1`: 触发告警 |
| **`alert_info`** | ✅ | `string` | **告警内容**。展示在通知卡片中 |
| **`status`** | ❌ | `string` | **级别控制**（仅 `is_warning=1` 时生效）。可选: `AbnormalYellow`, `AbnormalRed`, `Critical` |
| `alert_name` | ❌ | `string` | 告警标题，默认 "SQL Probe Alert" |

### 告警级别与行为矩阵

| is_warning | status | 最终级别 | 是否通知 | 是否中断 | 适用场景 |
| :---: | :--- | :--- | :---: | :---: | :--- |
| **0** | (忽略) | **INFO** 🔵 | ❌ | ❌ | 日常巡检，正常不打扰 |
| **0** | (恢复) | **SUCCESS** 🟢 | ✅ (需开启) | ❌ | 故障恢复通知 |
| **1** | `AbnormalYellow` | **WARNING** 🟡 | ✅ | ❌ | 数据瑕疵，需关注但不阻断 |
| **1** | `AbnormalRed` | **ERROR** 🟠 | ✅ | ✅ (可配置) | 严重问题，建议中断 |
| **1** | `Critical` | **CRITICAL** 🔴 | ✅ | 🚨 **强制** | 致命错误，必须停止 |

---

## 🛠 配置指南

### Webhook 配置
推荐使用 **Databricks Secrets**，也支持环境变量：

```bash
# 1. 创建 scope（如已有可跳过）
databricks secrets create-scope --scope sql-probe

# 2. 配置 Webhook（channel 名称可自定义）
databricks secrets put --scope sql-probe --key webhook-default      # 默认渠道
databricks secrets put --scope sql-probe --key webhook-your_channel # 自定义渠道
```

**命名规则**：
- `channel="xxx"` → Secrets: `webhook-xxx` / 环境变量: `FEISHU_WEBHOOK_XXX`
- `channel="default"` → Secrets: `webhook-default` / 环境变量: `FEISHU_WEBHOOK`

---

## 💡 典型场景

### 场景 1: ETL 强依赖检查
在写入下游前检查，质量不达标则中断任务：

```python
from sql_probe import SQLProbeNotifier, ProbeInterruptError

probe = SQLProbeNotifier(spark, source="ODS Layer", interrupt_on_error=True)

try:
    probe.execute(check_sql)
    # 检查通过才会执行到这里
    spark.sql("INSERT INTO downstream_table SELECT * FROM temp_table")
except ProbeInterruptError as e:
    print(f"ETL 被中断: {e.result.alert_name}")
    raise
```

### 场景 2: 发送到特定群组
将不同告警分发给不同团队：

```python
# 发送到自定义渠道
probe = SQLProbeNotifier(spark, channel="your_channel")
probe.execute(check_sql)
```

### 场景 3: 批量执行
一次性执行多个检查，汇总报告：

```python
checks = [
    {"sql": sql_check_null, "name": "非空检查"},
    {"sql": sql_check_dup, "name": "主键重复检查"},
    {"sql": sql_check_range, "name": "值域检查"}
]

# 跑完所有检查再汇总
results = probe.execute_batch(checks, interrupt_on_error=False)
print(results.summary())
```

### 场景 4: 恢复通知
当告警恢复正常时发送通知：

```python
# 开启 notify_on_ok，当从告警恢复到正常时会发送"已恢复"通知
result = probe.execute(check_sql, notify_on_ok=True)
```

### 场景 5: 聚合条件检查
无需在 SQL 中写告警逻辑，使用 Python 定义条件：

```python
from sql_probe import AggregationCondition, MultiCondition

# 单条件：总金额超过 10000 时告警
condition = AggregationCondition.sum("amount") > 10000
result = probe.execute(
    "SELECT * FROM orders",
    alert_name="金额检查",
    condition=condition
)

# 多条件组合
conditions = MultiCondition([
    AggregationCondition.count() > 100,
    AggregationCondition.avg("amount") > 500,
])
```

### 场景 6: SQL 验证（Dry Run）
执行前验证 SQL 是否符合规范：

```python
validation = probe.validate(sql_text)
if validation['valid']:
    print(f"SQL 合法，包含列: {validation['columns']}")
else:
    print(f"SQL 不合法: {validation['error']}")
```

---

## 📚 API 参考

### `SQLProbeNotifier`
```python
SQLProbeNotifier(
    spark,                      # SparkSession 对象
    webhook=None,               # (可选) 显式指定 Webhook URL，优先级最高
    channel="default",          # (可选) 指定通知渠道
    source=None,                # (可选) 告警来源标识，如 "ODS Task"
    interrupt_on_error=True,    # (可选) 遇到 ERROR 级别是否中断
    debug=False                 # (可选) 调试模式，打印详细日志
)
```

### `execute()`
```python
probe.execute(
    sql_text,                   # SQL 语句（需符合 SQL 契约）
    alert_name=None,            # 覆盖 SQL 中的 alert_name
    interrupt_on_error=None,    # 覆盖实例级别的中断设置
    silent=False,               # 静默模式（不发通知，仅返回结果）
    notify_on_ok=False,         # 恢复通知（上次失败这次成功时通知）
    empty_result_as="ok",       # 空结果处理: "ok" / "warning" / "error"
    condition=None,             # 聚合条件（使用时无需 SQL 契约字段）
    template=None,              # 自定义通知模板
) -> ProbeResult
```

### `execute_batch()`
```python
probe.execute_batch(
    checks,                     # 检查列表 [{"sql": ..., "name": ...}, ...]
    interrupt_on_error=None,    # 是否在遇到 ERROR 时中断
) -> ProbeResult
```

### `validate()`
```python
probe.validate(sql_text) -> dict
# 返回: {"valid": bool, "columns": list, "error": str|None}
```

### 静态帮助方法
```python
SQLProbeNotifier.help()           # 查看 Webhook 配置帮助
SQLProbeNotifier.help_sql()       # 查看 SQL 规范帮助
SQLProbeNotifier.help_features()  # 查看高级功能帮助
```

---

## 📂 项目结构

```text
sql-probe/
├── sql_probe/
│   ├── __init__.py          # 包入口，导出主要类
│   ├── notifier.py          # SQLProbeNotifier 主类
│   ├── core/                # 核心组件
│   │   ├── executor.py      # SQL 执行器
│   │   ├── resolver.py      # 级别解析器
│   │   ├── aggregator.py    # 结果聚合器
│   │   ├── aggregation.py   # 聚合条件定义
│   │   └── template.py      # 模板引擎
│   └── models/              # 数据模型
│       ├── level.py         # AlertLevel 枚举
│       ├── result.py        # ProbeResult 结果类
│       └── exceptions.py    # 自定义异常
├── feishu_notify/           # 飞书通知模块（独立子模块）
├── notebooks/
│   └── sql_probe_demo.py    # 📘 完整功能演示
└── README.md
```

---

## 🔗 更多资源

- **完整演示**: [`notebooks/sql_probe_demo.py`](./notebooks/sql_probe_demo.py) - 包含所有功能的交互式 Notebook
- **变化率监控**: 推荐使用 [Databricks SQL Alerts](https://docs.databricks.com/sql/user/alerts/) 原生功能

---

## License

MIT
