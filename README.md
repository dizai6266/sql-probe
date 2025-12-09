# SQL-Probe

**基于 SQL 的轻量级数据质量探针与告警工具**

专为 Databricks/Spark 环境设计，通过一段 SQL 即可实现数据质量检查、告警级别判断、多渠道通知以及 ETL 流程阻断。让数据监控像写 SQL 一样简单。

## ✨ 核心功能

*   **SQL 驱动 (SQL-Driven)**: 无需编写复杂的 Python 逻辑，一段 SQL 搞定检查与告警规则。
*   **智能分级 (Smart Leveling)**: 根据 SQL 返回的 `is_warning` 和 `status` 自动推断 Info, Warning, Error, Critical 级别。
*   **流程控制 (Flow Control)**: 支持基于告警级别自动阻断 ETL 任务（如检测到 Error 自动抛出异常中断 Notebook）。
*   **多渠道路由 (Multi-Channel)**: 内置多 Webhook 支持，轻松将不同类型的告警分发到不同的飞书群（如 DQ 群、ETL 运维群）。

---

## 🚀 快速开始

### 1. 部署
将 `sql-probe` 目录上传到 Databricks 的 `/Workspace/Shared/libs/` 目录下即可。

### 2. 最小化示例
```python
import sys
sys.path.append("/Workspace/Shared/libs")

from sql_probe import SQLProbeNotifier

# 初始化探针 (默认发送到 default 渠道)
probe = SQLProbeNotifier(spark)

# 执行 SQL 探针
# 逻辑：检查 my_table 中是否有 id 为 NULL 的记录，超过 100 条报红，否则报黄
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

SQL-Probe 依赖 SQL 返回的特定字段来决定行为。请确保你的 SQL 包含以下列：

| 字段名 | 必填 | 类型 | 说明 |
| :--- | :---: | :--- | :--- |
| **`is_warning`** | ✅ | `int` | **主开关**。<br>`0`: 正常 (Info)<br>`1`: 触发告警 (Warning/Error/Critical) |
| **`alert_info`** | ✅ | `string` | **告警内容**。用于在通知卡片中展示详细信息。 |
| **`status`** | ❌ | `string` | **级别控制** (仅当 `is_warning=1` 时生效)。<br>可选值: `AbnormalYellow`, `AbnormalRed`, `Critical`。<br>默认为 `AbnormalYellow`。 |
| `alert_name` | ❌ | `string` | 告警标题。默认为 "SQL Probe Alert"。 |

### 告警级别与行为矩阵

| is_warning | status | 最终级别 | 颜色 | 是否通知 | 是否中断 | 适用场景 |
| :---: | :--- | :--- | :---: | :---: | :---: | :--- |
| **0** | (忽略) | **INFO** | 🔵 | ❌ | ❌ | 日常巡检，正常不打扰 |
| **0** | (恢复) | **SUCCESS** | 🟢 | ✅ (条件触发) | ❌ | **故障恢复** (需开启 `notify_on_ok`) |
| **1** | `AbnormalYellow` | **WARNING** | 🟡 | ✅ | ❌ | 数据瑕疵，需关注但不阻断 |
| **1** | `AbnormalRed` | **ERROR** | 🟠 | ✅ | ✅ (可配置) | 严重质量问题，建议中断 |
| **1** | `Critical` | **CRITICAL** | 🔴 | ✅ | 🚨 **强制** | 致命错误，必须停止 |

---

## 🛠 配置指南

推荐使用 **Databricks Secrets** 管理 Webhook，也支持 **环境变量**。

### 配置 Webhook
只需配置一次，所有 Notebook 均可复用。

```bash
# 1. 创建 scope (如果已有可跳过)
databricks secrets create-scope --scope sql-probe

# 2. 配置不同渠道的 Webhook
databricks secrets put --scope sql-probe --key webhook-default   # 默认群
databricks secrets put --scope sql-probe --key webhook-dq        # 数据质量群
databricks secrets put --scope sql-probe --key webhook-etl       # ETL 运维群
```

> 也可以通过 Cluster 环境变量配置：`FEISHU_WEBHOOK`, `FEISHU_WEBHOOK_DQ` 等。

---

## 💡 典型场景

### 场景 1: ETL 强依赖检查 (Blocking Check)
在数据写入下游之前进行检查，如果质量不达标，直接中断任务，防止脏数据扩散。

```python
# 初始化时启用中断 (默认 interrupt_on_error=True)
probe = SQLProbeNotifier(spark, source="ODS Layer")

# 执行检查
# 如果结果是 AbnormalRed 或 Critical，这里会抛出 ProbeInterruptError，停止 Notebook
probe.execute(check_sql)

# 检查通过才会执行到这里
spark.sql("INSERT INTO downstream_table SELECT * FROM temp_table")
```

### 场景 2: 发送到特定群组 (Routing)
将不同类型的告警分发给不同的团队。

```python
# 发送到数据质量治理群
dq_probe = SQLProbeNotifier(spark, channel="dq")
dq_probe.execute(dq_sql)

# 发送到运维监控群
etl_probe = SQLProbeNotifier(spark, channel="etl")
etl_probe.execute(etl_sql)
```

### 场景 3: 批量执行 (Batch Execution)
一次性执行多个检查，最后汇总报告（通常用于非阻断性的全面体检）。

```python
checks = [
    {"sql": sql_check_null, "name": "非空检查"},
    {"sql": sql_check_dup, "name": "主键重复检查"},
    {"sql": sql_check_range, "name": "值域检查"}
]

# interrupt_on_error=False 确保跑完所有检查再汇总
results = probe.execute_batch(checks, interrupt_on_error=False)
```

---

## 📚 API 参考

### `SQLProbeNotifier`
```python
SQLProbeNotifier(
    spark,                      # SparkSession 对象
    webhook=None,               # (可选) 显式指定 Webhook URL，优先级最高
    channel="default",          # (可选) 指定通知渠道，自动查找对应的 Secret/Env
    source=None,                # (可选) 告警来源标识，如 "ODS Task"
    interrupt_on_error=True,    # (可选) 遇到 ERROR 级别是否中断
    debug=False                 # (可选) 调试模式，不发送真实请求
)
```

### `execute()`
```python
probe.execute(
    sql_text,                   # 监控 SQL 语句
    alert_name=None,            # 覆盖 SQL 中的 alert_name
    interrupt_on_error=None,    # 覆盖实例级别的中断设置
    silent=False,               # 静默模式 (不发通知，仅返回结果)
    notify_on_ok=False,         # 恢复通知 (上次失败这次成功时通知)
    empty_result_as="ok"        # 当 SQL 没查到数据时视为 "ok", "warning" 还是 "error"
) -> ProbeResult
```

## 📂 项目结构
```text
sql-probe/
├── core/                # 核心逻辑 (执行器、解析器、聚合器)
├── models/              # 数据模型 (AlertLevel, ProbeResult)
├── feishu-notify/       # 飞书通知模块 (独立子模块)
├── notifier.py          # 用户入口类
└── ...
```
