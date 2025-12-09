"""
SQL-Probe: 基于 SQL 的告警探针

提供 SQL 执行、自动告警级别推断、飞书通知和流程中断控制
为 Databricks/Spark 环境提供数据质量监控

多渠道配置（支持发送到不同飞书群）:
    | channel 参数 | Secrets Key      | 环境变量               |
    |-------------|------------------|------------------------|
    | "default"   | webhook-default  | FEISHU_WEBHOOK         |
    | "dq"        | webhook-dq       | FEISHU_WEBHOOK_DQ      |
    | "etl"       | webhook-etl      | FEISHU_WEBHOOK_ETL     |
    | "alert"     | webhook-alert    | FEISHU_WEBHOOK_ALERT   |
    
    自定义: channel="xxx" → Secrets: webhook-xxx / 环境变量: FEISHU_WEBHOOK_XXX (自动大写)

Usage:
    from sql_probe import SQLProbeNotifier
    
    # 发到默认群
    probe = SQLProbeNotifier(spark)
    
    # 发到数据质量群
    probe = SQLProbeNotifier(spark, channel="dq")
    
    # 发到 ETL 运维群
    probe = SQLProbeNotifier(spark, channel="etl")
    
    result = probe.execute('''
        SELECT
            'NULL值检查' as alert_name,
            CASE WHEN cnt > 0 THEN 1 ELSE 0 END as is_warning,
            concat('发现 ', cnt, ' 条异常') as alert_info,
            CASE WHEN cnt > 100 THEN 'AbnormalRed' ELSE 'AbnormalYellow' END as status
        FROM (SELECT count(*) as cnt FROM table WHERE id IS NULL)
    ''')
"""

from .models.level import AlertLevel
from .models.result import ProbeResult, RowDetail
from .models.exceptions import (
    ProbeError,
    ProbeInterruptError,
    SQLExecutionError,
    SQLValidationError,
)
from .notifier import SQLProbeNotifier
from .core.template import TemplateEngine
from .core.aggregation import AggregationCondition, MultiCondition
from .core.history import AlertHistory

__version__ = "0.1.0"

__all__ = [
    # 主类
    "SQLProbeNotifier",
    
    # 数据模型
    "AlertLevel",
    "ProbeResult",
    "RowDetail",
    
    # 异常
    "ProbeError",
    "ProbeInterruptError",
    "SQLExecutionError",
    "SQLValidationError",
    
    # 高级功能
    "TemplateEngine",
    "AggregationCondition",
    "MultiCondition",
    "AlertHistory",
]

