"""
SQL-Probe: 基于 SQL 的告警探针

提供 SQL 执行、自动告警级别推断、飞书通知和流程中断控制
为 Databricks/Spark 环境提供数据质量监控

多渠道配置（支持发送到不同飞书群）:
    命名规则:
    - channel="xxx" → Secrets Key: webhook-xxx / 环境变量: FEISHU_WEBHOOK_XXX
    - channel="default" 是特殊情况，环境变量为 FEISHU_WEBHOOK（无后缀）

Usage:
    from sql_probe import SQLProbeNotifier
    
    # 使用默认渠道
    probe = SQLProbeNotifier(spark)
    
    # 使用自定义渠道
    probe = SQLProbeNotifier(spark, channel="your_channel")
    
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
]

