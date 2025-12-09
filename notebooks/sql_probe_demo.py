# Databricks notebook source
# MAGIC %md
# MAGIC # SQL-Probe 功能演示 Notebook
# MAGIC 
# MAGIC 本 Notebook 演示如何调用部署在 `/Workspace/Users/dizai@joycastle.mobi/sql-probe` 的所有功能。
# MAGIC 
# MAGIC ## 功能列表
# MAGIC 1. 基础配置与初始化
# MAGIC 2. 单个 SQL 检查执行
# MAGIC 3. 批量检查执行
# MAGIC 4. 多渠道发送（路由到不同飞书群）
# MAGIC 5. 恢复通知功能
# MAGIC 6. 空结果处理
# MAGIC 7. 中断控制（阻断 ETL）
# MAGIC 8. 高级功能（聚合条件）
# MAGIC 9. SQL 验证（Dry Run）

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 基础配置与初始化
# MAGIC 
# MAGIC 首先设置 Webhook 环境变量，然后将 sql-probe 添加到 Python 路径。

# COMMAND ----------

import sys
import os

# ============================================================
# 📌 配置飞书 Webhook（测试用，生产环境建议使用 Databricks Secrets）
# ============================================================
FEISHU_WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/6d8b23ff-fe40-473f-a9c7-1db6398eda61"

# 设置默认渠道的 Webhook
# 命名规则: channel="xxx" → 环境变量 FEISHU_WEBHOOK_XXX（default 渠道特殊，为 FEISHU_WEBHOOK）
os.environ["FEISHU_WEBHOOK"] = FEISHU_WEBHOOK_URL

print(f"✅ 飞书 Webhook 已配置")

# ============================================================
# 📌 添加 sql-probe 到 Python 路径
# ============================================================
SQL_PROBE_PATH = "/Workspace/Users/dizai@joycastle.mobi/sql-probe"
if SQL_PROBE_PATH not in sys.path:
    sys.path.insert(0, SQL_PROBE_PATH)

# 先导入 feishu_notify（确保路径正确）
from feishu_notify import Notifier as FeishuNotifier
print("✅ feishu_notify 模块导入成功！")

# 导入核心模块
from sql_probe import (
    SQLProbeNotifier,
    ProbeResult,
    AlertLevel,
    ProbeInterruptError,
    AggregationCondition,
    MultiCondition,
)

print(f"✅ sql-probe 模块导入成功！")
print(f"📍 模块路径: {SQL_PROBE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 查看帮助信息

# COMMAND ----------

# 查看 Webhook 配置帮助
SQLProbeNotifier.help()

# COMMAND ----------

# 查看 SQL 规范帮助
SQLProbeNotifier.help_sql()

# COMMAND ----------

# 查看高级功能帮助
SQLProbeNotifier.help_features()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 单个 SQL 检查执行
# MAGIC 
# MAGIC 最基础的用法：执行一条符合规范的 SQL，自动推断告警级别并发送通知。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 创建测试数据

# COMMAND ----------

# 创建测试表
spark.sql("""
CREATE OR REPLACE TEMP VIEW test_orders AS
SELECT * FROM VALUES
    (1, 'order_001', 100.0, '2024-01-01', 'completed'),
    (2, 'order_002', NULL, '2024-01-02', 'completed'),
    (3, 'order_003', 50.0, '2024-01-03', 'pending'),
    (4, NULL, 200.0, '2024-01-04', 'completed'),
    (5, 'order_005', -10.0, '2024-01-05', 'completed')
AS orders(id, order_no, amount, order_date, status)
""")

spark.sql("SELECT * FROM test_orders").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 基础检查

# COMMAND ----------

# 初始化探针
probe = SQLProbeNotifier(
    spark,
    source="Demo Notebook",  # 来源标识
    debug=True,              # 开启调试日志
    interrupt_on_error=False # 暂时关闭中断，方便测试
)

print(f"探针已初始化: {probe}")

# COMMAND ----------

# 执行检查：检查空值
null_check_sql = """
SELECT
    '订单空值检查' as alert_name,
    CASE WHEN cnt > 0 THEN 1 ELSE 0 END as is_warning,
    concat('发现 ', cnt, ' 条订单号为空的记录') as alert_info,
    CASE 
        WHEN cnt > 10 THEN 'AbnormalRed' 
        WHEN cnt > 0 THEN 'AbnormalYellow'
        ELSE 'Normal' 
    END as status
FROM (
    SELECT count(*) as cnt 
    FROM test_orders 
    WHERE order_no IS NULL
)
"""

result = probe.execute(null_check_sql)

# 查看结果
print("📊 检查结果:")
print(result.summary())
print(f"\n详细信息: {result.to_dict()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 检查金额异常（负数值）

# COMMAND ----------

# 检查金额异常
amount_check_sql = """
SELECT
    '金额异常检查' as alert_name,
    CASE WHEN negative_cnt > 0 OR null_cnt > 0 THEN 1 ELSE 0 END as is_warning,
    concat('发现 ', negative_cnt, ' 条负数金额, ', null_cnt, ' 条空金额') as alert_info,
    CASE 
        WHEN negative_cnt > 0 THEN 'AbnormalRed'
        WHEN null_cnt > 0 THEN 'AbnormalYellow'
        ELSE 'Normal' 
    END as status
FROM (
    SELECT 
        count(CASE WHEN amount < 0 THEN 1 END) as negative_cnt,
        count(CASE WHEN amount IS NULL THEN 1 END) as null_cnt
    FROM test_orders
)
"""

result = probe.execute(amount_check_sql)
print("📊 金额检查结果:")
print(result.summary())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 批量检查执行
# MAGIC 
# MAGIC 一次性执行多个检查，最后汇总报告。

# COMMAND ----------

# 定义多个检查任务
checks = [
    {
        "name": "主键非空检查",
        "sql": """
            SELECT
                '主键非空检查' as alert_name,
                CASE WHEN cnt > 0 THEN 1 ELSE 0 END as is_warning,
                concat('发现 ', cnt, ' 条 ID 为空') as alert_info,
                CASE WHEN cnt > 0 THEN 'AbnormalRed' ELSE 'Normal' END as status
            FROM (SELECT count(*) as cnt FROM test_orders WHERE id IS NULL)
        """
    },
    {
        "name": "订单号唯一性检查",
        "sql": """
            SELECT
                '订单号唯一性检查' as alert_name,
                CASE WHEN dup_cnt > 0 THEN 1 ELSE 0 END as is_warning,
                concat('发现 ', dup_cnt, ' 个重复订单号') as alert_info,
                CASE WHEN dup_cnt > 0 THEN 'AbnormalYellow' ELSE 'Normal' END as status
            FROM (
                SELECT count(*) as dup_cnt 
                FROM (
                    SELECT order_no, count(*) as c 
                    FROM test_orders 
                    WHERE order_no IS NOT NULL
                    GROUP BY order_no 
                    HAVING count(*) > 1
                )
            )
        """
    },
    {
        "name": "金额范围检查",
        "sql": """
            SELECT
                '金额范围检查' as alert_name,
                CASE WHEN cnt > 0 THEN 1 ELSE 0 END as is_warning,
                concat('发现 ', cnt, ' 条金额异常') as alert_info,
                CASE WHEN cnt > 0 THEN 'AbnormalYellow' ELSE 'Normal' END as status
            FROM (
                SELECT count(*) as cnt 
                FROM test_orders 
                WHERE amount < 0 OR amount > 100000
            )
        """
    }
]

# 批量执行
batch_result = probe.execute_batch(checks)

print("📊 批量检查结果:")
print(batch_result.summary())
print(f"\n总检查项数: {len(checks)}")
print(f"触发告警数: {len(batch_result.warning_rows)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 多渠道发送（路由到不同飞书群）
# MAGIC 
# MAGIC 通过 `channel` 参数将告警发送到不同的飞书群。

# COMMAND ----------

# 注意：以下代码需要先配置好 Webhook
# 可以通过 Databricks Secrets 或环境变量配置

# 演示不同渠道的初始化方式
print("""
📢 多渠道配置示例:

命名规则:
  channel="xxx" → Secrets: webhook-xxx / 环境变量: FEISHU_WEBHOOK_XXX
  channel="default" (默认) → Secrets: webhook-default / 环境变量: FEISHU_WEBHOOK

# 使用默认渠道
probe = SQLProbeNotifier(spark)

# 使用自定义渠道（需先配置对应的 Secrets 或环境变量）
probe = SQLProbeNotifier(spark, channel="your_channel")

# 显式指定 Webhook（不走配置）
probe = SQLProbeNotifier(spark, webhook="https://open.feishu.cn/open-apis/bot/v2/hook/xxx")
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 恢复通知功能
# MAGIC 
# MAGIC 当告警从异常恢复到正常时，发送"已恢复"通知。

# COMMAND ----------

# 模拟告警恢复场景

# 第一次检查：触发告警
result1 = probe.execute("""
    SELECT
        '恢复测试' as alert_name,
        1 as is_warning,
        '发现异常数据' as alert_info,
        'AbnormalYellow' as status
""")
print(f"第一次检查: 级别={result1.level.name}, 触发={result1.triggered}")

# 第二次检查：恢复正常，启用 notify_on_ok
result2 = probe.execute("""
    SELECT
        '恢复测试' as alert_name,
        0 as is_warning,
        '数据正常' as alert_info,
        'Normal' as status
""", notify_on_ok=True)
print(f"第二次检查: 级别={result2.level.name}, 触发={result2.triggered}")
print("📧 已发送恢复通知")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 空结果处理
# MAGIC 
# MAGIC SQL 返回空结果时的处理策略。

# COMMAND ----------

# 创建一个必定返回空结果的查询
empty_sql = """
SELECT
    '空结果测试' as alert_name,
    0 as is_warning,
    '无数据' as alert_info,
    'Normal' as status
WHERE 1=0  -- 永远返回空
"""

# 空结果视为正常（默认）
result_ok = probe.execute(empty_sql, empty_result_as="ok")
print(f"empty_result_as='ok':      级别={result_ok.level.name}, 内容={result_ok.content}")

# 空结果视为警告
result_warning = probe.execute(empty_sql, empty_result_as="warning")
print(f"empty_result_as='warning': 级别={result_warning.level.name}, 内容={result_warning.content}")

# 空结果视为错误
result_error = probe.execute(empty_sql, empty_result_as="error")
print(f"empty_result_as='error':   级别={result_error.level.name}, 内容={result_error.content}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. 中断控制（阻断 ETL）
# MAGIC 
# MAGIC 根据告警级别自动中断 ETL 流程，防止脏数据写入下游。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 ERROR 级别中断（可配置）

# COMMAND ----------

# 创建启用中断的探针
probe_with_interrupt = SQLProbeNotifier(
    spark,
    source="ETL Task",
    interrupt_on_error=True,  # 遇到 ERROR 级别中断
    debug=True
)

# 测试 ERROR 级别的中断行为
try:
    result = probe_with_interrupt.execute("""
        SELECT
            'ERROR级别测试' as alert_name,
            1 as is_warning,
            '严重问题' as alert_info,
            'AbnormalRed' as status  -- ERROR 级别
    """)
    print("✅ 检查通过，继续执行")
except ProbeInterruptError as e:
    print(f"🛑 ETL 被中断: {e}")
    print(f"   触发告警: {e.result.alert_name}")
    print(f"   告警级别: {e.result.level.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 CRITICAL 级别强制中断

# COMMAND ----------

# CRITICAL 级别无论配置如何，都会强制中断
try:
    result = probe.execute("""
        SELECT
            'CRITICAL级别测试' as alert_name,
            1 as is_warning,
            '致命错误' as alert_info,
            'Critical' as status  -- CRITICAL 级别
    """)
    print("✅ 检查通过")
except ProbeInterruptError as e:
    print(f"🚨 强制中断: {e}")
    print(f"   这是 CRITICAL 级别，无法跳过")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 ETL 最佳实践

# COMMAND ----------

# 典型的 ETL 流程中使用 SQL-Probe
def etl_with_probe():
    """带有质量检查的 ETL 流程示例"""
    
    # 初始化探针
    probe = SQLProbeNotifier(
        spark,
        source="ODS Layer ETL",
        interrupt_on_error=True,
        debug=True  # 生产环境设为 False
    )
    
    # Step 1: 数据加载
    print("📥 Step 1: 加载数据...")
    # spark.sql("CREATE OR REPLACE TEMP VIEW raw_data AS SELECT ...")
    
    # Step 2: 质量检查（在写入前）
    print("🔍 Step 2: 数据质量检查...")
    try:
        probe.execute("""
            SELECT
                '写入前检查' as alert_name,
                0 as is_warning,
                '数据正常' as alert_info,
                'Normal' as status
        """)
        print("✅ 质量检查通过")
    except ProbeInterruptError:
        print("❌ 质量检查失败，ETL 中断")
        raise
    
    # Step 3: 写入下游（只有检查通过才会执行）
    print("📤 Step 3: 写入下游表...")
    # spark.sql("INSERT INTO downstream_table SELECT * FROM temp_table")
    print("✅ ETL 完成")

# 执行示例
try:
    etl_with_probe()
except ProbeInterruptError as e:
    print(f"ETL 流程被中断: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. 高级功能

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1 聚合条件检查

# COMMAND ----------

# 使用聚合条件进行检查
# 这是一种更简洁的方式，不需要在 SQL 中写 is_warning/status 列

# 定义条件：总金额超过 100 时告警
condition = AggregationCondition.sum("amount") > 100

# 执行检查
result = probe.execute(
    sql_text="SELECT * FROM test_orders WHERE amount IS NOT NULL",
    alert_name="金额聚合检查",
    condition=condition
)

print(f"📊 聚合检查结果:")
print(f"   告警名称: {result.alert_name}")
print(f"   是否触发: {result.triggered}")
print(f"   内容: {result.content}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 组合条件检查

# COMMAND ----------

# 多条件组合
conditions = MultiCondition([
    AggregationCondition.count() > 3,           # 记录数超过 3
    AggregationCondition.sum("amount") > 200,   # 总金额超过 200
])

result = probe.execute(
    sql_text="SELECT * FROM test_orders WHERE status = 'completed'",
    alert_name="组合条件检查",
    condition=conditions
)

print(f"📊 组合条件检查结果:")
print(f"   是否触发: {result.triggered}")
print(f"   内容: {result.content}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. SQL 验证（Dry Run）
# MAGIC 
# MAGIC 在实际执行前验证 SQL 是否符合规范。

# COMMAND ----------

# 验证合法的 SQL
valid_sql = """
SELECT
    '测试' as alert_name,
    0 as is_warning,
    '正常' as alert_info,
    'Normal' as status
"""

validation = probe.validate(valid_sql)
print(f"✅ 验证结果: valid={validation['valid']}")
print(f"   检测到的列: {validation.get('columns', [])}")

# COMMAND ----------

# 验证不合法的 SQL（缺少必要列）
invalid_sql = """
SELECT
    '测试' as alert_name,
    '正常' as alert_info
    -- 缺少 is_warning 和 status 列
"""

validation = probe.validate(invalid_sql)
print(f"❌ 验证结果: valid={validation['valid']}")
if validation.get('error'):
    print(f"   错误信息: {validation['error']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. 完整示例：数据质量检查流程

# COMMAND ----------

def run_dq_checks(table_name: str, probe: SQLProbeNotifier):
    """
    运行完整的数据质量检查流程
    
    Args:
        table_name: 要检查的表名
        probe: SQLProbeNotifier 实例
    """
    print(f"🔍 开始检查表: {table_name}")
    print("=" * 50)
    
    # 定义检查项
    checks = [
        {
            "name": f"{table_name} - 空值检查",
            "sql": f"""
                SELECT
                    '{table_name} 空值检查' as alert_name,
                    CASE WHEN null_cnt > 0 THEN 1 ELSE 0 END as is_warning,
                    concat('发现 ', null_cnt, ' 列存在空值') as alert_info,
                    CASE WHEN null_cnt > 3 THEN 'AbnormalRed'
                         WHEN null_cnt > 0 THEN 'AbnormalYellow'
                         ELSE 'Normal' END as status
                FROM (
                    SELECT 
                        sum(CASE WHEN order_no IS NULL THEN 1 ELSE 0 END) +
                        sum(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) as null_cnt
                    FROM {table_name}
                )
            """
        },
        {
            "name": f"{table_name} - 数据量检查",
            "sql": f"""
                SELECT
                    '{table_name} 数据量检查' as alert_name,
                    CASE WHEN cnt < 1 THEN 1 ELSE 0 END as is_warning,
                    concat('当前数据量: ', cnt, ' 条') as alert_info,
                    CASE WHEN cnt < 1 THEN 'AbnormalRed' ELSE 'Normal' END as status
                FROM (SELECT count(*) as cnt FROM {table_name})
            """
        },
        {
            "name": f"{table_name} - 业务规则检查",
            "sql": f"""
                SELECT
                    '{table_name} 业务规则检查' as alert_name,
                    CASE WHEN invalid_cnt > 0 THEN 1 ELSE 0 END as is_warning,
                    concat('发现 ', invalid_cnt, ' 条不符合业务规则') as alert_info,
                    CASE WHEN invalid_cnt > 5 THEN 'AbnormalRed'
                         WHEN invalid_cnt > 0 THEN 'AbnormalYellow'
                         ELSE 'Normal' END as status
                FROM (
                    SELECT count(*) as invalid_cnt 
                    FROM {table_name}
                    WHERE amount < 0 OR status NOT IN ('completed', 'pending', 'cancelled')
                )
            """
        }
    ]
    
    # 批量执行
    result = probe.execute_batch(checks)
    
    # 输出结果
    print("\n📊 检查结果汇总:")
    print(result.summary())
    
    # 输出各项检查详情
    if result.details:
        print("\n📋 各项检查详情:")
        for detail in result.details:
            emoji = "✅" if not detail.is_warning else ("🟡" if detail.level == AlertLevel.WARNING else "🔴")
            print(f"  {emoji} {detail.alert_name}: {detail.alert_info}")
    
    print("\n" + "=" * 50)
    return result

# 执行检查
result = run_dq_checks("test_orders", probe)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. 清理测试数据

# COMMAND ----------

# 清理测试表
spark.sql("DROP VIEW IF EXISTS test_orders")
print("✅ 测试数据已清理")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 快速参考卡片
# MAGIC 
# MAGIC ### 初始化
# MAGIC ```python
# MAGIC from sql_probe import SQLProbeNotifier, ProbeInterruptError
# MAGIC 
# MAGIC # 基础用法
# MAGIC probe = SQLProbeNotifier(spark)
# MAGIC 
# MAGIC # 完整参数
# MAGIC probe = SQLProbeNotifier(
# MAGIC     spark,
# MAGIC     channel="your_channel", # 自定义渠道（按需配置）
# MAGIC     source="My ETL",        # 来源标识
# MAGIC     interrupt_on_error=True,# 遇到 ERROR 中断
# MAGIC     debug=False             # 生产模式
# MAGIC )
# MAGIC ```
# MAGIC 
# MAGIC ### SQL 规范
# MAGIC ```sql
# MAGIC SELECT
# MAGIC     '告警名称' as alert_name,
# MAGIC     CASE WHEN 条件 THEN 1 ELSE 0 END as is_warning,
# MAGIC     '告警详情' as alert_info,
# MAGIC     CASE WHEN 严重 THEN 'AbnormalRed' 
# MAGIC          WHEN 警告 THEN 'AbnormalYellow'
# MAGIC          ELSE 'Normal' END as status
# MAGIC ```
# MAGIC 
# MAGIC ### 级别对照
# MAGIC | status | 级别 | 是否通知 | 是否中断 |
# MAGIC |--------|------|---------|---------|
# MAGIC | Normal | INFO | ❌ | ❌ |
# MAGIC | AbnormalYellow | WARNING | ✅ | ❌ |
# MAGIC | AbnormalRed | ERROR | ✅ | ✅ (可配置) |
# MAGIC | Critical | CRITICAL | ✅ | 🚨 强制 |
