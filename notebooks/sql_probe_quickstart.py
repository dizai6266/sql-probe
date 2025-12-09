# Databricks notebook source
# MAGIC %md
# MAGIC # SQL-Probe 快速入门
# MAGIC 
# MAGIC 复制此模板快速开始使用 SQL-Probe

# COMMAND ----------

# Step 1: 配置 Webhook 并导入模块
import sys
import os

# 配置飞书 Webhook（测试用，生产环境建议使用 Databricks Secrets）
os.environ["FEISHU_WEBHOOK"] = "https://open.feishu.cn/open-apis/bot/v2/hook/6d8b23ff-fe40-473f-a9c7-1db6398eda61"

sys.path.insert(0, "/Workspace/Users/dizai@joycastle.mobi/sql-probe")
from feishu_notify import Notifier as FeishuNotifier  # 先导入确保路径正确
from sql_probe import SQLProbeNotifier, ProbeInterruptError

# COMMAND ----------

# Step 2: 初始化探针
probe = SQLProbeNotifier(
    spark,
    source="My ETL Task",    # 自定义来源标识
    interrupt_on_error=True  # 遇到 ERROR 级别中断 ETL
)

# COMMAND ----------

# Step 3: 执行检查
# 将下面的 SQL 替换为你的检查逻辑

try:
    result = probe.execute('''
        SELECT
            '数据完整性检查' as alert_name,
            CASE WHEN error_count > 0 THEN 1 ELSE 0 END as is_warning,
            concat('发现 ', error_count, ' 条异常数据') as alert_info,
            CASE 
                WHEN error_count > 100 THEN 'AbnormalRed'
                WHEN error_count > 0 THEN 'AbnormalYellow'
                ELSE 'Normal' 
            END as status
        FROM (
            -- 替换为你的检查逻辑
            SELECT count(*) as error_count 
            FROM your_table 
            WHERE some_column IS NULL
        )
    ''')
    
    print(f"✅ 检查通过: {result.alert_name}")
    print(f"   级别: {result.level.name}")
    
except ProbeInterruptError as e:
    print(f"❌ 检查失败，ETL 中断: {e}")
    raise  # 重新抛出异常以停止后续执行

# COMMAND ----------

# Step 4: 继续 ETL 逻辑（只有检查通过才会执行到这里）
print("📤 开始写入下游表...")
# spark.sql("INSERT INTO downstream_table SELECT * FROM temp_view")
print("✅ ETL 完成")
