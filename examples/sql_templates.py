"""
SQL-Probe SQL 模板示例

提供常用监控场景的 SQL 模板，可直接复制使用
"""

# =============================================================================
# 数据完整性检查模板
# =============================================================================

NULL_CHECK_TEMPLATE = '''
-- NULL 值检查模板
-- 检查关键字段是否存在 NULL 值

SELECT
    '{check_name}' as alert_name,
    CASE WHEN null_count > {threshold} THEN 1 ELSE 0 END as is_warning,
    concat('字段 {column_name} 存在 ', null_count, ' 条 NULL 记录') as alert_info,
    CASE 
        WHEN null_count > {critical_threshold} THEN 'Critical'
        WHEN null_count > {error_threshold} THEN 'AbnormalRed'
        WHEN null_count > {threshold} THEN 'AbnormalYellow'
        ELSE 'Normal'
    END as status
FROM (
    SELECT count(*) as null_count
    FROM {table_name}
    WHERE {column_name} IS NULL
      AND {date_column} = current_date() - 1
)
'''

DUPLICATE_CHECK_TEMPLATE = '''
-- 主键重复检查模板
-- 检查主键是否存在重复

SELECT
    '{check_name}' as alert_name,
    CASE WHEN dup_count > 0 THEN 1 ELSE 0 END as is_warning,
    concat('主键 {pk_column} 存在 ', dup_count, ' 条重复记录') as alert_info,
    CASE WHEN dup_count > 0 THEN 'AbnormalRed' ELSE 'Normal' END as status
FROM (
    SELECT count(*) - count(distinct {pk_column}) as dup_count
    FROM {table_name}
    WHERE {date_column} = current_date() - 1
)
'''

COMPLETENESS_CHECK_TEMPLATE = '''
-- 数据完整性检查模板
-- 检查数据记录数是否在预期范围内

SELECT
    '{check_name}' as alert_name,
    CASE WHEN record_count < {min_threshold} OR record_count > {max_threshold} THEN 1 ELSE 0 END as is_warning,
    concat('记录数: ', record_count, ' (预期: {min_threshold} ~ {max_threshold})') as alert_info,
    CASE 
        WHEN record_count < {critical_min} OR record_count > {critical_max} THEN 'Critical'
        WHEN record_count < {min_threshold} OR record_count > {max_threshold} THEN 'AbnormalRed'
        ELSE 'Normal'
    END as status
FROM (
    SELECT count(*) as record_count
    FROM {table_name}
    WHERE {date_column} = current_date() - 1
)
'''


# =============================================================================
# 业务指标监控模板
# =============================================================================

DAU_CHECK_TEMPLATE = '''
-- DAU 监控模板
-- 检查 DAU 环比变化

SELECT
    'DAU监控' as alert_name,
    CASE WHEN dau_change < -{warning_threshold} THEN 1 ELSE 0 END as is_warning,
    concat('DAU: ', today_dau, ', 环比: ', round(dau_change * 100, 1), '%') as alert_info,
    CASE 
        WHEN dau_change < -{critical_threshold} THEN 'Critical'
        WHEN dau_change < -{error_threshold} THEN 'AbnormalRed'
        WHEN dau_change < -{warning_threshold} THEN 'AbnormalYellow'
        ELSE 'Normal'
    END as status
FROM (
    SELECT 
        today.dau as today_dau,
        yesterday.dau as yesterday_dau,
        (today.dau - yesterday.dau) / yesterday.dau as dau_change
    FROM (
        SELECT count(distinct user_id) as dau
        FROM {user_table}
        WHERE dt = current_date() - 1
    ) today
    CROSS JOIN (
        SELECT count(distinct user_id) as dau
        FROM {user_table}
        WHERE dt = current_date() - 2
    ) yesterday
)
'''

ROAS_CHECK_TEMPLATE = '''
-- ROAS 监控模板
-- 检查投放 ROAS 是否达标

SELECT
    'ROAS监控' as alert_name,
    CASE WHEN avg_roas < {target_roas} THEN 1 ELSE 0 END as is_warning,
    concat('ROAS: ', round(avg_roas, 3), ' (目标: {target_roas})') as alert_info,
    CASE 
        WHEN avg_roas < {critical_roas} THEN 'Critical'
        WHEN avg_roas < {error_roas} THEN 'AbnormalRed'
        WHEN avg_roas < {target_roas} THEN 'AbnormalYellow'
        ELSE 'Normal'
    END as status
FROM (
    SELECT sum(revenue) / nullif(sum(cost), 0) as avg_roas
    FROM {marketing_table}
    WHERE dt = current_date() - 1
)
'''

REVENUE_CHECK_TEMPLATE = '''
-- 收入监控模板
-- 检查收入环比变化

SELECT
    '收入监控' as alert_name,
    CASE WHEN revenue_change < -{warning_threshold} THEN 1 ELSE 0 END as is_warning,
    concat('收入: ', round(today_revenue, 2), ', 环比: ', round(revenue_change * 100, 1), '%') as alert_info,
    CASE 
        WHEN revenue_change < -{critical_threshold} THEN 'Critical'
        WHEN revenue_change < -{error_threshold} THEN 'AbnormalRed'
        WHEN revenue_change < -{warning_threshold} THEN 'AbnormalYellow'
        ELSE 'Normal'
    END as status
FROM (
    SELECT 
        today.revenue as today_revenue,
        (today.revenue - yesterday.revenue) / nullif(yesterday.revenue, 0) as revenue_change
    FROM (
        SELECT sum(amount) as revenue
        FROM {revenue_table}
        WHERE dt = current_date() - 1
    ) today
    CROSS JOIN (
        SELECT sum(amount) as revenue
        FROM {revenue_table}
        WHERE dt = current_date() - 2
    ) yesterday
)
'''


# =============================================================================
# 延迟监控模板
# =============================================================================

DATA_FRESHNESS_TEMPLATE = '''
-- 数据新鲜度监控模板
-- 检查数据是否按时更新

SELECT
    '{check_name}' as alert_name,
    CASE WHEN data_age_hours > {warning_hours} THEN 1 ELSE 0 END as is_warning,
    concat('最新数据时间: ', latest_time, ', 延迟: ', data_age_hours, ' 小时') as alert_info,
    CASE 
        WHEN data_age_hours > {critical_hours} THEN 'Critical'
        WHEN data_age_hours > {error_hours} THEN 'AbnormalRed'
        WHEN data_age_hours > {warning_hours} THEN 'AbnormalYellow'
        ELSE 'Normal'
    END as status
FROM (
    SELECT 
        max({timestamp_column}) as latest_time,
        (unix_timestamp(current_timestamp()) - unix_timestamp(max({timestamp_column}))) / 3600 as data_age_hours
    FROM {table_name}
)
'''

PARTITION_CHECK_TEMPLATE = '''
-- 分区存在性检查模板
-- 检查预期分区是否已生成

SELECT
    '{check_name}' as alert_name,
    CASE WHEN partition_exists = 0 THEN 1 ELSE 0 END as is_warning,
    CASE 
        WHEN partition_exists = 1 THEN '分区已就绪'
        ELSE concat('分区 ', '{expected_partition}', ' 不存在')
    END as alert_info,
    CASE WHEN partition_exists = 0 THEN 'AbnormalRed' ELSE 'Normal' END as status
FROM (
    SELECT count(*) as partition_exists
    FROM {table_name}
    WHERE {partition_column} = '{expected_partition}'
    LIMIT 1
)
'''


# =============================================================================
# 多条件组合检查模板
# =============================================================================

MULTI_CHECK_TEMPLATE = '''
-- 多条件组合检查模板
-- 同时检查多个指标

SELECT
    alert_name,
    is_warning,
    alert_info,
    status
FROM (
    -- 检查项 1: NULL 值
    SELECT
        'NULL值检查' as alert_name,
        CASE WHEN null_count > 0 THEN 1 ELSE 0 END as is_warning,
        concat('NULL记录数: ', null_count) as alert_info,
        CASE WHEN null_count > 100 THEN 'AbnormalRed' 
             WHEN null_count > 0 THEN 'AbnormalYellow' 
             ELSE 'Normal' END as status
    FROM (SELECT count(*) as null_count FROM {table} WHERE id IS NULL AND dt = current_date() - 1)
    
    UNION ALL
    
    -- 检查项 2: 重复值
    SELECT
        '重复值检查' as alert_name,
        CASE WHEN dup_count > 0 THEN 1 ELSE 0 END as is_warning,
        concat('重复记录数: ', dup_count) as alert_info,
        CASE WHEN dup_count > 0 THEN 'AbnormalRed' ELSE 'Normal' END as status
    FROM (SELECT count(*) - count(distinct id) as dup_count FROM {table} WHERE dt = current_date() - 1)
    
    UNION ALL
    
    -- 检查项 3: 数据量
    SELECT
        '数据量检查' as alert_name,
        CASE WHEN record_count < {min_records} THEN 1 ELSE 0 END as is_warning,
        concat('记录数: ', record_count, ' (最小: {min_records})') as alert_info,
        CASE WHEN record_count < {min_records} THEN 'AbnormalYellow' ELSE 'Normal' END as status
    FROM (SELECT count(*) as record_count FROM {table} WHERE dt = current_date() - 1)
)
'''


# =============================================================================
# Databricks 原生 Alert 兼容模板（无需 status 列）
# =============================================================================

DATABRICKS_COMPATIBLE_TEMPLATE = '''
-- Databricks 原生 Alert 兼容模板
-- 不需要 status 列，SQL-Probe 会根据 is_warning 自动推断级别
-- is_warning=0 → INFO（不通知）
-- is_warning=1 → ERROR（发送通知，可中断）

SELECT
  '{alert_name}' as alert_name,
  {is_warning_condition} as is_warning,
  {alert_info_expression} as alert_info
FROM {data_source}
'''

# 示例：CPI 波动监控（兼容 Databricks 原生 Alert）
CPI_ALERT_COMPATIBLE = '''
-- CPI 波动监控（Databricks 原生格式，无 status 列）
-- 前一日 CPI vs 前面 3 日平均 CPI 变化超过 ±15%

WITH daily_cpi AS (
    SELECT 
        date,
        network,
        platform,
        sum(cost) as cost,
        sum(installs) as installs,
        CASE WHEN sum(installs) > 0 THEN sum(cost) / sum(installs) ELSE NULL END as cpi
    FROM marketing_data
    WHERE date >= current_date - 4 AND date < current_date
    GROUP BY date, network, platform
),
cpi_analysis AS (
    SELECT
        network,
        platform,
        max(CASE WHEN date = current_date - 1 THEN cpi END) as yesterday_cpi,
        avg(CASE WHEN date < current_date - 1 THEN cpi END) as avg_3days_cpi
    FROM daily_cpi
    GROUP BY network, platform
    HAVING max(CASE WHEN date = current_date - 1 THEN cpi END) IS NOT NULL
       AND avg(CASE WHEN date < current_date - 1 THEN cpi END) IS NOT NULL
)
SELECT
    'CPI波动监控' as alert_name,
    CASE WHEN abs((yesterday_cpi - avg_3days_cpi) / avg_3days_cpi) > 0.15 THEN 1 ELSE 0 END as is_warning,
    concat(
        network, '_', platform,
        ' 昨日CPI: $', round(yesterday_cpi, 2),
        ' 3日均: $', round(avg_3days_cpi, 2),
        ' 变化: ', round((yesterday_cpi - avg_3days_cpi) / avg_3days_cpi * 100, 1), '%'
    ) as alert_info
FROM cpi_analysis
WHERE abs((yesterday_cpi - avg_3days_cpi) / avg_3days_cpi) > 0.15
'''

# 示例：数据源比对（兼容 Databricks 原生 Alert，无 status）
DATASOURCE_COMPARE_COMPATIBLE = '''
-- 数据源比对（Databricks 原生格式）
-- 比较两个数据源的差异率

WITH source_a AS (
    SELECT sum(value) as total_a FROM data_source_a WHERE dt = current_date - 1
),
source_b AS (
    SELECT sum(value) as total_b FROM data_source_b WHERE dt = current_date - 1
)
SELECT
    '数据源比对' as alert_name,
    CASE WHEN abs(1 - total_b / total_a) > 0.0002 THEN 1 ELSE 0 END as is_warning,
    concat('差异率: ', round(abs(1 - total_b / total_a) * 100, 4), '%') as alert_info
FROM source_a, source_b
'''

# 示例：每日报告（使用 NormalGreen 状态）
DAILY_REPORT_WITH_NORMALGREEN = '''
-- 每日报告模板（使用 NormalGreen 状态）
-- 总是发送报告，不触发告警

WITH report_data AS (
    SELECT 
        date,
        sum(cost) as cost,
        sum(installs) as installs,
        round(sum(cost) / sum(installs), 2) as cpi
    FROM marketing_data
    WHERE date >= current_date - 5 AND date < current_date
    GROUP BY date
)
SELECT
    '每日数据报告' as alert_name,
    concat(date, ' | cost: ', cost, ' | installs: ', installs, ' | cpi: ', cpi) as alert_info,
    0 as is_warning,
    'NormalGreen' as status  -- NormalGreen 也支持！
FROM report_data
ORDER BY date
'''


# =============================================================================
# 使用示例
# =============================================================================

def get_null_check_sql(
    check_name: str,
    table_name: str,
    column_name: str,
    date_column: str = "dt",
    threshold: int = 0,
    error_threshold: int = 100,
    critical_threshold: int = 1000
) -> str:
    """生成 NULL 值检查 SQL"""
    return NULL_CHECK_TEMPLATE.format(
        check_name=check_name,
        table_name=table_name,
        column_name=column_name,
        date_column=date_column,
        threshold=threshold,
        error_threshold=error_threshold,
        critical_threshold=critical_threshold
    )


def get_duplicate_check_sql(
    check_name: str,
    table_name: str,
    pk_column: str,
    date_column: str = "dt"
) -> str:
    """生成主键重复检查 SQL"""
    return DUPLICATE_CHECK_TEMPLATE.format(
        check_name=check_name,
        table_name=table_name,
        pk_column=pk_column,
        date_column=date_column
    )


def get_dau_check_sql(
    user_table: str,
    warning_threshold: float = 0.2,
    error_threshold: float = 0.3,
    critical_threshold: float = 0.5
) -> str:
    """生成 DAU 监控 SQL"""
    return DAU_CHECK_TEMPLATE.format(
        user_table=user_table,
        warning_threshold=warning_threshold,
        error_threshold=error_threshold,
        critical_threshold=critical_threshold
    )


def get_roas_check_sql(
    marketing_table: str,
    target_roas: float = 1.0,
    error_roas: float = 0.8,
    critical_roas: float = 0.5
) -> str:
    """生成 ROAS 监控 SQL"""
    return ROAS_CHECK_TEMPLATE.format(
        marketing_table=marketing_table,
        target_roas=target_roas,
        error_roas=error_roas,
        critical_roas=critical_roas
    )


# 示例调用
if __name__ == "__main__":
    # 生成 NULL 检查 SQL
    sql = get_null_check_sql(
        check_name="用户ID为空检查",
        table_name="user_events",
        column_name="user_id",
        threshold=0,
        error_threshold=100
    )
    print("NULL 检查 SQL:")
    print(sql)
    print()
    
    # 生成 DAU 监控 SQL
    sql = get_dau_check_sql(
        user_table="user_activity",
        warning_threshold=0.15
    )
    print("DAU 监控 SQL:")
    print(sql)

