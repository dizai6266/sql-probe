"""
SQL-Probe 基本使用示例

本文件展示 SQL-Probe 的基本用法，可在 Databricks Notebook 中运行
"""

# =============================================================================
# 示例 1: 基本初始化和执行
# =============================================================================

def example_basic():
    """基本用法示例"""
    from sql_probe import SQLProbeNotifier
    
    # 初始化探针（在 Databricks 中 spark 已自动可用）
    probe = SQLProbeNotifier(
        spark=spark,  # noqa: F821
        webhook="https://open.feishu.cn/open-apis/bot/v2/hook/your-webhook",
        source="数据质量检查"
    )
    
    # 执行简单检查
    result = probe.execute('''
        SELECT
            '数据完整性检查' as alert_name,
            CASE WHEN missing_rate > 0.1 THEN 1 ELSE 0 END as is_warning,
            concat('缺失率: ', round(missing_rate * 100, 2), '%') as alert_info,
            CASE 
                WHEN missing_rate > 0.3 THEN 'AbnormalRed'
                WHEN missing_rate > 0.1 THEN 'AbnormalYellow'
                ELSE 'Normal'
            END as status
        FROM (
            SELECT 
                1 - count(user_id) / count(*) as missing_rate
            FROM your_table
            WHERE dt = current_date() - 1
        )
    ''')
    
    # 检查结果
    print(f"级别: {result.level.name}")
    print(f"触发: {result.triggered}")
    print(f"内容: {result.content}")
    
    return result


# =============================================================================
# 示例 2: ETL 流程中的阻断性检查
# =============================================================================

def example_etl_with_check():
    """ETL 流程中使用 SQL-Probe 进行质量检查"""
    from sql_probe import SQLProbeNotifier, ProbeInterruptError
    
    probe = SQLProbeNotifier(
        spark=spark,  # noqa: F821
        webhook="https://...",
        source="DailyETL",
        interrupt_on_error=True  # ERROR 级别自动中断
    )
    
    # Step 1: 数据抽取
    print("Step 1: 数据抽取...")
    spark.sql("""  # noqa: F821
        CREATE OR REPLACE TEMP VIEW staging AS
        SELECT * FROM source_table WHERE dt = current_date() - 1
    """)
    
    # Step 2: 数据质量检查
    print("Step 2: 数据质量检查...")
    probe.execute('''
        SELECT
            'NULL值检查' as alert_name,
            CASE WHEN null_cnt > 100 THEN 1 ELSE 0 END as is_warning,
            concat('主键为空记录数: ', null_cnt) as alert_info,
            CASE WHEN null_cnt > 100 THEN 'AbnormalRed' ELSE 'Normal' END as status
        FROM (SELECT count(*) as null_cnt FROM staging WHERE id IS NULL)
        
        UNION ALL
        
        SELECT
            '重复值检查' as alert_name,
            CASE WHEN dup_cnt > 0 THEN 1 ELSE 0 END as is_warning,
            concat('重复记录数: ', dup_cnt) as alert_info,
            CASE WHEN dup_cnt > 0 THEN 'AbnormalRed' ELSE 'Normal' END as status
        FROM (
            SELECT count(*) - count(distinct id) as dup_cnt 
            FROM staging
        )
    ''')
    
    # Step 3: 数据加载（只有检查通过才会执行到这里）
    print("Step 3: 数据加载...")
    spark.sql("""  # noqa: F821
        INSERT INTO target_table
        SELECT * FROM staging
    """)
    
    print("ETL 完成！")


# =============================================================================
# 示例 3: 批量监控
# =============================================================================

def example_batch_monitoring():
    """批量执行多个监控检查"""
    from sql_probe import SQLProbeNotifier
    
    probe = SQLProbeNotifier(
        spark=spark,  # noqa: F821
        webhook="https://...",
        source="日常监控"
    )
    
    # 定义多个检查 SQL
    checks = [
        {
            "sql": '''
                SELECT
                    'DAU监控' as alert_name,
                    CASE WHEN dau_change < -0.2 THEN 1 ELSE 0 END as is_warning,
                    concat('DAU 环比变化: ', round(dau_change * 100, 1), '%') as alert_info,
                    CASE 
                        WHEN dau_change < -0.3 THEN 'AbnormalRed'
                        WHEN dau_change < -0.2 THEN 'AbnormalYellow'
                        ELSE 'Normal'
                    END as status
                FROM (
                    SELECT 
                        (today_dau - yesterday_dau) / yesterday_dau as dau_change
                    FROM dau_metrics
                    WHERE dt = current_date() - 1
                )
            ''',
            "name": "DAU监控"
        },
        {
            "sql": '''
                SELECT
                    'ROAS监控' as alert_name,
                    CASE WHEN avg_roas < 1.0 THEN 1 ELSE 0 END as is_warning,
                    concat('平均ROAS: ', round(avg_roas, 2)) as alert_info,
                    CASE 
                        WHEN avg_roas < 0.5 THEN 'Critical'
                        WHEN avg_roas < 0.8 THEN 'AbnormalRed'
                        WHEN avg_roas < 1.0 THEN 'AbnormalYellow'
                        ELSE 'Normal'
                    END as status
                FROM (
                    SELECT avg(roas) as avg_roas
                    FROM marketing_metrics
                    WHERE dt = current_date() - 1
                )
            ''',
            "name": "ROAS监控"
        },
        {
            "sql": '''
                SELECT
                    '收入监控' as alert_name,
                    CASE WHEN revenue_change < -0.1 THEN 1 ELSE 0 END as is_warning,
                    concat('收入环比: ', round(revenue_change * 100, 1), '%') as alert_info,
                    CASE 
                        WHEN revenue_change < -0.2 THEN 'AbnormalRed'
                        WHEN revenue_change < -0.1 THEN 'AbnormalYellow'
                        ELSE 'Normal'
                    END as status
                FROM revenue_metrics
                WHERE dt = current_date() - 1
            ''',
            "name": "收入监控"
        }
    ]
    
    # 批量执行（跑完所有再汇总）
    result = probe.execute_batch(
        checks,
        interrupt_on_error=False,  # 不中断，跑完所有
        title_prefix="【日报】"
    )
    
    print(f"汇总级别: {result.level.name}")
    print(f"汇总内容:\n{result.content}")
    
    return result


# =============================================================================
# 示例 4: 自定义中断处理
# =============================================================================

def example_custom_interrupt_handling():
    """自定义中断处理逻辑"""
    from sql_probe import SQLProbeNotifier, ProbeInterruptError
    
    probe = SQLProbeNotifier(
        spark=spark,  # noqa: F821
        webhook="https://...",
        source="关键任务"
    )
    
    try:
        result = probe.execute('''
            SELECT
                '关键指标检查' as alert_name,
                1 as is_warning,
                '指标异常' as alert_info,
                'AbnormalRed' as status
        ''')
        print(f"检查通过: {result.level.name}")
        
    except ProbeInterruptError as e:
        print(f"检查被中断!")
        print(f"级别: {e.result.level.name}")
        print(f"内容: {e.result.content}")
        
        # 选项 1: 重新抛出，终止 Notebook
        # raise
        
        # 选项 2: 记录后继续
        print("已记录异常，继续执行...")
        
        # 选项 3: 发送额外通知
        # probe.warning("已知问题，继续执行")
    
    # 后续代码可以继续执行
    print("后续处理...")


# =============================================================================
# 示例 5: 调试模式
# =============================================================================

def example_debug_mode():
    """调试模式使用"""
    from sql_probe import SQLProbeNotifier
    
    # 开启调试模式
    probe = SQLProbeNotifier(
        spark=spark,  # noqa: F821
        webhook="https://...",
        source="调试",
        debug=True  # 开启调试日志
    )
    
    sql = '''
        SELECT
            '测试检查' as alert_name,
            1 as is_warning,
            '测试消息' as alert_info,
            'AbnormalYellow' as status
    '''
    
    # 1. 验证 SQL 格式
    validation = probe.validate(sql)
    print(f"SQL 验证结果: {validation}")
    
    if not validation["valid"]:
        print(f"SQL 格式错误: {validation['error']}")
        return
    
    # 2. 静默执行（不发通知）
    result = probe.execute(
        sql,
        silent=True,  # 不发送通知
        interrupt_on_error=False  # 不中断
    )
    
    # 3. 查看详细结果
    print(f"\n结果摘要:\n{result.summary()}")
    print(f"\n详情:")
    for detail in result.details:
        print(f"  - {detail.alert_name}: {detail.alert_info} [{detail.level.name}]")


# =============================================================================
# 示例 6: 使用已有的 Notifier 实例
# =============================================================================

def example_with_existing_notifier():
    """使用已有的 feishu-notify Notifier"""
    from feishu_notify import Notifier
    from sql_probe import SQLProbeNotifier
    
    # 使用团队已有的 Notifier 配置
    notifier = Notifier(
        webhook="https://...",
        source="共享通知器",
        enable_dedup=True,
        enable_rate_limit=True
    )
    
    # 传入已有的 notifier
    probe = SQLProbeNotifier(
        spark=spark,  # noqa: F821
        notifier=notifier  # 复用已有实例
    )
    
    result = probe.execute('''
        SELECT
            '使用共享通知器' as alert_name,
            0 as is_warning,
            '正常' as alert_info,
            'Normal' as status
    ''')
    
    print(f"执行完成: {result.level.name}")


# =============================================================================
# 运行示例
# =============================================================================

if __name__ == "__main__":
    # 注意：以下代码需要在 Databricks 环境中运行
    # 这里仅展示调用方式
    
    print("=" * 60)
    print("SQL-Probe 使用示例")
    print("=" * 60)
    
    # 在 Databricks Notebook 中取消注释以运行
    # example_basic()
    # example_etl_with_check()
    # example_batch_monitoring()
    # example_custom_interrupt_handling()
    # example_debug_mode()
    # example_with_existing_notifier()
    
    print("\n请在 Databricks Notebook 中运行这些示例")

