import sys
import os
import logging

# 1. 设置路径，确保能导入 sql_probe 和 feishu_notify
# 获取脚本所在目录
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

# 2. Mock Spark 环境
class MockRow:
    def __init__(self, data):
        self._data = data
    
    def asDict(self):
        return self._data

class MockDataFrame:
    def __init__(self, data):
        self._data = data
        if data:
            self.columns = list(data[0].keys())
        else:
            # 默认给一些列，避免空数据时验证失败
            self.columns = ["alert_name", "is_warning", "alert_info", "status"]

    def collect(self):
        return [MockRow(d) for d in self._data]

class MockSparkSession:
    def sql(self, query):
        print(f"\n[MockSpark] Executing SQL:\n{query.strip()}\n")
        
        # 模拟返回一个告警结果
        return MockDataFrame([{
            "alert_name": "本地测试告警 (dizai-test)",
            "is_warning": 1,
            "alert_info": "这是一条来自本地开发机的测试告警，用于验证 Webhook 配置是否正确。",
            "status": "AbnormalYellow",
            "cnt": 999
        }])

# 3. 运行测试
def run_test():
    print(f"当前 Python 路径: {sys.path}")
    
    # 显式尝试导入 feishu_notify 以诊断问题
    try:
        import feishu_notify
        print(f"✅ 成功导入 feishu_notify (路径: {feishu_notify.__file__})")
        from feishu_notify.notifier import Notifier
        print("✅ 成功导入 feishu_notify.notifier.Notifier")
    except ImportError as e:
        print(f"❌ 导入 feishu_notify 失败: {e}")
        return

    # 尝试导入 sql_probe
    try:
        from sql_probe import SQLProbeNotifier
        print("✅ 成功导入 sql_probe 库")
    except ImportError as e:
        print(f"❌ 导入 sql_probe 失败: {e}")
        return

    # 配置 logging
    logging.basicConfig(level=logging.INFO)

    # 模拟 Spark
    spark = MockSparkSession()
    
    # 用户的 Webhook
    webhook_url = "https://open.feishu.cn/open-apis/bot/v2/hook/6d8b23ff-fe40-473f-a9c7-1db6398eda61"
    
    print(f"🚀 初始化探针，Webhook: {webhook_url}")
    
    # 初始化探针
    # source 设置为 "Local Test"
    probe = SQLProbeNotifier(
        spark, 
        webhook=webhook_url, 
        source="dizai-test",
        debug=True  # 开启 debug 模式可以看到更多日志
    )

    print("running execute...")
    
    # 执行测试 SQL
    try:
        result = probe.execute('''
            SELECT 
                'dizai-test' as alert_name,
                1 as is_warning, 
                'Testing webhook connectivity' as alert_info,
                'AbnormalYellow' as status
        ''')
        
        print("\n📊 执行结果:")
        print(f"  - 触发状态: {result.triggered}")
        print(f"  - 级别: {result.level.name}")
        print(f"  - 内容: {result.content}")
        
        if result.triggered:
            print("\n✅ 测试完成！请检查飞书群 'dizai-test' 是否收到消息。")
        else:
            print("\n❓ 未触发告警，请检查逻辑。")
            
    except Exception as e:
        print(f"\n❌ 执行出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_test()
