"""
SQLProbeNotifier 集成测试
"""

import pytest
from unittest.mock import Mock, MagicMock, patch

from sql_probe import SQLProbeNotifier, AlertLevel, ProbeResult, ProbeInterruptError
from sql_probe.models.exceptions import SQLExecutionError, SQLValidationError


class TestSQLProbeNotifier:
    """SQLProbeNotifier 集成测试"""
    
    @pytest.fixture
    def mock_spark(self):
        """创建模拟的 SparkSession"""
        spark = Mock()
        return spark
    
    @pytest.fixture
    def mock_df_normal(self):
        """创建返回正常结果的模拟 DataFrame"""
        df = Mock()
        df.columns = ["alert_name", "is_warning", "alert_info", "status"]
        df.collect.return_value = [
            Mock(asDict=lambda: {
                "alert_name": "Test",
                "is_warning": 0,
                "alert_info": "All good",
                "status": "Normal"
            })
        ]
        return df
    
    @pytest.fixture
    def mock_df_warning(self):
        """创建返回警告结果的模拟 DataFrame"""
        df = Mock()
        df.columns = ["alert_name", "is_warning", "alert_info", "status"]
        df.collect.return_value = [
            Mock(asDict=lambda: {
                "alert_name": "Test",
                "is_warning": 1,
                "alert_info": "Warning message",
                "status": "AbnormalYellow"
            })
        ]
        return df
    
    @pytest.fixture
    def mock_df_error(self):
        """创建返回错误结果的模拟 DataFrame"""
        df = Mock()
        df.columns = ["alert_name", "is_warning", "alert_info", "status"]
        df.collect.return_value = [
            Mock(asDict=lambda: {
                "alert_name": "Test",
                "is_warning": 1,
                "alert_info": "Error message",
                "status": "AbnormalRed"
            })
        ]
        return df
    
    @pytest.fixture
    def mock_df_critical(self):
        """创建返回严重结果的模拟 DataFrame"""
        df = Mock()
        df.columns = ["alert_name", "is_warning", "alert_info", "status"]
        df.collect.return_value = [
            Mock(asDict=lambda: {
                "alert_name": "Test",
                "is_warning": 1,
                "alert_info": "Critical message",
                "status": "Critical"
            })
        ]
        return df
    
    def test_execute_normal(self, mock_spark, mock_df_normal):
        """测试执行正常 SQL"""
        mock_spark.sql.return_value = mock_df_normal
        
        probe = SQLProbeNotifier(mock_spark, debug=False)
        result = probe.execute("SELECT ...", silent=True)
        
        assert result.level == AlertLevel.INFO
        assert result.triggered == False
        assert result.success == True
    
    def test_execute_warning_no_interrupt(self, mock_spark, mock_df_warning):
        """测试执行警告 SQL - 不中断"""
        mock_spark.sql.return_value = mock_df_warning
        
        probe = SQLProbeNotifier(mock_spark, debug=False)
        result = probe.execute("SELECT ...", silent=True)
        
        assert result.level == AlertLevel.WARNING
        assert result.triggered == True
        # WARNING 级别不会中断
    
    def test_execute_error_interrupt(self, mock_spark, mock_df_error):
        """测试执行错误 SQL - 中断"""
        mock_spark.sql.return_value = mock_df_error
        
        probe = SQLProbeNotifier(mock_spark, debug=False, interrupt_on_error=True)
        
        with pytest.raises(ProbeInterruptError) as exc_info:
            probe.execute("SELECT ...", silent=True)
        
        assert exc_info.value.result.level == AlertLevel.ERROR
    
    def test_execute_error_no_interrupt_when_disabled(self, mock_spark, mock_df_error):
        """测试执行错误 SQL - 禁用中断"""
        mock_spark.sql.return_value = mock_df_error
        
        probe = SQLProbeNotifier(mock_spark, debug=False, interrupt_on_error=False)
        result = probe.execute("SELECT ...", silent=True)
        
        assert result.level == AlertLevel.ERROR
        assert result.triggered == True
        # 没有抛出异常
    
    def test_execute_critical_always_interrupt(self, mock_spark, mock_df_critical):
        """测试执行严重 SQL - 始终中断"""
        mock_spark.sql.return_value = mock_df_critical
        
        # 即使 interrupt_on_error=False，CRITICAL 也会中断
        probe = SQLProbeNotifier(mock_spark, debug=False, interrupt_on_error=False)
        
        with pytest.raises(ProbeInterruptError) as exc_info:
            probe.execute("SELECT ...", silent=True)
        
        assert exc_info.value.result.level == AlertLevel.CRITICAL
    
    def test_execute_with_force_level(self, mock_spark, mock_df_error):
        """测试强制级别覆盖"""
        mock_spark.sql.return_value = mock_df_error
        
        probe = SQLProbeNotifier(mock_spark, debug=False)
        result = probe.execute(
            "SELECT ...",
            silent=True,
            force_level=AlertLevel.INFO,
            interrupt_on_error=False
        )
        
        # 强制为 INFO，不触发中断
        assert result.level == AlertLevel.INFO
    
    def test_execute_with_max_level(self, mock_spark, mock_df_critical):
        """测试最大级别限制"""
        mock_spark.sql.return_value = mock_df_critical
        
        probe = SQLProbeNotifier(mock_spark, debug=False)
        result = probe.execute(
            "SELECT ...",
            silent=True,
            max_level=AlertLevel.WARNING,
            interrupt_on_error=False
        )
        
        # 降级为 WARNING
        assert result.level == AlertLevel.WARNING
    
    def test_execute_sql_execution_error(self, mock_spark):
        """测试 SQL 执行错误"""
        mock_spark.sql.side_effect = Exception("Table not found")
        
        probe = SQLProbeNotifier(mock_spark, debug=False)
        
        with pytest.raises(SQLExecutionError):
            probe.execute("SELECT * FROM nonexistent", silent=True)
    
    def test_execute_sql_validation_error(self, mock_spark):
        """测试 SQL 结果验证错误"""
        df = Mock()
        df.columns = ["wrong_column"]  # 缺少必需列
        mock_spark.sql.return_value = df
        
        probe = SQLProbeNotifier(mock_spark, debug=False)
        
        with pytest.raises(SQLValidationError):
            probe.execute("SELECT wrong_column FROM table", silent=True)
    
    def test_validate_sql(self, mock_spark):
        """测试 SQL 验证（Dry Run）"""
        df = Mock()
        df.columns = ["alert_name", "is_warning", "alert_info", "status"]
        mock_spark.sql.return_value = df
        
        probe = SQLProbeNotifier(mock_spark, debug=False)
        result = probe.validate("SELECT ...")
        
        assert result["valid"] == True
        assert "is_warning" in [c.lower() for c in result["columns"]]
    
    def test_validate_sql_missing_columns(self, mock_spark):
        """测试 SQL 验证 - 缺少列"""
        df = Mock()
        df.columns = ["alert_name"]  # 缺少必需列
        mock_spark.sql.return_value = df
        
        probe = SQLProbeNotifier(mock_spark, debug=False)
        result = probe.validate("SELECT alert_name FROM table")
        
        assert result["valid"] == False
        assert "缺少必需列" in result["error"]


class TestSQLProbeNotifierBatch:
    """SQLProbeNotifier 批量执行测试"""
    
    @pytest.fixture
    def mock_spark(self):
        spark = Mock()
        return spark
    
    def test_execute_batch_all_pass(self, mock_spark):
        """测试批量执行 - 全部通过"""
        def create_normal_df():
            df = Mock()
            df.columns = ["alert_name", "is_warning", "alert_info", "status"]
            df.collect.return_value = [
                Mock(asDict=lambda: {
                    "alert_name": "Test",
                    "is_warning": 0,
                    "alert_info": "OK",
                    "status": "Normal"
                })
            ]
            return df
        
        mock_spark.sql.return_value = create_normal_df()
        
        probe = SQLProbeNotifier(mock_spark, debug=False)
        result = probe.execute_batch([
            {"sql": "SELECT 1", "name": "检查1"},
            {"sql": "SELECT 2", "name": "检查2"},
        ], silent=True)
        
        assert result.level == AlertLevel.INFO
        assert result.triggered == False
        assert "全部" in result.content and "检查通过" in result.content
    
    def test_execute_batch_with_failures(self, mock_spark):
        """测试批量执行 - 有失败"""
        call_count = [0]
        
        def mock_sql(sql):
            call_count[0] += 1
            df = Mock()
            df.columns = ["alert_name", "is_warning", "alert_info", "status"]
            
            if call_count[0] == 1:
                # 第一个查询返回正常
                df.collect.return_value = [
                    Mock(asDict=lambda: {
                        "alert_name": "检查1",
                        "is_warning": 0,
                        "alert_info": "OK",
                        "status": "Normal"
                    })
                ]
            else:
                # 第二个查询返回错误
                df.collect.return_value = [
                    Mock(asDict=lambda: {
                        "alert_name": "检查2",
                        "is_warning": 1,
                        "alert_info": "Error",
                        "status": "AbnormalRed"
                    })
                ]
            return df
        
        mock_spark.sql.side_effect = mock_sql
        
        probe = SQLProbeNotifier(mock_spark, debug=False, interrupt_on_error=False)
        result = probe.execute_batch([
            {"sql": "SELECT 1", "name": "检查1"},
            {"sql": "SELECT 2", "name": "检查2"},
        ], silent=True, interrupt_on_error=False)
        
        assert result.level == AlertLevel.ERROR
        assert result.triggered == True
    
    def test_execute_batch_interrupt_on_error(self, mock_spark):
        """测试批量执行 - ERROR 时中断"""
        def create_error_df():
            df = Mock()
            df.columns = ["alert_name", "is_warning", "alert_info", "status"]
            df.collect.return_value = [
                Mock(asDict=lambda: {
                    "alert_name": "Test",
                    "is_warning": 1,
                    "alert_info": "Error",
                    "status": "AbnormalRed"
                })
            ]
            return df
        
        mock_spark.sql.return_value = create_error_df()
        
        probe = SQLProbeNotifier(mock_spark, debug=False, interrupt_on_error=True)
        
        with pytest.raises(ProbeInterruptError):
            probe.execute_batch([
                {"sql": "SELECT 1", "name": "检查1"},
            ], silent=True)


class TestProbeInterruptError:
    """ProbeInterruptError 异常测试"""
    
    def test_exception_with_result(self):
        """测试带结果的异常"""
        result = ProbeResult(
            level=AlertLevel.ERROR,
            triggered=True,
            alert_name="Test",
            content="Error message"
        )
        
        exc = ProbeInterruptError("流程中断", result=result)
        
        assert exc.result == result
        assert "ERROR" in str(exc)
        assert "Test" in str(exc)
    
    def test_exception_without_result(self):
        """测试无结果的异常"""
        exc = ProbeInterruptError("简单中断")
        
        assert exc.result is None
        assert "简单中断" in str(exc)
    
    def test_exception_can_be_caught(self):
        """测试异常可以被捕获"""
        result = ProbeResult(
            level=AlertLevel.ERROR,
            triggered=True,
            alert_name="Test",
            content="Error"
        )
        
        try:
            raise ProbeInterruptError("中断", result=result)
        except ProbeInterruptError as e:
            assert e.result.level == AlertLevel.ERROR
            # 可以选择继续执行
            pass
        
        # 代码可以继续执行
        assert True

