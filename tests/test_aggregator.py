"""
ResultAggregator 结果聚合器测试
"""

import pytest
from sql_probe.core.aggregator import ResultAggregator
from sql_probe.core.resolver import LevelResolver
from sql_probe.models.level import AlertLevel
from sql_probe.models.result import ProbeResult


class TestResultAggregator:
    """ResultAggregator 测试"""
    
    @pytest.fixture
    def aggregator(self):
        return ResultAggregator()
    
    @pytest.fixture
    def resolver(self):
        return LevelResolver()
    
    def test_aggregate_no_warning(self, aggregator, resolver):
        """测试无告警结果聚合"""
        rows = [{"is_warning": 0, "status": "Normal", "alert_info": "OK", "alert_name": "Test"}]
        level, details = resolver.resolve_all(rows)
        
        result = aggregator.aggregate(
            details=details,
            level=level,
            execution_time=1.5,
            sql_text="SELECT ...",
            default_alert_name="Test Alert"
        )
        
        assert result.level == AlertLevel.INFO
        assert result.triggered == False
        assert result.content == "所有检查项正常"
        assert result.row_count == 1
        assert result.execution_time == 1.5
    
    def test_aggregate_single_warning(self, aggregator, resolver):
        """测试单条告警结果聚合"""
        rows = [{
            "is_warning": 1,
            "status": "AbnormalYellow",
            "alert_info": "ROAS 低于阈值",
            "alert_name": "ROAS检查"
        }]
        level, details = resolver.resolve_all(rows)
        
        result = aggregator.aggregate(
            details=details,
            level=level,
            execution_time=0.8,
            sql_text="SELECT ..."
        )
        
        assert result.level == AlertLevel.WARNING
        assert result.triggered == True
        assert "[AbnormalYellow] ROAS 低于阈值" in result.content
        assert result.alert_name == "ROAS检查"
    
    def test_aggregate_multiple_warnings(self, aggregator, resolver):
        """测试多条告警结果聚合"""
        rows = [
            {"is_warning": 1, "status": "AbnormalYellow", "alert_info": "问题1", "alert_name": "Test"},
            {"is_warning": 1, "status": "AbnormalRed", "alert_info": "问题2", "alert_name": "Test"},
            {"is_warning": 0, "status": "Normal", "alert_info": "OK", "alert_name": "Test"},
        ]
        level, details = resolver.resolve_all(rows)
        
        result = aggregator.aggregate(
            details=details,
            level=level,
            execution_time=1.0,
            sql_text="SELECT ..."
        )
        
        assert result.level == AlertLevel.ERROR
        assert result.triggered == True
        assert "共 2 项告警" in result.content
        assert "问题1" in result.content
        assert "问题2" in result.content
    
    def test_aggregate_default_alert_name(self, aggregator, resolver):
        """测试默认告警名称"""
        rows = [{"is_warning": 0, "status": "Normal", "alert_info": "OK"}]
        level, details = resolver.resolve_all(rows)
        
        # 使用传入的 default_alert_name
        result = aggregator.aggregate(
            details=details,
            level=level,
            execution_time=0.5,
            sql_text="SELECT ...",
            default_alert_name="我的检查"
        )
        assert result.alert_name == "我的检查"
        
        # 没有传入 default_alert_name，使用 SQL 结果中的
        rows = [{"is_warning": 0, "status": "Normal", "alert_info": "OK", "alert_name": "SQL检查"}]
        level, details = resolver.resolve_all(rows)
        result = aggregator.aggregate(
            details=details,
            level=level,
            execution_time=0.5,
            sql_text="SELECT ..."
        )
        assert result.alert_name == "SQL检查"
    
    def test_aggregate_batch_all_pass(self, aggregator):
        """测试批量结果聚合 - 全部通过"""
        results = [
            ProbeResult(
                level=AlertLevel.INFO,
                triggered=False,
                alert_name="检查1",
                content="正常",
                row_count=1,
                execution_time=0.5
            ),
            ProbeResult(
                level=AlertLevel.INFO,
                triggered=False,
                alert_name="检查2",
                content="正常",
                row_count=1,
                execution_time=0.3
            )
        ]
        
        result = aggregator.aggregate_batch(results)
        
        assert result.level == AlertLevel.INFO
        assert result.triggered == False
        assert "全部 2 项检查通过" in result.content
        assert result.execution_time == 0.8
    
    def test_aggregate_batch_with_warnings(self, aggregator):
        """测试批量结果聚合 - 有告警"""
        results = [
            ProbeResult(
                level=AlertLevel.INFO,
                triggered=False,
                alert_name="检查1",
                content="正常",
                row_count=1,
                execution_time=0.5
            ),
            ProbeResult(
                level=AlertLevel.WARNING,
                triggered=True,
                alert_name="检查2",
                content="发现问题",
                row_count=1,
                execution_time=0.3
            ),
            ProbeResult(
                level=AlertLevel.ERROR,
                triggered=True,
                alert_name="检查3",
                content="严重问题",
                row_count=1,
                execution_time=0.4
            )
        ]
        
        result = aggregator.aggregate_batch(results)
        
        assert result.level == AlertLevel.ERROR  # 最高级别
        assert result.triggered == True
        assert "2/3 项触发告警" in result.content
        assert result.execution_time == 1.2
    
    def test_aggregate_batch_empty(self, aggregator):
        """测试空批量结果聚合"""
        result = aggregator.aggregate_batch([])
        
        assert result.level == AlertLevel.INFO
        assert result.triggered == False
        assert result.content == "无检查项"
    
    def test_create_error_result(self, aggregator):
        """测试创建错误结果"""
        result = aggregator.create_error_result(
            error_message="SQL 语法错误",
            sql_text="SELECT * FORM table",
            alert_name="失败检查",
            execution_time=0.1
        )
        
        assert result.level == AlertLevel.ERROR
        assert result.triggered == True
        assert result.success == False
        assert "SQL 语法错误" in result.error_message
        assert "执行失败" in result.content


class TestProbeResult:
    """ProbeResult 数据类测试"""
    
    def test_bool_triggered(self):
        """测试 bool 转换 - 触发告警"""
        result = ProbeResult(
            level=AlertLevel.WARNING,
            triggered=True,
            alert_name="Test",
            content="Warning"
        )
        assert bool(result) == True
    
    def test_bool_not_triggered(self):
        """测试 bool 转换 - 未触发告警"""
        result = ProbeResult(
            level=AlertLevel.INFO,
            triggered=False,
            alert_name="Test",
            content="OK"
        )
        assert bool(result) == False
    
    def test_is_critical(self):
        """测试 is_critical 属性"""
        assert ProbeResult(level=AlertLevel.CRITICAL, triggered=True, alert_name="", content="").is_critical == True
        assert ProbeResult(level=AlertLevel.ERROR, triggered=True, alert_name="", content="").is_critical == False
    
    def test_is_error(self):
        """测试 is_error 属性"""
        assert ProbeResult(level=AlertLevel.CRITICAL, triggered=True, alert_name="", content="").is_error == True
        assert ProbeResult(level=AlertLevel.ERROR, triggered=True, alert_name="", content="").is_error == True
        assert ProbeResult(level=AlertLevel.WARNING, triggered=True, alert_name="", content="").is_error == False
    
    def test_is_warning(self):
        """测试 is_warning 属性"""
        assert ProbeResult(level=AlertLevel.WARNING, triggered=True, alert_name="", content="").is_warning == True
        assert ProbeResult(level=AlertLevel.INFO, triggered=False, alert_name="", content="").is_warning == False
    
    def test_to_dict(self):
        """测试序列化为字典"""
        result = ProbeResult(
            level=AlertLevel.WARNING,
            triggered=True,
            alert_name="Test",
            content="Warning message",
            row_count=1,
            execution_time=0.5
        )
        
        d = result.to_dict()
        assert d["level"] == "WARNING"
        assert d["triggered"] == True
        assert d["alert_name"] == "Test"
        assert d["content"] == "Warning message"
    
    def test_summary(self):
        """测试结果摘要"""
        result = ProbeResult(
            level=AlertLevel.ERROR,
            triggered=True,
            alert_name="ROAS检查",
            content="ROAS 低于阈值",
            row_count=1,
            execution_time=0.5
        )
        
        summary = result.summary()
        assert "ROAS检查" in summary
        assert "ERROR" in summary
        assert "是" in summary  # 是否触发: 是

