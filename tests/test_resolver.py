"""
LevelResolver 级别解析器测试
"""

import pytest
from sql_probe.core.resolver import LevelResolver
from sql_probe.models.level import AlertLevel


class TestLevelResolver:
    """LevelResolver 测试"""
    
    @pytest.fixture
    def resolver(self):
        return LevelResolver()
    
    def test_resolve_row_normal(self, resolver):
        """测试正常行解析"""
        row = {
            "alert_name": "Test",
            "is_warning": 0,
            "alert_info": "All good",
            "status": "Normal"
        }
        detail = resolver.resolve_row(row)
        
        assert detail.alert_name == "Test"
        assert detail.is_warning == False
        assert detail.alert_info == "All good"
        assert detail.status == "Normal"
        assert detail.level == AlertLevel.INFO
    
    def test_resolve_row_warning(self, resolver):
        """测试警告行解析"""
        row = {
            "alert_name": "Test",
            "is_warning": 1,
            "alert_info": "Warning message",
            "status": "AbnormalYellow"
        }
        detail = resolver.resolve_row(row)
        
        assert detail.is_warning == True
        assert detail.level == AlertLevel.WARNING
    
    def test_resolve_row_error(self, resolver):
        """测试错误行解析"""
        row = {
            "alert_name": "Test",
            "is_warning": 1,
            "alert_info": "Error message",
            "status": "AbnormalRed"
        }
        detail = resolver.resolve_row(row)
        
        assert detail.is_warning == True
        assert detail.level == AlertLevel.ERROR
    
    def test_resolve_row_critical(self, resolver):
        """测试严重行解析"""
        row = {
            "alert_name": "Test",
            "is_warning": 1,
            "alert_info": "Critical message",
            "status": "Critical"
        }
        detail = resolver.resolve_row(row)
        
        assert detail.is_warning == True
        assert detail.level == AlertLevel.CRITICAL
    
    def test_resolve_row_is_warning_zero_ignores_status(self, resolver):
        """测试 is_warning=0 时忽略 status"""
        row = {
            "alert_name": "Test",
            "is_warning": 0,
            "alert_info": "Should be INFO",
            "status": "AbnormalRed"  # 即使 status 是 ERROR，也应返回 INFO
        }
        detail = resolver.resolve_row(row)
        
        assert detail.level == AlertLevel.INFO
    
    def test_resolve_row_case_insensitive(self, resolver):
        """测试列名不区分大小写"""
        row = {
            "ALERT_NAME": "Test",
            "IS_WARNING": 1,
            "Alert_Info": "Message",
            "STATUS": "AbnormalYellow"
        }
        detail = resolver.resolve_row(row)
        
        assert detail.alert_name == "Test"
        assert detail.is_warning == True
        assert detail.level == AlertLevel.WARNING
    
    def test_resolve_row_default_values(self, resolver):
        """测试缺失字段的默认值"""
        row = {"is_warning": 1, "status": "Normal"}
        detail = resolver.resolve_row(row)
        
        assert detail.alert_name == "未命名告警"
        assert detail.alert_info == ""
    
    def test_resolve_all_empty(self, resolver):
        """测试空行列表"""
        level, details = resolver.resolve_all([])
        
        assert level == AlertLevel.INFO
        assert details == []
    
    def test_resolve_all_single_row(self, resolver):
        """测试单行解析"""
        rows = [{
            "alert_name": "Test",
            "is_warning": 1,
            "alert_info": "Warning",
            "status": "AbnormalYellow"
        }]
        level, details = resolver.resolve_all(rows)
        
        assert level == AlertLevel.WARNING
        assert len(details) == 1
    
    def test_resolve_all_highest_level(self, resolver):
        """测试多行取最高级别"""
        rows = [
            {"is_warning": 1, "status": "AbnormalYellow", "alert_info": "W1"},
            {"is_warning": 1, "status": "AbnormalRed", "alert_info": "E1"},
            {"is_warning": 0, "status": "Normal", "alert_info": "OK"},
        ]
        level, details = resolver.resolve_all(rows)
        
        assert level == AlertLevel.ERROR
        assert len(details) == 3
    
    def test_resolve_all_only_warnings_count(self, resolver):
        """测试只有 is_warning=1 的行参与级别计算"""
        rows = [
            {"is_warning": 0, "status": "Critical", "alert_info": "Ignored"},  # is_warning=0
            {"is_warning": 1, "status": "AbnormalYellow", "alert_info": "Counted"},
        ]
        level, details = resolver.resolve_all(rows)
        
        # 第一行虽然 status=Critical，但 is_warning=0，所以 level 只是 INFO
        # 第二行 is_warning=1，status=AbnormalYellow -> WARNING
        # 最高级别应该是 WARNING，不是 CRITICAL
        assert level == AlertLevel.WARNING
    
    def test_apply_overrides_none(self, resolver):
        """测试无覆盖配置"""
        level = resolver.apply_overrides(AlertLevel.WARNING)
        assert level == AlertLevel.WARNING
    
    def test_apply_overrides_force_level(self, resolver):
        """测试强制级别覆盖"""
        level = resolver.apply_overrides(
            AlertLevel.ERROR,
            force_level=AlertLevel.INFO
        )
        assert level == AlertLevel.INFO
    
    def test_apply_overrides_max_level(self, resolver):
        """测试最大级别限制"""
        # ERROR 超过 max_level=WARNING，应降级为 WARNING
        level = resolver.apply_overrides(
            AlertLevel.ERROR,
            max_level=AlertLevel.WARNING
        )
        assert level == AlertLevel.WARNING
        
        # WARNING 没有超过 max_level=WARNING，保持不变
        level = resolver.apply_overrides(
            AlertLevel.WARNING,
            max_level=AlertLevel.WARNING
        )
        assert level == AlertLevel.WARNING
    
    def test_apply_overrides_force_takes_precedence(self, resolver):
        """测试 force_level 优先于 max_level"""
        level = resolver.apply_overrides(
            AlertLevel.WARNING,
            force_level=AlertLevel.CRITICAL,
            max_level=AlertLevel.INFO
        )
        # force_level 优先
        assert level == AlertLevel.CRITICAL
    
    def test_get_warning_rows(self, resolver):
        """测试获取告警行"""
        rows = [
            {"is_warning": 1, "status": "AbnormalYellow", "alert_info": "W1"},
            {"is_warning": 0, "status": "Normal", "alert_info": "OK"},
            {"is_warning": 1, "status": "AbnormalRed", "alert_info": "E1"},
        ]
        _, details = resolver.resolve_all(rows)
        
        warning_rows = resolver.get_warning_rows(details)
        assert len(warning_rows) == 2
        assert all(d.is_warning for d in warning_rows)
    
    def test_is_triggered(self, resolver):
        """测试是否触发告警判断"""
        # 有告警
        rows_with_warning = [
            {"is_warning": 0, "status": "Normal", "alert_info": "OK"},
            {"is_warning": 1, "status": "AbnormalYellow", "alert_info": "W1"},
        ]
        _, details = resolver.resolve_all(rows_with_warning)
        assert resolver.is_triggered(details) == True
        
        # 无告警
        rows_no_warning = [
            {"is_warning": 0, "status": "Normal", "alert_info": "OK"},
            {"is_warning": 0, "status": "Normal", "alert_info": "OK2"},
        ]
        _, details = resolver.resolve_all(rows_no_warning)
        assert resolver.is_triggered(details) == False

