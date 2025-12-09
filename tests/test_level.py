"""
AlertLevel 枚举测试
"""

import pytest
from sql_probe.models.level import AlertLevel, STATUS_MAP


class TestAlertLevel:
    """AlertLevel 枚举测试"""
    
    def test_level_ordering(self):
        """测试级别大小比较"""
        assert AlertLevel.DEBUG < AlertLevel.INFO
        assert AlertLevel.INFO < AlertLevel.WARNING
        assert AlertLevel.WARNING < AlertLevel.ERROR
        assert AlertLevel.ERROR < AlertLevel.CRITICAL
    
    def test_from_status_normal(self):
        """测试 Normal 状态映射"""
        assert AlertLevel.from_status("Normal") == AlertLevel.INFO
        assert AlertLevel.from_status("normal") == AlertLevel.INFO
        assert AlertLevel.from_status("NORMAL") == AlertLevel.INFO
    
    def test_from_status_warning(self):
        """测试 AbnormalYellow 状态映射"""
        assert AlertLevel.from_status("AbnormalYellow") == AlertLevel.WARNING
        assert AlertLevel.from_status("abnormalyellow") == AlertLevel.WARNING
    
    def test_from_status_error(self):
        """测试 AbnormalRed 状态映射"""
        assert AlertLevel.from_status("AbnormalRed") == AlertLevel.ERROR
        assert AlertLevel.from_status("abnormalred") == AlertLevel.ERROR
    
    def test_from_status_critical(self):
        """测试 Critical 状态映射"""
        assert AlertLevel.from_status("Critical") == AlertLevel.CRITICAL
        assert AlertLevel.from_status("critical") == AlertLevel.CRITICAL
    
    def test_from_status_unknown(self):
        """测试未知状态默认为 INFO"""
        assert AlertLevel.from_status("Unknown") == AlertLevel.INFO
        assert AlertLevel.from_status("") == AlertLevel.INFO
        assert AlertLevel.from_status("random") == AlertLevel.INFO
    
    def test_from_is_warning_zero(self):
        """测试 is_warning=0 时返回 INFO"""
        assert AlertLevel.from_is_warning(0, "AbnormalRed") == AlertLevel.INFO
        assert AlertLevel.from_is_warning(0, "Critical") == AlertLevel.INFO
    
    def test_from_is_warning_one(self):
        """测试 is_warning=1 时根据 status 映射"""
        assert AlertLevel.from_is_warning(1, "Normal") == AlertLevel.INFO
        assert AlertLevel.from_is_warning(1, "AbnormalYellow") == AlertLevel.WARNING
        assert AlertLevel.from_is_warning(1, "AbnormalRed") == AlertLevel.ERROR
        assert AlertLevel.from_is_warning(1, "Critical") == AlertLevel.CRITICAL
    
    def test_should_notify(self):
        """测试是否需要通知"""
        assert AlertLevel.DEBUG.should_notify() == False
        assert AlertLevel.INFO.should_notify() == False
        assert AlertLevel.WARNING.should_notify() == True
        assert AlertLevel.ERROR.should_notify() == True
        assert AlertLevel.CRITICAL.should_notify() == True
    
    def test_should_interrupt_default(self):
        """测试默认中断阈值（ERROR）"""
        assert AlertLevel.DEBUG.should_interrupt() == False
        assert AlertLevel.INFO.should_interrupt() == False
        assert AlertLevel.WARNING.should_interrupt() == False
        assert AlertLevel.ERROR.should_interrupt() == True
        assert AlertLevel.CRITICAL.should_interrupt() == True
    
    def test_should_interrupt_custom_threshold(self):
        """测试自定义中断阈值"""
        # 阈值设为 WARNING
        assert AlertLevel.INFO.should_interrupt(AlertLevel.WARNING) == False
        assert AlertLevel.WARNING.should_interrupt(AlertLevel.WARNING) == True
        assert AlertLevel.ERROR.should_interrupt(AlertLevel.WARNING) == True
    
    def test_emoji(self):
        """测试 Emoji 映射"""
        assert AlertLevel.DEBUG.emoji == "🔍"
        assert AlertLevel.INFO.emoji == "ℹ️"
        assert AlertLevel.WARNING.emoji == "⚠️"
        assert AlertLevel.ERROR.emoji == "❌"
        assert AlertLevel.CRITICAL.emoji == "🚨"
    
    def test_color(self):
        """测试颜色映射"""
        assert AlertLevel.DEBUG.color == "grey"
        assert AlertLevel.INFO.color == "blue"
        assert AlertLevel.WARNING.color == "yellow"
        assert AlertLevel.ERROR.color == "orange"
        assert AlertLevel.CRITICAL.color == "red"
    
    def test_str(self):
        """测试字符串转换"""
        assert str(AlertLevel.INFO) == "INFO"
        assert str(AlertLevel.WARNING) == "WARNING"
        assert str(AlertLevel.ERROR) == "ERROR"


class TestStatusMap:
    """STATUS_MAP 映射表测试"""
    
    def test_status_map_contents(self):
        """测试映射表内容"""
        assert STATUS_MAP["Normal"] == AlertLevel.INFO
        assert STATUS_MAP["AbnormalYellow"] == AlertLevel.WARNING
        assert STATUS_MAP["AbnormalRed"] == AlertLevel.ERROR
        assert STATUS_MAP["Critical"] == AlertLevel.CRITICAL
    
    def test_status_map_length(self):
        """测试映射表长度"""
        assert len(STATUS_MAP) == 4

