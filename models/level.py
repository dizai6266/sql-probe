"""
告警级别枚举

定义 SQL-Probe 的告警级别体系，与 SQL status 字段和 feishu-notify 的 NotifyLevel 对应
"""

from enum import IntEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from feishu_notify.core.types import NotifyLevel


class AlertLevel(IntEnum):
    """
    告警级别枚举
    
    数值越大表示越严重，支持比较运算
    
    级别体系:
        - DEBUG (0): 调试信息，不通知
        - INFO (10): 正常状态，对应 SQL status='Normal'
        - WARNING (20): 警告，对应 SQL status='AbnormalYellow'
        - ERROR (30): 错误，对应 SQL status='AbnormalRed'
        - CRITICAL (40): 严重/紧急，对应 SQL status='Critical'
    """
    DEBUG = 0
    INFO = 10
    WARNING = 20
    ERROR = 30
    CRITICAL = 40
    
    def __str__(self) -> str:
        return self.name
    
    @classmethod
    def from_status(cls, status: str) -> "AlertLevel":
        """
        从 SQL status 字段映射到告警级别
        
        Args:
            status: SQL 返回的 status 字段值
            
        Returns:
            对应的 AlertLevel
            
        Mapping (兼容 Databricks 原生 Alert 的所有状态):
            - 'Normal', 'NormalGreen', 'Ok' -> INFO
            - 'AbnormalYellow', 'Warning', 'Yellow' -> WARNING
            - 'AbnormalRed', 'Error', 'Red' -> ERROR
            - 'Critical', 'Urgent' -> CRITICAL
        """
        status_lower = status.lower().strip()
        
        # INFO 级别的状态
        if status_lower in ("normal", "normalgreen", "ok", "info", "green"):
            return cls.INFO
        
        # WARNING 级别的状态
        if status_lower in ("abnormalyellow", "warning", "yellow", "warn"):
            return cls.WARNING
        
        # ERROR 级别的状态
        if status_lower in ("abnormalred", "error", "red", "err"):
            return cls.ERROR
        
        # CRITICAL 级别的状态
        if status_lower in ("critical", "urgent", "fatal"):
            return cls.CRITICAL
        
        # 默认返回 INFO
        return cls.INFO
    
    @classmethod
    def from_is_warning(cls, is_warning: int, status: str = "Normal") -> "AlertLevel":
        """
        根据 is_warning 和 status 推断级别
        
        Args:
            is_warning: SQL 返回的 is_warning 字段（0 或 1）
            status: SQL 返回的 status 字段
            
        Returns:
            推断的 AlertLevel
        """
        if is_warning != 1:
            return cls.INFO
        return cls.from_status(status)
    
    def should_notify(self) -> bool:
        """是否需要发送通知"""
        return self >= AlertLevel.WARNING
    
    def should_interrupt(self, threshold: "AlertLevel" = None) -> bool:
        """
        是否需要中断执行
        
        Args:
            threshold: 中断阈值，默认为 ERROR
        """
        if threshold is None:
            threshold = AlertLevel.ERROR
        return self >= threshold
    
    def to_notify_level(self) -> "NotifyLevel":
        """
        转换为 feishu-notify 的 NotifyLevel
        
        Returns:
            对应的 NotifyLevel 枚举值
        """
        # 延迟导入避免循环依赖
        try:
            from feishu_notify.core.types import NotifyLevel
        except ImportError:
            # 如果没有安装 feishu-notify，返回字符串
            return self.name.lower()
        
        mapping = {
            AlertLevel.DEBUG: NotifyLevel.INFO,
            AlertLevel.INFO: NotifyLevel.INFO,
            AlertLevel.WARNING: NotifyLevel.WARNING,
            AlertLevel.ERROR: NotifyLevel.ERROR,
            AlertLevel.CRITICAL: NotifyLevel.CRITICAL,
        }
        return mapping.get(self, NotifyLevel.INFO)
    
    @property
    def emoji(self) -> str:
        """获取级别对应的 Emoji"""
        emojis = {
            AlertLevel.DEBUG: "🔍",
            AlertLevel.INFO: "ℹ️",
            AlertLevel.WARNING: "⚠️",
            AlertLevel.ERROR: "❌",
            AlertLevel.CRITICAL: "🚨",
        }
        return emojis.get(self, "ℹ️")
    
    @property
    def color(self) -> str:
        """获取级别对应的颜色（用于飞书卡片）"""
        colors = {
            AlertLevel.DEBUG: "grey",
            AlertLevel.INFO: "blue",
            AlertLevel.WARNING: "yellow",
            AlertLevel.ERROR: "orange",
            AlertLevel.CRITICAL: "red",
        }
        return colors.get(self, "blue")


# 状态映射表（便于外部直接使用，兼容 Databricks 原生 Alert 所有状态）
STATUS_MAP = {
    # INFO 级别
    "Normal": AlertLevel.INFO,
    "NormalGreen": AlertLevel.INFO,
    "Ok": AlertLevel.INFO,
    "Info": AlertLevel.INFO,
    "Green": AlertLevel.INFO,
    # WARNING 级别
    "AbnormalYellow": AlertLevel.WARNING,
    "Warning": AlertLevel.WARNING,
    "Yellow": AlertLevel.WARNING,
    "Warn": AlertLevel.WARNING,
    # ERROR 级别
    "AbnormalRed": AlertLevel.ERROR,
    "Error": AlertLevel.ERROR,
    "Red": AlertLevel.ERROR,
    "Err": AlertLevel.ERROR,
    # CRITICAL 级别
    "Critical": AlertLevel.CRITICAL,
    "Urgent": AlertLevel.CRITICAL,
    "Fatal": AlertLevel.CRITICAL,
}

