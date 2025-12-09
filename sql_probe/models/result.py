"""
探针执行结果对象

定义 ProbeResult 和 RowDetail 数据类
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional

from .level import AlertLevel


@dataclass
class RowDetail:
    """
    单行结果详情
    
    存储 SQL 返回的每一行数据及其解析后的级别
    """
    alert_name: str
    is_warning: bool
    alert_info: str
    status: str
    level: AlertLevel
    raw_data: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "alert_name": self.alert_name,
            "is_warning": self.is_warning,
            "alert_info": self.alert_info,
            "status": self.status,
            "level": self.level.name,
            "raw_data": self.raw_data,
        }


@dataclass
class ProbeResult:
    """
    探针执行结果
    
    包含告警级别、触发状态、内容摘要、执行详情等信息
    支持链式判断和序列化
    
    Attributes:
        level: 最终告警级别（取所有行中的最高级别）
        triggered: 是否触发告警（存在 is_warning=1 的行）
        alert_name: 告警名称
        content: 聚合后的告警内容摘要
        details: 各行的详细信息
        row_count: 返回行数
        execution_time: SQL 执行耗时（秒）
        executed_at: 执行时间
        sql_text: 执行的 SQL 文本
        success: 是否执行成功
        error_message: 错误信息（执行失败时）
    """
    
    # 核心属性
    level: AlertLevel
    triggered: bool  # 是否触发告警 (is_warning=1 的行存在)
    
    # 内容
    alert_name: str
    content: str  # 聚合后的告警内容
    details: List[RowDetail] = field(default_factory=list)
    
    # 元信息
    row_count: int = 0
    execution_time: float = 0.0  # 秒
    executed_at: datetime = field(default_factory=datetime.now)
    sql_text: str = ""
    
    # 状态
    success: bool = True
    error_message: Optional[str] = None
    
    def __bool__(self) -> bool:
        """允许 if result: 判断是否有告警"""
        return self.triggered
    
    @property
    def is_critical(self) -> bool:
        """是否为 CRITICAL 级别"""
        return self.level >= AlertLevel.CRITICAL
    
    @property
    def is_error(self) -> bool:
        """是否为 ERROR 或更高级别"""
        return self.level >= AlertLevel.ERROR
    
    @property
    def is_warning(self) -> bool:
        """是否为 WARNING 或更高级别"""
        return self.level >= AlertLevel.WARNING
    
    @property
    def warning_rows(self) -> List[RowDetail]:
        """获取所有告警行（is_warning=1）"""
        return [d for d in self.details if d.is_warning]
    
    @property
    def formatted_timestamp(self) -> str:
        """获取格式化的执行时间"""
        return self.executed_at.strftime("%Y-%m-%d %H:%M:%S")
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典（便于序列化）"""
        return {
            "level": self.level.name,
            "triggered": self.triggered,
            "alert_name": self.alert_name,
            "content": self.content,
            "details": [d.to_dict() for d in self.details],
            "row_count": self.row_count,
            "execution_time": self.execution_time,
            "executed_at": self.formatted_timestamp,
            "sql_text": self.sql_text,
            "success": self.success,
            "error_message": self.error_message,
        }
    
    def summary(self) -> str:
        """生成结果摘要"""
        lines = [
            f"告警名称: {self.alert_name}",
            f"告警级别: {self.level.emoji} {self.level.name}",
            f"是否触发: {'是' if self.triggered else '否'}",
            f"返回行数: {self.row_count}",
            f"执行耗时: {self.execution_time:.2f}s",
        ]
        if self.triggered:
            lines.append(f"告警内容: {self.content}")
        if not self.success:
            lines.append(f"错误信息: {self.error_message}")
        return "\n".join(lines)
    
    def __repr__(self) -> str:
        return (
            f"ProbeResult(level={self.level.name}, triggered={self.triggered}, "
            f"alert_name='{self.alert_name}', row_count={self.row_count})"
        )

