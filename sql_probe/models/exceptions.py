"""
SQL-Probe 异常类型

定义探针执行过程中可能抛出的各种异常
"""

from typing import Optional, List, TYPE_CHECKING

if TYPE_CHECKING:
    from .result import ProbeResult


class ProbeError(Exception):
    """
    探针基础异常
    
    所有 SQL-Probe 异常的基类
    """
    pass


class ProbeInterruptError(ProbeError):
    """
    探针中断异常
    
    当告警级别达到中断阈值时抛出，用于中断 Notebook/流程执行
    携带 ProbeResult 以便调用方获取详细信息
    
    Usage:
        try:
            result = probe.execute(sql)
        except ProbeInterruptError as e:
            print(f"中断原因: {e.result.content}")
            print(f"告警级别: {e.result.level}")
            # 可选: raise 继续中断，或 pass 忽略
    """
    
    def __init__(self, message: str, result: Optional["ProbeResult"] = None):
        """
        初始化中断异常
        
        Args:
            message: 异常消息
            result: 探针执行结果
        """
        super().__init__(message)
        self.result = result
    
    def __str__(self) -> str:
        base = super().__str__()
        if self.result:
            return f"{base} [Level: {self.result.level.name}, Alert: {self.result.alert_name}]"
        return base
    
    def __repr__(self) -> str:
        return f"ProbeInterruptError('{self}', result={self.result!r})"


class SQLExecutionError(ProbeError):
    """
    SQL 执行错误
    
    当 SQL 执行失败时抛出（语法错误、表不存在、权限问题等）
    """
    
    def __init__(self, message: str, sql: str = "", original_error: Exception = None):
        """
        初始化 SQL 执行错误
        
        Args:
            message: 错误消息
            sql: 执行的 SQL 文本
            original_error: 原始异常
        """
        super().__init__(message)
        self.sql = sql
        self.original_error = original_error
    
    def __str__(self) -> str:
        base = super().__str__()
        if self.sql:
            # 截断过长的 SQL
            sql_preview = self.sql[:200] + "..." if len(self.sql) > 200 else self.sql
            return f"{base}\nSQL: {sql_preview}"
        return base


class SQLValidationError(ProbeError):
    """
    SQL 结果验证错误
    
    当 SQL 返回结果不符合规范时抛出（缺少必需列等）
    """
    
    def __init__(self, message: str, missing_columns: List[str] = None, actual_columns: List[str] = None):
        """
        初始化验证错误
        
        Args:
            message: 错误消息
            missing_columns: 缺失的列名列表
            actual_columns: 实际返回的列名列表
        """
        super().__init__(message)
        self.missing_columns = missing_columns or []
        self.actual_columns = actual_columns or []
    
    def __str__(self) -> str:
        base = super().__str__()
        parts = [base]
        if self.missing_columns:
            parts.append(f"缺失列: {self.missing_columns}")
        if self.actual_columns:
            parts.append(f"实际列: {self.actual_columns}")
        return "\n".join(parts)


class NotificationError(ProbeError):
    """
    通知发送错误
    
    当飞书通知发送失败时抛出
    注意: 通常不会阻断主流程，仅记录日志
    """
    
    def __init__(self, message: str, original_error: Exception = None):
        """
        初始化通知错误
        
        Args:
            message: 错误消息
            original_error: 原始异常
        """
        super().__init__(message)
        self.original_error = original_error

