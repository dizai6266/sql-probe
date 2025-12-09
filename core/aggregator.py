"""
结果聚合器

负责将多行结果聚合为最终的 ProbeResult
"""

from datetime import datetime
from typing import List, Optional

from ..models.level import AlertLevel
from ..models.result import ProbeResult, RowDetail


class ResultAggregator:
    """
    结果聚合器
    
    将多行 SQL 结果聚合为单个 ProbeResult:
    1. 聚合告警内容
    2. 统计触发状态
    3. 生成可读的内容摘要
    """
    
    def aggregate(
        self,
        details: List[RowDetail],
        level: AlertLevel,
        execution_time: float,
        sql_text: str,
        default_alert_name: Optional[str] = None
    ) -> ProbeResult:
        """
        聚合单次执行结果
        
        Args:
            details: 各行的 RowDetail 列表
            level: 最终告警级别
            execution_time: 执行耗时（秒）
            sql_text: 执行的 SQL 文本
            default_alert_name: 默认告警名称（若 SQL 未返回）
            
        Returns:
            聚合后的 ProbeResult
        """
        # 判断是否触发告警
        triggered = any(d.is_warning for d in details)
        
        # 获取告警名称（优先用参数传入的，其次用 SQL 结果中的第一个）
        alert_name = default_alert_name
        if not alert_name and details:
            alert_name = details[0].alert_name
        alert_name = alert_name or "未命名告警"
        
        # 聚合告警内容
        content = self._aggregate_content(details)
        
        return ProbeResult(
            level=level,
            triggered=triggered,
            alert_name=alert_name,
            content=content,
            details=details,
            row_count=len(details),
            execution_time=execution_time,
            executed_at=datetime.now(),
            sql_text=sql_text,
            success=True
        )
    
    def _aggregate_content(self, details: List[RowDetail]) -> str:
        """
        聚合告警内容
        
        Args:
            details: 各行的 RowDetail 列表
            
        Returns:
            聚合后的内容字符串
        """
        warning_details = [d for d in details if d.is_warning]
        
        if not warning_details:
            return "所有检查项正常"
        
        if len(warning_details) == 1:
            d = warning_details[0]
            return f"[{d.status}] {d.alert_info}"
        
        # 多条告警，格式化为列表
        lines = [f"共 {len(warning_details)} 项告警:"]
        for i, d in enumerate(warning_details, 1):
            lines.append(f"  {i}. [{d.status}] {d.alert_info}")
        
        return "\n".join(lines)
    
    def aggregate_batch(
        self,
        results: List[ProbeResult],
        default_alert_name: str = "批量检查"
    ) -> ProbeResult:
        """
        聚合批量执行结果
        
        Args:
            results: 多个 ProbeResult 列表
            default_alert_name: 汇总结果的名称
            
        Returns:
            聚合后的 ProbeResult
        """
        if not results:
            return ProbeResult(
                level=AlertLevel.INFO,
                triggered=False,
                alert_name=default_alert_name,
                content="无检查项",
                row_count=0,
                execution_time=0
            )
        
        # 合并所有详情
        all_details = []
        total_time = 0.0
        for r in results:
            all_details.extend(r.details)
            total_time += r.execution_time
        
        # 取最高级别
        highest_level = max(r.level for r in results)
        triggered = any(r.triggered for r in results)
        
        # 聚合内容
        content = self._aggregate_batch_content(results)
        
        return ProbeResult(
            level=highest_level,
            triggered=triggered,
            alert_name=default_alert_name,
            content=content,
            details=all_details,
            row_count=len(all_details),
            execution_time=total_time,
            executed_at=datetime.now()
        )
    
    def _aggregate_batch_content(self, results: List[ProbeResult]) -> str:
        """
        聚合批量结果的内容
        
        Args:
            results: 多个 ProbeResult 列表
            
        Returns:
            聚合后的内容字符串
        """
        triggered_results = [r for r in results if r.triggered]
        
        if not triggered_results:
            return f"全部 {len(results)} 项检查通过"
        
        lines = [f"共 {len(triggered_results)}/{len(results)} 项触发告警:"]
        for r in triggered_results:
            # 截断过长的内容
            content_preview = r.content[:50] + "..." if len(r.content) > 50 else r.content
            lines.append(f"  • [{r.level.name}] {r.alert_name}: {content_preview}")
        
        return "\n".join(lines)
    
    def create_error_result(
        self,
        error_message: str,
        sql_text: str = "",
        alert_name: str = "执行失败",
        execution_time: float = 0.0
    ) -> ProbeResult:
        """
        创建执行失败的结果对象
        
        Args:
            error_message: 错误信息
            sql_text: SQL 文本
            alert_name: 告警名称
            execution_time: 执行耗时
            
        Returns:
            表示失败的 ProbeResult
        """
        return ProbeResult(
            level=AlertLevel.ERROR,
            triggered=True,
            alert_name=alert_name,
            content=f"执行失败: {error_message}",
            details=[],
            row_count=0,
            execution_time=execution_time,
            executed_at=datetime.now(),
            sql_text=sql_text,
            success=False,
            error_message=error_message
        )

