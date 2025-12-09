"""
级别解析器

负责从 SQL 结果中推断告警级别
"""

from typing import Any, Dict, List, Tuple, Optional

from ..models.level import AlertLevel
from ..models.result import RowDetail


class LevelResolver:
    """
    级别解析器
    
    从 SQL 结果推断告警级别:
    1. 解析每行的 is_warning 和 status 字段
    2. 只有 is_warning=1 的行才参与级别计算
    3. 取所有告警行中的最高级别作为最终级别
    """
    
    def resolve_row(self, row: Dict[str, Any]) -> RowDetail:
        """
        解析单行结果
        
        Args:
            row: SQL 返回的单行数据（字典）
            
        Returns:
            解析后的 RowDetail 对象
            
        兼容性:
            - 如果缺少 status 列，根据 is_warning 自动推断:
              - is_warning=0 → 'Normal' (INFO)
              - is_warning=1 → 'AbnormalRed' (ERROR)
        """
        # 提取字段（不区分大小写）
        row_lower = {k.lower(): v for k, v in row.items()}
        
        is_warning_raw = row_lower.get("is_warning", 0)
        alert_info = str(row_lower.get("alert_info", ""))
        alert_name = str(row_lower.get("alert_name", "未命名告警"))
        
        # 解析 is_warning（支持 int、str、bool）
        try:
            is_warning = bool(int(is_warning_raw))
        except (ValueError, TypeError):
            is_warning = bool(is_warning_raw)
        
        # 处理 status 字段（兼容缺失情况）
        if "status" in row_lower and row_lower["status"] is not None:
            status = str(row_lower["status"]).strip()
        else:
            # status 缺失时根据 is_warning 推断默认值
            status = "AbnormalRed" if is_warning else "Normal"
        
        # 推断级别
        if not is_warning:
            # is_warning=0 强制为 INFO，无论 status 是什么
            level = AlertLevel.INFO
        else:
            # is_warning=1 时根据 status 映射级别
            level = AlertLevel.from_status(status)
        
        return RowDetail(
            alert_name=alert_name,
            is_warning=is_warning,
            alert_info=alert_info,
            status=status,
            level=level,
            raw_data=row
        )
    
    def resolve_all(self, rows: List[Dict[str, Any]]) -> Tuple[AlertLevel, List[RowDetail]]:
        """
        解析所有行，返回最高级别和详情列表
        
        Args:
            rows: SQL 返回的所有行
            
        Returns:
            (highest_level, details) 元组
            - highest_level: 所有行中的最高告警级别
            - details: 每行的 RowDetail 列表
        """
        if not rows:
            return AlertLevel.INFO, []
        
        details = [self.resolve_row(row) for row in rows]
        
        # 取所有行中的最高级别
        highest_level = max(d.level for d in details)
        
        return highest_level, details
    
    def apply_overrides(
        self,
        level: AlertLevel,
        force_level: Optional[AlertLevel] = None,
        max_level: Optional[AlertLevel] = None
    ) -> AlertLevel:
        """
        应用级别覆盖配置
        
        Args:
            level: 原始推断的级别
            force_level: 强制覆盖为指定级别
            max_level: 最大级别上限（用于测试环境降级）
            
        Returns:
            最终级别
        """
        # force_level 优先级最高
        if force_level is not None:
            return force_level
        
        # max_level 限制上限
        if max_level is not None and level > max_level:
            return max_level
        
        return level
    
    def get_warning_rows(self, details: List[RowDetail]) -> List[RowDetail]:
        """
        获取所有触发告警的行
        
        Args:
            details: 所有行的详情列表
            
        Returns:
            is_warning=True 的行列表
        """
        return [d for d in details if d.is_warning]
    
    def is_triggered(self, details: List[RowDetail]) -> bool:
        """
        判断是否有任何行触发告警
        
        Args:
            details: 所有行的详情列表
            
        Returns:
            是否存在 is_warning=True 的行
        """
        return any(d.is_warning for d in details)

