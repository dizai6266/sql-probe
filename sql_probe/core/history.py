"""
告警历史记录

存储和查询告警执行历史，支持变化率检测
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field, asdict
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class AlertRecord:
    """告警记录"""
    alert_name: str
    level: str
    triggered: bool
    value: Optional[float]  # 主要指标值（用于变化率计算）
    content: str
    row_count: int
    execution_time: float
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            'timestamp': self.timestamp.isoformat(),
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AlertRecord":
        data = data.copy()
        if isinstance(data.get('timestamp'), str):
            data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        return cls(**data)


class AlertHistory:
    """
    告警历史管理器
    
    功能：
    1. 存储告警执行历史
    2. 查询历史记录
    3. 计算指标变化率
    4. 检测异常趋势
    
    Usage:
        history = AlertHistory(max_records=1000)
        
        # 记录
        history.record(result)
        
        # 查询
        records = history.get("数据质量检查", limit=10)
        
        # 变化率
        change = history.get_change_rate("DAU监控")
    """
    
    def __init__(self, max_records: int = 1000):
        """
        Args:
            max_records: 每个告警保留的最大记录数
        """
        self.max_records = max_records
        self._history: Dict[str, List[AlertRecord]] = defaultdict(list)
    
    def record(self, result: Any, value: Optional[float] = None) -> None:
        """
        记录一次告警执行结果
        
        Args:
            result: ProbeResult 对象
            value: 主要指标值（可选，用于变化率计算）
        """
        # 尝试从 result 提取 value
        if value is None and result.details:
            # 尝试从第一行提取数值
            first_detail = result.details[0]
            raw_data = first_detail.raw_data
            # 查找可能的数值列
            for key, val in raw_data.items():
                if key.lower() not in ('alert_name', 'is_warning', 'alert_info', 'status'):
                    try:
                        value = float(val)
                        break
                    except (ValueError, TypeError):
                        pass
        
        record = AlertRecord(
            alert_name=result.alert_name,
            level=result.level.name,
            triggered=result.triggered,
            value=value,
            content=result.content[:200],  # 截断过长内容
            row_count=result.row_count,
            execution_time=result.execution_time,
            timestamp=result.executed_at
        )
        
        # 添加记录
        self._history[result.alert_name].append(record)
        
        # 限制数量
        if len(self._history[result.alert_name]) > self.max_records:
            self._history[result.alert_name] = self._history[result.alert_name][-self.max_records:]
    
    def get(
        self,
        alert_name: str,
        limit: int = 10,
        since: Optional[datetime] = None
    ) -> List[AlertRecord]:
        """
        获取告警历史记录
        
        Args:
            alert_name: 告警名称
            limit: 返回数量限制
            since: 起始时间
            
        Returns:
            记录列表（最新的在前）
        """
        records = self._history.get(alert_name, [])
        
        if since:
            records = [r for r in records if r.timestamp >= since]
        
        # 按时间倒序
        records = sorted(records, key=lambda r: r.timestamp, reverse=True)
        
        return records[:limit]
    
    def get_all_names(self) -> List[str]:
        """获取所有告警名称"""
        return list(self._history.keys())
    
    def get_change_rate(
        self,
        alert_name: str,
        periods: int = 2
    ) -> Optional[Dict[str, Any]]:
        """
        计算指标变化率
        
        Args:
            alert_name: 告警名称
            periods: 比较的周期数（默认比较最近2次）
            
        Returns:
            {
                "current": float,      # 当前值
                "previous": float,     # 上一次值
                "change": float,       # 变化量
                "change_rate": float,  # 变化率 (%)
                "trend": str           # "up" / "down" / "stable"
            }
        """
        records = self.get(alert_name, limit=periods)
        
        if len(records) < 2:
            return None
        
        # 获取有效的数值记录
        values = [r.value for r in records if r.value is not None]
        
        if len(values) < 2:
            return None
        
        current = values[0]  # 最新的
        previous = values[1]  # 上一次的
        
        change = current - previous
        
        if previous != 0:
            change_rate = (change / abs(previous)) * 100
        else:
            change_rate = 100.0 if change > 0 else 0.0
        
        if abs(change_rate) < 1:
            trend = "stable"
        elif change_rate > 0:
            trend = "up"
        else:
            trend = "down"
        
        return {
            "current": current,
            "previous": previous,
            "change": change,
            "change_rate": round(change_rate, 2),
            "trend": trend,
        }
    
    def detect_anomaly(
        self,
        alert_name: str,
        threshold_rate: float = 50.0,
        min_records: int = 3
    ) -> Optional[Dict[str, Any]]:
        """
        检测异常变化
        
        Args:
            alert_name: 告警名称
            threshold_rate: 变化率阈值 (%)
            min_records: 最少需要的历史记录数
            
        Returns:
            {
                "is_anomaly": bool,
                "change_rate": float,
                "message": str
            }
        """
        records = self.get(alert_name, limit=min_records)
        
        if len(records) < min_records:
            return {
                "is_anomaly": False,
                "change_rate": 0,
                "message": f"历史记录不足（需要至少 {min_records} 条）"
            }
        
        change = self.get_change_rate(alert_name)
        
        if change is None:
            return {
                "is_anomaly": False,
                "change_rate": 0,
                "message": "无法计算变化率"
            }
        
        is_anomaly = abs(change["change_rate"]) >= threshold_rate
        
        if is_anomaly:
            direction = "上升" if change["change_rate"] > 0 else "下降"
            message = f"检测到异常！{alert_name} {direction}了 {abs(change['change_rate']):.1f}%"
        else:
            message = f"{alert_name} 变化率 {change['change_rate']:.1f}%，在正常范围内"
        
        return {
            "is_anomaly": is_anomaly,
            "change_rate": change["change_rate"],
            "trend": change["trend"],
            "current": change["current"],
            "previous": change["previous"],
            "message": message
        }
    
    def get_statistics(
        self,
        alert_name: str,
        days: int = 7
    ) -> Dict[str, Any]:
        """
        获取统计信息
        
        Args:
            alert_name: 告警名称
            days: 统计天数
            
        Returns:
            统计信息字典
        """
        since = datetime.now() - timedelta(days=days)
        records = self.get(alert_name, limit=1000, since=since)
        
        if not records:
            return {
                "total": 0,
                "triggered_count": 0,
                "trigger_rate": 0,
                "avg_execution_time": 0,
            }
        
        triggered_count = sum(1 for r in records if r.triggered)
        values = [r.value for r in records if r.value is not None]
        exec_times = [r.execution_time for r in records]
        
        stats = {
            "total": len(records),
            "triggered_count": triggered_count,
            "trigger_rate": round(triggered_count / len(records) * 100, 2),
            "avg_execution_time": round(sum(exec_times) / len(exec_times), 2),
        }
        
        if values:
            stats.update({
                "value_avg": round(sum(values) / len(values), 2),
                "value_max": max(values),
                "value_min": min(values),
            })
        
        return stats
    
    def clear(self, alert_name: Optional[str] = None) -> None:
        """
        清除历史记录
        
        Args:
            alert_name: 告警名称（为空则清除全部）
        """
        if alert_name:
            self._history.pop(alert_name, None)
        else:
            self._history.clear()
    
    def export(self) -> Dict[str, List[Dict]]:
        """导出所有历史记录"""
        return {
            name: [r.to_dict() for r in records]
            for name, records in self._history.items()
        }
    
    def import_data(self, data: Dict[str, List[Dict]]) -> None:
        """导入历史记录"""
        for name, records in data.items():
            self._history[name] = [AlertRecord.from_dict(r) for r in records]

