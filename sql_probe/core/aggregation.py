"""
聚合条件处理

支持对 SQL 结果进行聚合后判断告警条件
"""

from typing import Any, Dict, List, Optional, Callable
from enum import Enum


class AggregationType(str, Enum):
    """聚合类型"""
    SUM = "sum"
    AVG = "avg"
    MAX = "max"
    MIN = "min"
    COUNT = "count"
    FIRST = "first"
    LAST = "last"


class Operator(str, Enum):
    """比较运算符"""
    GT = ">"       # 大于
    GTE = ">="     # 大于等于
    LT = "<"       # 小于
    LTE = "<="     # 小于等于
    EQ = "=="      # 等于
    NEQ = "!="     # 不等于


class AggregationCondition:
    """
    聚合条件
    
    用于对 SQL 结果的某一列进行聚合后，与阈值比较
    
    Usage:
        # 当 amount 列的总和 > 10000 时触发告警
        condition = AggregationCondition(
            column="amount",
            aggregation=AggregationType.SUM,
            operator=Operator.GT,
            threshold=10000
        )
        
        # 简写形式
        condition = AggregationCondition.sum("amount") > 10000
    """
    
    def __init__(
        self,
        column: str,
        aggregation: AggregationType = AggregationType.FIRST,
        operator: Operator = Operator.GT,
        threshold: float = 0
    ):
        self.column = column
        self.aggregation = aggregation
        self.operator = operator
        self.threshold = threshold
    
    def evaluate(self, rows: List[Dict[str, Any]]) -> tuple:
        """
        评估条件
        
        Args:
            rows: SQL 返回的行列表
            
        Returns:
            (triggered: bool, value: float, message: str)
        """
        if not rows:
            return False, 0, "无数据"
        
        # 提取列值
        values = []
        for row in rows:
            # 不区分大小写查找列
            row_lower = {k.lower(): v for k, v in row.items()}
            val = row_lower.get(self.column.lower())
            if val is not None:
                try:
                    values.append(float(val))
                except (ValueError, TypeError):
                    pass
        
        if not values:
            return False, 0, f"列 {self.column} 无有效数值"
        
        # 计算聚合值
        agg_value = self._aggregate(values)
        
        # 比较
        triggered = self._compare(agg_value)
        
        message = f"{self.column} 的 {self.aggregation.value} 值为 {agg_value:.2f}，{self.operator.value} {self.threshold} = {triggered}"
        
        return triggered, agg_value, message
    
    def _aggregate(self, values: List[float]) -> float:
        """计算聚合值"""
        if self.aggregation == AggregationType.SUM:
            return sum(values)
        elif self.aggregation == AggregationType.AVG:
            return sum(values) / len(values)
        elif self.aggregation == AggregationType.MAX:
            return max(values)
        elif self.aggregation == AggregationType.MIN:
            return min(values)
        elif self.aggregation == AggregationType.COUNT:
            return len(values)
        elif self.aggregation == AggregationType.FIRST:
            return values[0]
        elif self.aggregation == AggregationType.LAST:
            return values[-1]
        else:
            return values[0]
    
    def _compare(self, value: float) -> bool:
        """比较值与阈值"""
        if self.operator == Operator.GT:
            return value > self.threshold
        elif self.operator == Operator.GTE:
            return value >= self.threshold
        elif self.operator == Operator.LT:
            return value < self.threshold
        elif self.operator == Operator.LTE:
            return value <= self.threshold
        elif self.operator == Operator.EQ:
            return value == self.threshold
        elif self.operator == Operator.NEQ:
            return value != self.threshold
        else:
            return False
    
    # 便捷构造方法
    @classmethod
    def sum(cls, column: str) -> "AggregationConditionBuilder":
        return AggregationConditionBuilder(column, AggregationType.SUM)
    
    @classmethod
    def avg(cls, column: str) -> "AggregationConditionBuilder":
        return AggregationConditionBuilder(column, AggregationType.AVG)
    
    @classmethod
    def max(cls, column: str) -> "AggregationConditionBuilder":
        return AggregationConditionBuilder(column, AggregationType.MAX)
    
    @classmethod
    def min(cls, column: str) -> "AggregationConditionBuilder":
        return AggregationConditionBuilder(column, AggregationType.MIN)
    
    @classmethod
    def count(cls, column: str = "*") -> "AggregationConditionBuilder":
        return AggregationConditionBuilder(column, AggregationType.COUNT)
    
    @classmethod
    def first(cls, column: str) -> "AggregationConditionBuilder":
        return AggregationConditionBuilder(column, AggregationType.FIRST)


class AggregationConditionBuilder:
    """
    聚合条件构建器，支持链式调用
    
    Usage:
        condition = AggregationCondition.sum("amount") > 10000
        condition = AggregationCondition.avg("score") < 60
        condition = AggregationCondition.count("*") >= 100
    """
    
    def __init__(self, column: str, aggregation: AggregationType):
        self.column = column
        self.aggregation = aggregation
    
    def __gt__(self, threshold: float) -> AggregationCondition:
        return AggregationCondition(self.column, self.aggregation, Operator.GT, threshold)
    
    def __ge__(self, threshold: float) -> AggregationCondition:
        return AggregationCondition(self.column, self.aggregation, Operator.GTE, threshold)
    
    def __lt__(self, threshold: float) -> AggregationCondition:
        return AggregationCondition(self.column, self.aggregation, Operator.LT, threshold)
    
    def __le__(self, threshold: float) -> AggregationCondition:
        return AggregationCondition(self.column, self.aggregation, Operator.LTE, threshold)
    
    def __eq__(self, threshold: float) -> AggregationCondition:
        return AggregationCondition(self.column, self.aggregation, Operator.EQ, threshold)
    
    def __ne__(self, threshold: float) -> AggregationCondition:
        return AggregationCondition(self.column, self.aggregation, Operator.NEQ, threshold)


class MultiCondition:
    """
    多条件组合
    
    支持 AND 和 OR 逻辑组合多个条件
    
    Usage:
        # AND 组合
        condition = MultiCondition.all([
            AggregationCondition.sum("amount") > 10000,
            AggregationCondition.count("*") > 100
        ])
        
        # OR 组合
        condition = MultiCondition.any([
            AggregationCondition.max("error_count") > 0,
            AggregationCondition.avg("latency") > 1000
        ])
    """
    
    def __init__(self, conditions: List[AggregationCondition], logic: str = "all"):
        """
        Args:
            conditions: 条件列表
            logic: "all" (AND) 或 "any" (OR)
        """
        self.conditions = conditions
        self.logic = logic
    
    def evaluate(self, rows: List[Dict[str, Any]]) -> tuple:
        """
        评估所有条件
        
        Returns:
            (triggered: bool, details: List[tuple], message: str)
        """
        results = []
        for cond in self.conditions:
            triggered, value, msg = cond.evaluate(rows)
            results.append((triggered, value, msg))
        
        if self.logic == "all":
            final_triggered = all(r[0] for r in results)
            logic_word = "且"
        else:
            final_triggered = any(r[0] for r in results)
            logic_word = "或"
        
        messages = [r[2] for r in results]
        message = f" {logic_word} ".join(messages)
        
        return final_triggered, results, message
    
    @classmethod
    def all(cls, conditions: List[AggregationCondition]) -> "MultiCondition":
        """所有条件都满足（AND）"""
        return cls(conditions, "all")
    
    @classmethod
    def any(cls, conditions: List[AggregationCondition]) -> "MultiCondition":
        """任一条件满足（OR）"""
        return cls(conditions, "any")

