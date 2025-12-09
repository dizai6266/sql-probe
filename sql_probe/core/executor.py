"""
SQL 执行器

负责执行 SQL 并验证返回结果的格式
"""

import time
import logging
from typing import Any, Dict, List, Tuple, Set, Optional

from ..models.exceptions import SQLExecutionError, SQLValidationError

logger = logging.getLogger(__name__)


class SQLExecutor:
    """
    SQL 执行器
    
    负责:
    1. 执行 SQL 查询
    2. 验证返回结果包含必需列
    3. 将结果转换为字典列表
    
    Attributes:
        REQUIRED_COLUMNS: 必需的列名集合
        OPTIONAL_COLUMNS: 可选的列名集合（如果缺失会使用默认值）
        
    兼容性说明:
        - 为兼容 Databricks 原生 Alert SQL，`status` 列是可选的
        - 如果缺少 `status` 列，将根据 `is_warning` 自动推断:
          - is_warning=0 → status='Normal' (INFO 级别)
          - is_warning=1 → status='AbnormalRed' (ERROR 级别)
    """
    
    # SQL 结果必须包含的列（最小集合）
    REQUIRED_COLUMNS: Set[str] = {"is_warning", "alert_info"}
    
    # 可选列（可从参数传入或 SQL 返回，缺失时使用默认值）
    OPTIONAL_COLUMNS: Set[str] = {"alert_name", "status"}
    
    def __init__(self, spark, timeout: int = 300):
        """
        初始化执行器
        
        Args:
            spark: SparkSession 实例
            timeout: SQL 执行超时时间（秒）
        """
        self.spark = spark
        self.timeout = timeout
    
    def execute(self, sql: str, skip_validation: bool = False) -> Tuple[List[Dict[str, Any]], float]:
        """
        执行 SQL 并返回结果
        
        Args:
            sql: 要执行的 SQL 文本
            skip_validation: 是否跳过必需列验证（用于聚合条件检查等场景）
            
        Returns:
            (rows, execution_time) 元组
            - rows: 结果行列表，每行为字典
            - execution_time: 执行耗时（秒）
            
        Raises:
            SQLExecutionError: SQL 执行失败
            SQLValidationError: 结果格式不符合规范（skip_validation=False 时）
        """
        start_time = time.time()
        
        try:
            # 执行 SQL
            logger.debug(f"执行 SQL: {sql[:200]}...")
            df = self.spark.sql(sql)
            
            # 验证列（除非跳过验证）
            if not skip_validation:
                self._validate_columns(df, sql)
            
            # 收集结果（期望结果行数较少）
            rows = [row.asDict() for row in df.collect()]
            
            execution_time = time.time() - start_time
            logger.debug(f"SQL 执行完成，返回 {len(rows)} 行，耗时 {execution_time:.2f}s")
            
            return rows, execution_time
            
        except SQLValidationError:
            # 验证错误直接抛出
            raise
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"SQL 执行失败: {e}")
            raise SQLExecutionError(
                f"SQL 执行失败: {str(e)}",
                sql=sql,
                original_error=e
            )
    
    def _validate_columns(self, df, sql: str) -> None:
        """
        验证 DataFrame 包含必需列
        
        Args:
            df: Spark DataFrame
            sql: 原始 SQL（用于错误信息）
            
        Raises:
            SQLValidationError: 缺少必需列
        """
        # 获取实际列名（转小写以进行不区分大小写的比较）
        actual_columns = set(c.lower() for c in df.columns)
        required_lower = set(c.lower() for c in self.REQUIRED_COLUMNS)
        
        missing = required_lower - actual_columns
        
        if missing:
            raise SQLValidationError(
                f"SQL 结果缺少必需列: {missing}",
                missing_columns=list(missing),
                actual_columns=list(df.columns)
            )
    
    def validate(self, sql: str) -> Dict[str, Any]:
        """
        验证 SQL 格式（Dry Run）
        
        使用 LIMIT 0 只获取 schema，不执行完整查询
        
        Args:
            sql: 要验证的 SQL 文本
            
        Returns:
            验证结果字典:
            {
                "valid": bool,
                "columns": List[str],
                "error": Optional[str]
            }
        """
        try:
            # 使用 LIMIT 0 只获取 schema
            # 注意：末尾加换行符，避免行注释（--）把括号也注释掉
            cleaned_sql = sql.strip().rstrip(';')
            wrapped_sql = f"SELECT * FROM ({cleaned_sql}\n) t LIMIT 0"
            df = self.spark.sql(wrapped_sql)
            columns = list(df.columns)
            
            # 检查必需列
            actual_lower = set(c.lower() for c in columns)
            required_lower = set(c.lower() for c in self.REQUIRED_COLUMNS)
            missing = required_lower - actual_lower
            
            if missing:
                return {
                    "valid": False,
                    "columns": columns,
                    "error": f"缺少必需列: {list(missing)}"
                }
            
            return {
                "valid": True,
                "columns": columns,
                "error": None
            }
            
        except Exception as e:
            logger.warning(f"SQL 验证失败: {e}")
            return {
                "valid": False,
                "columns": [],
                "error": str(e)
            }
    
    def get_required_columns(self) -> Set[str]:
        """获取必需列集合"""
        return self.REQUIRED_COLUMNS.copy()
    
    def get_all_columns(self) -> Set[str]:
        """获取所有支持的列（必需 + 可选）"""
        return self.REQUIRED_COLUMNS | self.OPTIONAL_COLUMNS

