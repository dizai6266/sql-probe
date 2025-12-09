"""
SQL-Probe 通知器

调用 feishu-notify 发送通知，提供 SQL 执行、级别推断、通知发送和流程中断功能

Webhook 配置（支持多个飞书群）:
    通过 channel 参数按需配置不同的飞书群:
    
    命名规则:
    - channel="xxx" → Secrets Key: webhook-xxx / 环境变量: FEISHU_WEBHOOK_XXX
    - channel="default" 是特殊情况，环境变量为 FEISHU_WEBHOOK（无后缀）

配置优先级: 显式参数 > Databricks Secrets > 环境变量 > 回退到 default
"""

import logging
import os
import sys
from typing import Any, Dict, List, Optional, Union

from .models.level import AlertLevel
from .models.result import ProbeResult
from .models.exceptions import (
    ProbeError,
    ProbeInterruptError,
    SQLExecutionError,
    SQLValidationError,
)
from .core.executor import SQLExecutor
from .core.resolver import LevelResolver
from .core.aggregator import ResultAggregator
from .core.template import TemplateEngine
from .core.aggregation import AggregationCondition, MultiCondition

logger = logging.getLogger(__name__)


class SQLProbeNotifier:
    """
    SQL 探针通知器
    
    通过组合方式调用 feishu-notify，提供:
    1. SQL 执行和结果验证
    2. 自动告警级别推断
    3. 飞书通知发送
    4. 流程中断控制
    
    Usage:
        ```python
        from sql_probe import SQLProbeNotifier, ProbeInterruptError
        
        # 使用默认渠道（读取 webhook-default 或 FEISHU_WEBHOOK）
        probe = SQLProbeNotifier(spark)
        
        # 使用自定义渠道（读取 webhook-{channel} 或 FEISHU_WEBHOOK_{CHANNEL}）
        probe = SQLProbeNotifier(spark, channel="your_channel")
        
        # 显式指定 webhook（不走配置）
        probe = SQLProbeNotifier(spark, webhook="https://...")
        
        # 执行检查
        result = probe.execute('''
            SELECT
                'NULL值检查' as alert_name,
                CASE WHEN cnt > 100 THEN 1 ELSE 0 END as is_warning,
                concat('发现 ', cnt, ' 条异常') as alert_info,
                CASE WHEN cnt > 100 THEN 'AbnormalRed' ELSE 'Normal' END as status
            FROM (SELECT count(*) as cnt FROM table WHERE id IS NULL)
        ''')
        ```
    """
    
    # 配置常量
    DEFAULT_SECRET_SCOPE = "sql-probe"
    ENV_SOURCE = "SQL_PROBE_SOURCE"
    
    def __init__(
        self,
        spark,
        webhook: Optional[str] = None,
        channel: str = "default",
        source: Optional[str] = None,
        notifier: Optional[Any] = None,
        debug: bool = False,
        interrupt_on_error: bool = True,
        secret_scope: Optional[str] = None,
    ):
        """
        初始化探针通知器
        
        Args:
            spark: SparkSession 实例
            webhook: 飞书 Webhook URL（直接传入，优先级最高）
            channel: 通知渠道，按需自定义，对应的配置规则:
                     - channel="xxx" → Secrets: webhook-xxx / 环境变量: FEISHU_WEBHOOK_XXX
                     - channel="default" (默认) → Secrets: webhook-default / 环境变量: FEISHU_WEBHOOK
            source: 消息来源标识
            notifier: 已初始化的 feishu-notify Notifier 实例（可选）
            debug: 是否开启调试模式
            interrupt_on_error: 默认是否在 ERROR 级别中断
            secret_scope: Databricks Secrets scope（默认 "sql-probe"）
        """
        self.spark = spark
        self.debug = debug
        self.default_interrupt_on_error = interrupt_on_error
        self.secret_scope = secret_scope or self.DEFAULT_SECRET_SCOPE
        
        # 解析配置
        self.source = self._resolve_source(source)
        resolved_webhook = self._resolve_webhook(webhook, channel)
        
        # 初始化核心组件
        self.executor = SQLExecutor(spark)
        self.resolver = LevelResolver()
        self.aggregator = ResultAggregator()
        self.template_engine = TemplateEngine()
        
        # 告警状态历史（用于 notify_on_ok 功能）
        # key: alert_name, value: 上一次是否触发告警
        self._alert_status: Dict[str, bool] = {}
        
        # 初始化通知器
        self.notifier = self._init_notifier(resolved_webhook, self.source, notifier)
        
        # 配置日志
        if debug:
            logging.basicConfig(level=logging.DEBUG)
            logger.setLevel(logging.DEBUG)
            logger.debug(f"[SQL-Probe] 初始化完成，source={self.source}, channel={channel}, webhook={'已配置' if resolved_webhook else '未配置'}")
    
    def _resolve_webhook(self, webhook: Optional[str], channel: str) -> Optional[str]:
        """
        解析 Webhook URL
        
        优先级: 显式参数 > Databricks Secrets > 环境变量
        
        Args:
            webhook: 显式传入的 webhook URL
            channel: 通知渠道名称（可自定义，如 "default", "your_channel"）
        """
        # 1. 显式参数优先
        if webhook:
            return webhook
        
        # 2. 构建 secret key 和环境变量名
        if channel == "default":
            secret_key = "webhook-default"
            env_key = "FEISHU_WEBHOOK"
        else:
            secret_key = f"webhook-{channel}"
            env_key = f"FEISHU_WEBHOOK_{channel.upper()}"
        
        # 3. 尝试 Databricks Secrets
        secret_webhook = self._get_secret(self.secret_scope, secret_key)
        if secret_webhook:
            if self.debug:
                logger.debug(f"[SQL-Probe] 从 Secrets 读取: {self.secret_scope}/{secret_key}")
            return secret_webhook
        
        # 4. 尝试环境变量
        env_webhook = os.getenv(env_key)
        if env_webhook:
            if self.debug:
                logger.debug(f"[SQL-Probe] 从环境变量读取: {env_key}")
            return env_webhook
        
        # 5. 如果不是 default，回退到 default
        if channel != "default":
            if self.debug:
                logger.debug(f"[SQL-Probe] channel={channel} 未配置，回退到 default")
            return self._resolve_webhook(None, "default")
        
        return None
    
    def _resolve_source(self, source: Optional[str]) -> str:
        """解析 source 标识"""
        if source:
            return source
        return os.getenv(self.ENV_SOURCE, "SQL-Probe")
    
    def _get_secret(self, scope: str, key: str) -> Optional[str]:
        """
        从 Databricks Secrets 获取值
        
        Returns:
            Secret 值，不在 Databricks 环境或不存在则返回 None
        """
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(self.spark)
            return dbutils.secrets.get(scope=scope, key=key)
        except Exception:
            return None
    
    def _init_notifier(
        self,
        webhook: Optional[str],
        source: str,
        notifier: Optional[Any]
    ) -> Optional[Any]:
        """
        初始化通知器
        
        Args:
            webhook: Webhook URL
            source: 来源标识
            notifier: 已有的 Notifier 实例
            
        Returns:
            Notifier 实例或 None
        """
        if notifier is not None:
            return notifier
        
        if webhook:
            try:
                # 尝试导入 feishu-notify
                # 方法1: 直接导入（如果已在 sys.path 中）
                try:
                    from feishu_notify.notifier import Notifier
                except ImportError:
                    # 方法2: 计算相对路径并添加到 sys.path
                    import os.path
                    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                    if parent_dir not in sys.path:
                        sys.path.insert(0, parent_dir)
                    from feishu_notify.notifier import Notifier
                return Notifier(webhook=webhook, source=source)
            except ImportError as e:
                logger.warning(
                    f"feishu-notify 未安装，通知功能将不可用。"
                    f"请安装 feishu-notify 或传入已初始化的 notifier 实例。错误: {e}"
                )
                return None
        
        return None
    
    def execute(
        self,
        sql_text: str,
        alert_name: Optional[str] = None,
        *,
        interrupt_on_error: Optional[bool] = None,
        silent: bool = False,
        title_prefix: str = "",
        mentions: Optional[List[str]] = None,
        links: Optional[List[Dict[str, str]]] = None,
        force_level: Optional[AlertLevel] = None,
        max_level: Optional[AlertLevel] = None,
        notify_on_ok: bool = False,
        empty_result_as: str = "ok",
        template: Optional[str] = None,
        condition: Optional[Union[AggregationCondition, MultiCondition]] = None,
    ) -> ProbeResult:
        """
        执行 SQL 探针检查
        
        Args:
            sql_text: 符合规范的 SQL 语句（必须返回 is_warning, alert_info, status 列）
            alert_name: 告警名称（可选，可从 SQL 结果的 alert_name 列提取）
            interrupt_on_error: 是否在 ERROR 级别中断（默认使用初始化时的配置）
            silent: 静默模式，不发送通知
            title_prefix: 通知标题前缀
            mentions: 需要 @ 的用户 ID 列表
            links: 附加链接列表，格式 [{"text": "查看", "url": "..."}]
            force_level: 强制指定级别（忽略 SQL 结果推断）
            max_level: 最大级别限制（用于测试环境降级）
            notify_on_ok: 当从告警恢复到正常时，是否发送"已恢复"通知
            empty_result_as: SQL 返回空结果时的处理方式:
                            - "ok": 视为正常（默认）
                            - "warning": 视为警告
                            - "error": 视为错误
            template: 自定义通知模板，支持变量如 {alert_name}, {level}, {content} 等
            condition: 聚合条件，如 AggregationCondition.sum("amount") > 10000
            
        Returns:
            ProbeResult 执行结果
            
        Raises:
            ProbeInterruptError: 当级别达到中断阈值时抛出
            SQLExecutionError: SQL 执行失败
            SQLValidationError: SQL 结果格式不符合规范
        """
        if interrupt_on_error is None:
            interrupt_on_error = self.default_interrupt_on_error
        
        if self.debug:
            logger.debug(f"[SQL-Probe] 执行 SQL:\n{sql_text[:300]}...")
        
        try:
            # 1. 执行 SQL（如果使用 condition，跳过标准列验证）
            skip_validation = condition is not None
            rows, execution_time = self.executor.execute(sql_text, skip_validation=skip_validation)
            
            if self.debug:
                logger.debug(f"[SQL-Probe] 返回 {len(rows)} 行，耗时 {execution_time:.2f}s")
            
            # 2. 处理空结果
            if not rows:
                result = self._handle_empty_result(
                    empty_result_as=empty_result_as,
                    execution_time=execution_time,
                    sql_text=sql_text,
                    alert_name=alert_name or "未命名告警"
                )
            else:
                # 3. 检查聚合条件（如果有）
                if condition:
                    result = self._evaluate_condition(
                        rows=rows,
                        condition=condition,
                        execution_time=execution_time,
                        sql_text=sql_text,
                        alert_name=alert_name
                    )
                else:
                    # 4. 解析级别（传统方式）
                    level, details = self.resolver.resolve_all(rows)
                    level = self.resolver.apply_overrides(level, force_level, max_level)
                    
                    if self.debug:
                        logger.debug(f"[SQL-Probe] 解析级别: {level.name}")
                    
                    # 5. 聚合结果
                    result = self.aggregator.aggregate(
                        details=details,
                        level=level,
                        execution_time=execution_time,
                        sql_text=sql_text,
                        default_alert_name=alert_name
                    )
            
            # 6. 发送通知（包括恢复通知）
            if not silent:
                self._send_notification_with_recovery(
                    result=result,
                    title_prefix=title_prefix,
                    mentions=mentions,
                    links=links,
                    notify_on_ok=notify_on_ok,
                    template=template
                )
            
            # 7. 更新告警状态
            self._alert_status[result.alert_name] = result.triggered
            
            # 8. 检查是否需要中断
            self._check_interrupt(result, interrupt_on_error)
            
            return result
            
        except ProbeInterruptError:
            # 中断异常直接抛出
            raise
        except (SQLExecutionError, SQLValidationError):
            # SQL 相关异常也抛出
            raise
        except ProbeError:
            raise
        except Exception as e:
            # 未预期异常包装后抛出
            logger.exception(f"探针执行异常: {e}")
            raise ProbeError(f"探针执行异常: {e}") from e
    
    def execute_batch(
        self,
        tasks: List[Dict[str, Any]],
        *,
        interrupt_on_error: Optional[bool] = None,
        silent: bool = False,
        title_prefix: str = "",
        mentions: Optional[List[str]] = None,
        links: Optional[List[Dict[str, str]]] = None,
    ) -> ProbeResult:
        """
        批量执行多个 SQL 检查
        
        Args:
            tasks: 任务列表，每项包含:
                   - sql: SQL 文本（必填）
                   - name: 告警名称（可选）
                   - 其他参数会传递给 execute()
            interrupt_on_error: 是否在 ERROR 级别中断（应用于汇总结果）
            silent: 静默模式
            title_prefix: 通知标题前缀
            mentions: 需要 @ 的用户 ID 列表
            links: 附加链接列表
            
        Returns:
            聚合后的 ProbeResult
        """
        if interrupt_on_error is None:
            interrupt_on_error = self.default_interrupt_on_error
        
        results = []
        
        for task in tasks:
            # 提取任务参数
            sql = task.get("sql")
            name = task.get("name")
            
            if not sql:
                logger.warning(f"跳过无效任务: {task}")
                continue
            
            try:
                # 单个任务先不中断，最后统一处理
                result = self.execute(
                    sql_text=sql,
                    alert_name=name,
                    interrupt_on_error=False,  # 暂时禁用中断
                    silent=True,  # 暂时禁用通知，最后统一发送
                )
                results.append(result)
                
            except ProbeError as e:
                # 记录错误但继续执行
                if self.debug:
                    logger.warning(f"[SQL-Probe] 任务 '{name}' 执行失败: {e}")
                
                # 创建一个错误结果
                error_result = self.aggregator.create_error_result(
                    error_message=str(e),
                    sql_text=sql,
                    alert_name=name or "未命名"
                )
                results.append(error_result)
        
        # 聚合所有结果
        aggregated = self.aggregator.aggregate_batch(
            results,
            default_alert_name=f"{title_prefix}批量检查" if title_prefix else "批量检查"
        )
        
        # 发送汇总通知
        if not silent:
            self._send_notification(
                result=aggregated,
                title_prefix=title_prefix,
                mentions=mentions,
                links=links
            )
        
        # 检查是否需要中断
        self._check_interrupt(aggregated, interrupt_on_error)
        
        return aggregated
    
    def validate(self, sql_text: str) -> Dict[str, Any]:
        """
        验证 SQL 格式（Dry Run）
        
        Args:
            sql_text: 要验证的 SQL 文本
            
        Returns:
            验证结果:
            {
                "valid": bool,
                "columns": List[str],
                "error": Optional[str]
            }
        """
        return self.executor.validate(sql_text)
    
    def _send_notification(
        self,
        result: ProbeResult,
        title_prefix: str = "",
        mentions: Optional[List[str]] = None,
        links: Optional[List[Dict[str, str]]] = None,
        template: Optional[str] = None
    ) -> None:
        """
        发送飞书通知
        
        Args:
            result: 探针结果
            title_prefix: 标题前缀
            mentions: @ 用户列表
            links: 链接列表
            template: 自定义通知模板
        """
        # 不需要通知的情况
        if not result.level.should_notify():
            if self.debug:
                logger.debug(f"[SQL-Probe] 级别 {result.level.name} 不需要通知")
            return
        
        if self.notifier is None:
            if self.debug:
                logger.debug("[SQL-Probe] 通知器未初始化，跳过通知")
            return
        
        try:
            # 构建标题
            title = f"{title_prefix}{result.alert_name}"
            
            # 构建内容（支持自定义模板）
            if template:
                content = self.template_engine.render(template, result)
            else:
                content = self.template_engine.render(
                    TemplateEngine.DEFAULT_TEMPLATE, 
                    result
                )
            
            # 根据级别选择发送方法
            if result.level >= AlertLevel.CRITICAL:
                self.notifier.critical(
                    title=title,
                    content=content,
                    mentions=mentions,
                    links=links
                )
            elif result.level >= AlertLevel.ERROR:
                self.notifier.error(
                    title=title,
                    error_msg=content,
                    mentions=mentions,
                    links=links
                )
            elif result.level >= AlertLevel.WARNING:
                self.notifier.warning(
                    title=title,
                    content=content,
                    mentions=mentions,
                    links=links
                )
            
            if self.debug:
                logger.debug(f"[SQL-Probe] 通知发送成功: {title}")
                
        except Exception as e:
            # 通知失败不应阻断主流程
            logger.warning(f"[SQL-Probe] 通知发送失败: {e}")
    
    def _handle_empty_result(
        self,
        empty_result_as: str,
        execution_time: float,
        sql_text: str,
        alert_name: str
    ) -> ProbeResult:
        """
        处理 SQL 返回空结果的情况
        
        Args:
            empty_result_as: 空结果处理方式 ("ok"/"warning"/"error")
            execution_time: 执行耗时
            sql_text: SQL 文本
            alert_name: 告警名称
            
        Returns:
            ProbeResult
        """
        level_map = {
            "ok": AlertLevel.INFO,
            "warning": AlertLevel.WARNING,
            "error": AlertLevel.ERROR,
        }
        level = level_map.get(empty_result_as.lower(), AlertLevel.INFO)
        triggered = level >= AlertLevel.WARNING
        
        content = "SQL 返回空结果"
        if empty_result_as.lower() == "ok":
            content = "SQL 返回空结果（视为正常）"
        elif empty_result_as.lower() == "warning":
            content = "SQL 返回空结果（视为警告）"
        elif empty_result_as.lower() == "error":
            content = "SQL 返回空结果（视为错误）"
        
        if self.debug:
            logger.debug(f"[SQL-Probe] 空结果处理: {empty_result_as} -> {level.name}")
        
        from datetime import datetime
        return ProbeResult(
            level=level,
            triggered=triggered,
            alert_name=alert_name,
            content=content,
            details=[],
            row_count=0,
            execution_time=execution_time,
            executed_at=datetime.now(),
            sql_text=sql_text,
            success=True
        )
    
    def _send_notification_with_recovery(
        self,
        result: ProbeResult,
        title_prefix: str,
        mentions: Optional[List[str]],
        links: Optional[List[Dict[str, str]]],
        notify_on_ok: bool,
        template: Optional[str] = None
    ) -> None:
        """
        发送通知，支持恢复通知和自定义模板
        
        Args:
            result: 探针结果
            title_prefix: 标题前缀
            mentions: @ 用户列表
            links: 链接列表
            notify_on_ok: 是否在恢复正常时发送通知
            template: 自定义通知模板
        """
        # 检查是否从告警恢复到正常
        was_triggered = self._alert_status.get(result.alert_name, False)
        is_recovered = was_triggered and not result.triggered
        
        if is_recovered and notify_on_ok:
            # 发送恢复通知
            self._send_recovery_notification(
                result=result,
                title_prefix=title_prefix
            )
        elif result.triggered:
            # 发送告警通知
            self._send_notification(
                result=result,
                title_prefix=title_prefix,
                mentions=mentions,
                links=links,
                template=template
            )
        elif self.debug:
            logger.debug(f"[SQL-Probe] 状态正常，跳过通知: {result.alert_name}")
    
    def _send_recovery_notification(
        self,
        result: ProbeResult,
        title_prefix: str
    ) -> None:
        """
        发送恢复正常的通知
        
        Args:
            result: 探针结果
            title_prefix: 标题前缀
        """
        if self.notifier is None:
            return
        
        try:
            title = f"{title_prefix}✅ {result.alert_name} 已恢复正常"
            content = f"告警已恢复正常\n\n**执行耗时**: {result.execution_time:.2f}s"
            
            # 使用 info/success 级别发送恢复通知
            if hasattr(self.notifier, 'success'):
                self.notifier.success(title=title, content=content)
            elif hasattr(self.notifier, 'info'):
                self.notifier.info(title=title, content=content)
            else:
                # 回退到 warning 但内容表明是恢复
                self.notifier.warning(title=title, content=content)
            
            if self.debug:
                logger.debug(f"[SQL-Probe] 恢复通知发送成功: {title}")
                
        except Exception as e:
            logger.warning(f"[SQL-Probe] 恢复通知发送失败: {e}")
    
    def _evaluate_condition(
        self,
        rows: List[Dict[str, Any]],
        condition: Union[AggregationCondition, MultiCondition],
        execution_time: float,
        sql_text: str,
        alert_name: Optional[str]
    ) -> ProbeResult:
        """
        评估聚合条件
        
        Args:
            rows: SQL 返回的行
            condition: 聚合条件
            execution_time: 执行耗时
            sql_text: SQL 文本
            alert_name: 告警名称
        """
        from datetime import datetime
        
        triggered, value, message = condition.evaluate(rows)
        level = AlertLevel.WARNING if triggered else AlertLevel.INFO
        
        if self.debug:
            logger.debug(f"[SQL-Probe] 聚合条件评估: {message}")
        
        return ProbeResult(
            level=level,
            triggered=triggered,
            alert_name=alert_name or "聚合条件检查",
            content=message,
            details=[],
            row_count=len(rows),
            execution_time=execution_time,
            executed_at=datetime.now(),
            sql_text=sql_text,
            success=True
        )
    
    def _check_interrupt(self, result: ProbeResult, interrupt_on_error: bool) -> None:
        """
        检查是否需要中断执行
        
        Args:
            result: 探针结果
            interrupt_on_error: 是否在 ERROR 级别中断
            
        Raises:
            ProbeInterruptError: 当需要中断时
        """
        # CRITICAL 级别强制中断
        if result.level >= AlertLevel.CRITICAL:
            raise ProbeInterruptError(
                f"严重告警触发，流程强制中断: {result.alert_name}",
                result=result
            )
        
        # ERROR 级别根据配置中断
        if result.level >= AlertLevel.ERROR and interrupt_on_error:
            raise ProbeInterruptError(
                f"错误告警触发，流程中断: {result.alert_name}",
                result=result
            )
    
    # ==================== 便捷方法 ====================
    
    @staticmethod
    def help() -> None:
        """打印配置帮助信息"""
        help_text = """
╔══════════════════════════════════════════════════════════════════════════════╗
║                        SQL-Probe Webhook 配置说明                             ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  通过 channel 参数按需配置不同的飞书群:                                         ║
║                                                                              ║
║  命名规则:                                                                    ║
║  ┌─────────────────┬──────────────────────┬──────────────────────────────┐   ║
║  │ channel 参数     │ Secrets Key          │ 环境变量                      │   ║
║  ├─────────────────┼──────────────────────┼──────────────────────────────┤   ║
║  │ "default" (默认) │ webhook-default      │ FEISHU_WEBHOOK               │   ║
║  │ "xxx" (自定义)   │ webhook-xxx          │ FEISHU_WEBHOOK_XXX           │   ║
║  └─────────────────┴──────────────────────┴──────────────────────────────┘   ║
║                                                                              ║
║  说明: channel 名称可自由定义，Secrets key 保持原样，环境变量名自动转大写         ║
║                                                                              ║
║  配置优先级: webhook参数 > Databricks Secrets > 环境变量 > 回退到default        ║
║                                                                              ║
║  Databricks Secrets 配置示例:                                                 ║
║    databricks secrets create-scope --scope sql-probe                         ║
║    databricks secrets put --scope sql-probe --key webhook-default            ║
║    databricks secrets put --scope sql-probe --key webhook-your_channel       ║
║                                                                              ║
║  使用示例:                                                                    ║
║    probe = SQLProbeNotifier(spark)                        # 默认渠道         ║
║    probe = SQLProbeNotifier(spark, channel="your_channel") # 自定义渠道      ║
║    probe = SQLProbeNotifier(spark, webhook="https://...")  # 显式指定        ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""
        print(help_text)
    
    @staticmethod
    def help_sql() -> None:
        """打印 SQL 规范帮助信息"""
        help_text = """
╔══════════════════════════════════════════════════════════════════════════════╗
║                          SQL-Probe SQL 规范说明                               ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  SQL 必须返回以下列:                                                          ║
║  ┌─────────────┬────────┬──────┬────────────────────────────────────────┐    ║
║  │ 列名        │ 类型    │ 必填 │ 说明                                    │    ║
║  ├─────────────┼────────┼──────┼────────────────────────────────────────┤    ║
║  │ alert_name  │ STRING │ 否   │ 告警名称（可从参数传入）                   │    ║
║  │ is_warning  │ INT    │ 是   │ 0=正常，1=触发告警                        │    ║
║  │ alert_info  │ STRING │ 是   │ 告警详细信息                              │    ║
║  │ status      │ STRING │ 是   │ 状态码，决定告警级别                       │    ║
║  └─────────────┴────────┴──────┴────────────────────────────────────────┘    ║
║                                                                              ║
║  status 状态码映射:                                                           ║
║  ┌─────────────────┬───────────┬──────────────┬─────────────┐               ║
║  │ status          │ 告警级别   │ 通知行为      │ 中断行为     │               ║
║  ├─────────────────┼───────────┼──────────────┼─────────────┤               ║
║  │ Normal          │ INFO      │ 不通知        │ 不中断      │               ║
║  │ AbnormalYellow  │ WARNING   │ 发送通知      │ 不中断      │               ║
║  │ AbnormalRed     │ ERROR     │ 发送通知      │ 可配置中断   │               ║
║  │ Critical        │ CRITICAL  │ 发送通知+@all │ 强制中断    │               ║
║  └─────────────────┴───────────┴──────────────┴─────────────┘               ║
║                                                                              ║
║  SQL 示例:                                                                   ║
║    SELECT                                                                    ║
║        '空值检查' as alert_name,                                              ║
║        CASE WHEN cnt > 0 THEN 1 ELSE 0 END as is_warning,                    ║
║        concat('发现 ', cnt, ' 条空值') as alert_info,                         ║
║        CASE WHEN cnt > 100 THEN 'AbnormalRed'                                ║
║             WHEN cnt > 0 THEN 'AbnormalYellow'                               ║
║             ELSE 'Normal' END as status                                      ║
║    FROM (SELECT count(*) as cnt FROM t WHERE id IS NULL)                     ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""
        print(help_text)
    
    @staticmethod
    def help_features() -> None:
        """打印高级功能帮助信息"""
        help_text = """
╔══════════════════════════════════════════════════════════════════════════════╗
║                        SQL-Probe 高级功能说明                                  ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  1. 恢复通知 (notify_on_ok)                                                   ║
║  ─────────────────────────────────────────────────────────────────────────── ║
║  当告警从异常恢复到正常时，发送"已恢复"通知                                       ║
║                                                                              ║
║  probe.execute(sql, notify_on_ok=True)                                       ║
║                                                                              ║
║  场景：凌晨告警，修复后希望收到恢复通知，而不是一直悬着                             ║
║                                                                              ║
║  2. 空结果处理 (empty_result_as)                                              ║
║  ─────────────────────────────────────────────────────────────────────────── ║
║  SQL 返回 0 行时，应该视为什么状态？                                             ║
║                                                                              ║
║  ┌──────────────────────┬────────────────────────────────────────────────┐   ║
║  │ empty_result_as      │ 场景                                           │   ║
║  ├──────────────────────┼────────────────────────────────────────────────┤   ║
║  │ "ok" (默认)          │ 检查异常数据，0行=没异常=正常                     │   ║
║  │ "warning"            │ 检查必要数据，0行=数据缺失=警告                   │   ║
║  │ "error"              │ 检查关键数据，0行=严重问题=错误                   │   ║
║  └──────────────────────┴────────────────────────────────────────────────┘   ║
║                                                                              ║
║  probe.execute(sql, empty_result_as="warning")                               ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""
        print(help_text)
    
    def info(self, message: str) -> None:
        """打印 INFO 级别日志"""
        logger.info(f"[{self.source}] {message}")
    
    def warning(self, message: str) -> None:
        """打印 WARNING 级别日志并发送通知"""
        logger.warning(f"[{self.source}] {message}")
        if self.notifier:
            self.notifier.warning(title=self.source, content=message)
    
    def error(self, message: str, interrupt: bool = False) -> None:
        """打印 ERROR 级别日志并发送通知"""
        logger.error(f"[{self.source}] {message}")
        if self.notifier:
            self.notifier.error(title=self.source, error_msg=message)
        if interrupt:
            raise ProbeInterruptError(message)
    
    def get_required_columns(self) -> List[str]:
        """获取 SQL 必需返回的列名"""
        return list(self.executor.REQUIRED_COLUMNS)
    
    def __repr__(self) -> str:
        return f"SQLProbeNotifier(source='{self.source}', debug={self.debug})"

