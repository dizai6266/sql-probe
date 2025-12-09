"""
通知模板引擎

支持自定义通知模板，使用 {variable} 语法进行变量替换
"""

import re
from typing import Any, Dict, Optional
from datetime import datetime


class TemplateEngine:
    """
    通知模板引擎
    
    支持的变量:
        - {alert_name}: 告警名称
        - {level}: 告警级别 (INFO/WARNING/ERROR/CRITICAL)
        - {level_emoji}: 级别对应的 emoji
        - {status}: 原始状态码
        - {content}: 告警内容摘要
        - {row_count}: 返回行数
        - {warning_count}: 告警行数
        - {execution_time}: 执行耗时（秒）
        - {timestamp}: 执行时间
        - {triggered}: 是否触发 (True/False)
        - {value}: 第一行的主要值（用于简单场景）
        - {details}: 详细信息列表
    
    Usage:
        engine = TemplateEngine()
        content = engine.render(
            template="{alert_name} 异常！当前值: {value}",
            result=probe_result
        )
    """
    
    # 默认模板
    DEFAULT_TEMPLATE = """{content}

**级别**: {level_emoji} {level}
**触发行数**: {warning_count}/{row_count}
**执行耗时**: {execution_time}s"""
    
    # 简洁模板
    SIMPLE_TEMPLATE = "{level_emoji} {alert_name}: {content}"
    
    # 详细模板
    DETAILED_TEMPLATE = """## {alert_name}

**状态**: {level_emoji} {level}
**触发**: {triggered}
**内容**: {content}

### 执行信息
- 返回行数: {row_count}
- 告警行数: {warning_count}
- 执行耗时: {execution_time}s
- 执行时间: {timestamp}

### 详细信息
{details}"""

    def __init__(self):
        self._pattern = re.compile(r'\{(\w+)\}')
    
    def render(
        self,
        template: str,
        result: Any,  # ProbeResult
        extra_vars: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        渲染模板
        
        Args:
            template: 模板字符串
            result: ProbeResult 对象
            extra_vars: 额外的变量
            
        Returns:
            渲染后的字符串
        """
        # 构建变量字典
        variables = self._build_variables(result)
        
        # 合并额外变量
        if extra_vars:
            variables.update(extra_vars)
        
        # 替换变量
        def replace(match):
            key = match.group(1)
            return str(variables.get(key, f'{{{key}}}'))
        
        return self._pattern.sub(replace, template)
    
    def _build_variables(self, result: Any) -> Dict[str, Any]:
        """
        从 ProbeResult 构建变量字典
        """
        # 获取第一行的值（用于简单场景）
        value = ""
        if result.details:
            first_detail = result.details[0]
            value = first_detail.alert_info
        
        # 格式化详细信息
        details_text = self._format_details(result.details) if result.details else "无"
        
        # 级别 emoji
        level_emoji = getattr(result.level, 'emoji', '📊')
        
        return {
            'alert_name': result.alert_name,
            'level': result.level.name,
            'level_emoji': level_emoji,
            'content': result.content,
            'row_count': result.row_count,
            'warning_count': len(result.warning_rows),
            'execution_time': f"{result.execution_time:.2f}",
            'timestamp': result.executed_at.strftime("%Y-%m-%d %H:%M:%S"),
            'triggered': "是" if result.triggered else "否",
            'value': value,
            'details': details_text,
            'success': "成功" if result.success else "失败",
            'error_message': result.error_message or "",
        }
    
    def _format_details(self, details: list) -> str:
        """格式化详细信息列表"""
        if not details:
            return "无"
        
        lines = []
        for i, d in enumerate(details, 1):
            status = "⚠️" if d.is_warning else "✅"
            lines.append(f"{i}. {status} [{d.status}] {d.alert_info}")
        
        return "\n".join(lines)
    
    @classmethod
    def get_preset(cls, name: str) -> str:
        """
        获取预设模板
        
        Args:
            name: 模板名称 ("default", "simple", "detailed")
            
        Returns:
            模板字符串
        """
        presets = {
            'default': cls.DEFAULT_TEMPLATE,
            'simple': cls.SIMPLE_TEMPLATE,
            'detailed': cls.DETAILED_TEMPLATE,
        }
        return presets.get(name, cls.DEFAULT_TEMPLATE)

