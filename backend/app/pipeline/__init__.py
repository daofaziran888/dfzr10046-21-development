"""跨模块数据管线：承载 22 个模块之间的真实数据处理链路。

模块划分：
- watermark: 装载水位、窗口推进、抽取水位校验
- scd_processor: 拉链区间加工（闭链/冲突检测/属性失效联动）
- fact_assembler: 事实装配（代理键解析/外键命中率/桥接归一）
- quality_checker: 质量闸（阈值校验/阻断放行/重跑触发）
- reprocess_orchestrator: 重跑编排（覆盖策略/血缘传递/迟到回补）
- impact_chain_queue: 影响链工作队列（领取/撤销/排期冲突裁决）
- snapshot_manager: 维度快照生成与发布
- integration: 端到端集成链路
"""

from app.pipeline.watermark import WatermarkManager
from app.pipeline.scd_processor import ScdProcessor
from app.pipeline.fact_assembler import FactAssembler
from app.pipeline.quality_checker import QualityChecker
from app.pipeline.reprocess_orchestrator import ReprocessOrchestrator
from app.pipeline.impact_chain_queue import ImpactChainQueue
from app.pipeline.snapshot_manager import SnapshotManager
from app.pipeline.integration import PipelineOrchestrator

__all__ = [
    "WatermarkManager",
    "ScdProcessor",
    "FactAssembler",
    "QualityChecker",
    "ReprocessOrchestrator",
    "ImpactChainQueue",
    "SnapshotManager",
    "PipelineOrchestrator",
]