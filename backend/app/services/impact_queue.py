"""影响链工作队列单例：血缘关系与重跑任务两个模块共享同一份队列。

队列核心逻辑在 app.pipeline.impact_chain_queue；这里只持有跨模块共享实例，
保证血缘侧领取/撤销与重跑页确认/裁决看到的是同一份待办。
"""
from __future__ import annotations

from app.pipeline.impact_chain_queue import ImpactChainQueue

IMPACT_QUEUE = ImpactChainQueue()
