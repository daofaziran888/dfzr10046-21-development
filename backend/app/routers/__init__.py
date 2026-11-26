"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import impact_chain, resource

# impact_chain 使用固定前缀 /api/impact-chain，需先于资源路由注册，
# 避免被 /api/{module} 动态路径抢先匹配。
ROUTERS = [impact_chain, resource]
