"""业务路由汇总：跨模块领域路由优先注册，通用资源路由兜底模块 key 动态路径。"""
from __future__ import annotations

from app.routers import domain, resource

ROUTERS = [domain, resource]
