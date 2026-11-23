"""数据血缘领域服务：管理表级和字段级的数据血缘图、上下游追踪和断裂检测。

数据链路位置：元数据层，维护数据从源到目标的完整血缘关系图。"""

from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class LineageGraphService(BaseModuleService):
    MODULE = "lineage_graph"
    ENTITY = "血缘关系"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["目标表", "来源表列表", "转换逻辑", "中间层级", "血缘深度"]
    REQUIRED_FIELDS = ["目标表", "来源表列表", "转换逻辑"]
    STATUS_ORDER = ["已建立", "待验证", "已断裂"]
    ACTION_RULES = {"刷新血缘": "已建立", "标记断裂": "已断裂"}
    NEGATIVE_ACTIONS = []

    def trace_upstream(
        self,
        target_table: str,
        max_depth: int = 5,
        lineage_ids: list[int] | set[int] | None = None,
    ) -> dict[str, Any]:
        """从目标表向上追溯所有上游来源表和转换链路。

        返回完整的上游依赖树，每个节点包含表名、所在层级和转换逻辑。
        用于变更影响分析。

        传入 lineage_ids 时（按运行编号回查的场景），只在该轮登记的血缘
        范围内追溯，避免同一目标表存在多版方案时把各版来源混在一起。
        """
        scoped = lineage_ids is not None
        id_filter = {int(x) for x in lineage_ids} if scoped else None
        all_entries = store.rows(self.MODULE)

        def eligible(entry: dict[str, Any]) -> bool:
            if entry.get("status") != "已建立":
                return False
            if id_filter is not None and int(entry.get("id", 0)) not in id_filter:
                return False
            return True

        visited: set[str] = set()
        tree: list[dict[str, Any]] = []
        target_tables: set[str] = set()
        all_sources: set[str] = set()
        queue = [(target_table, 0)]
        while queue and len(visited) < 50:
            current, depth = queue.pop(0)
            if current in visited or depth > max_depth:
                continue
            visited.add(current)
            for entry in all_entries:
                if not eligible(entry) or entry.get("目标表") != current:
                    continue
                sources = [s.strip() for s in entry.get("来源表列表", "").split(",") if s.strip()]
                node = {
                    "table": current,
                    "depth": depth,
                    "sources": sources,
                    "transform": entry.get("转换逻辑"),
                    "lineage_id": entry.get("id"),
                    "plan_id": entry.get("转换方案ID"),
                }
                tree.append(node)
                target_tables.add(current)
                all_sources.update(sources)
                for src in sources:
                    if src not in visited:
                        queue.append((src, depth + 1))

        # 根表（最上游）= 作为来源出现、但在追溯范围内不再有对应目标条目的表。
        root_tables = sorted(all_sources - target_tables)
        return {
            "target_table": target_table,
            "max_depth": max_depth,
            "scoped": scoped,
            "lineage_ids": sorted(id_filter) if id_filter is not None else [],
            "total_nodes": len(tree),
            "upstream_tree": tree,
            "root_tables": root_tables,
            "leaf_tables": root_tables,
        }

    def trace_run_upstream(self, run_id: int, target_table: str, max_depth: int = 5) -> dict[str, Any]:
        """按运行编号回查当轮完整链路：只在该轮登记的血缘范围内向上追溯。"""
        run = store.find("pipeline_monitor", run_id)
        if run is None or run.get("pipeline_id") is None:
            return {"ok": False, "message": "运行记录不存在"}
        lineage_ids = [int(x) for x in run.get("lineage_ids", [])]
        result = self.trace_upstream(target_table, max_depth=max_depth, lineage_ids=lineage_ids)
        result["ok"] = True
        result["run_id"] = run_id
        result["pipeline_id"] = run.get("pipeline_id")
        result["effective_plan_id"] = run.get("effective_plan_id")
        return result

    def trace_downstream(self, source_table: str, max_depth: int = 5) -> dict[str, Any]:
        """从来源表向下追溯所有下游受影响表和转换链路。

        用于评估某个表变更时的影响范围，联动质量规则模块通知下游。
        """
        all_entries = store.rows(self.MODULE)
        visited: set[str] = set()
        tree: list[dict[str, Any]] = []
        queue = [(source_table, 0)]
        while queue and len(visited) < 50:
            current, depth = queue.pop(0)
            if current in visited or depth > max_depth:
                continue
            visited.add(current)
            for entry in all_entries:
                sources = entry.get("来源表列表", "").split(",")
                source_set = {s.strip() for s in sources if s.strip()}
                if current in source_set and entry.get("status") == "已建立":
                    target = entry.get("目标表", "")
                    node = {
                        "table": target,
                        "depth": depth + 1,
                        "upstream_of": current,
                        "transform": entry.get("转换逻辑"),
                        "lineage_id": entry.get("id"),
                        "plan_id": entry.get("转换方案ID"),
                    }
                    tree.append(node)
                    if target not in visited:
                        queue.append((target, depth + 1))

        return {
            "source_table": source_table,
            "max_depth": max_depth,
            "total_affected": len(tree),
            "downstream_tree": tree,
            "end_consumers": [n["table"] for n in tree if not any(
                e.get("来源表列表", "").find(n["table"]) >= 0
                for e in all_entries if e.get("status") == "已建立"
            )],
        }

    def detect_broken_lineage(self) -> dict[str, Any]:
        """扫描并检测断裂的血缘关系。

        当上游来源表已被迁移、归档或删除时，其下游血缘关系变为断裂状态。
        检测结果包含受影响的目标表和对应的修复建议。
        """
        all_entries = store.rows(self.MODULE)
        broken: list[dict[str, Any]] = []
        for entry in all_entries:
            if entry.get("status") == "已断裂":
                broken.append({
                    "lineage_id": entry.get("id"),
                    "target_table": entry.get("目标表"),
                    "sources": entry.get("来源表列表"),
                    "needs_revalidation": True,
                })

        table_archive_rows = store.rows("table_archive")
        migration_rows = store.rows("data_migration")

        for entry in all_entries:
            if entry.get("status") != "已建立":
                continue
            sources = entry.get("来源表列表", "").split(",")
            for src in sources:
                src = src.strip()
                if not src:
                    continue
                archived = any(
                    a.get("目标表") == src and a.get("status") == "已启用"
                    for a in table_archive_rows
                )
                if archived:
                    entry["status"] = "已断裂"
                    broken.append({
                        "lineage_id": entry.get("id"),
                        "target_table": entry.get("目标表"),
                        "source": src,
                        "reason": "来源表已归档",
                        "needs_revalidation": True,
                    })

        return {
            "broken_count": len(broken),
            "broken_lineages": broken,
            "suggestion": "请重新校验断裂血缘并更新目标表关联",
        }

    def build_inheritance_chain(self, table_name: str) -> dict[str, Any]:
        """为指定表构建完整的继承链（上下游合起来）。

        从该表出发，同时向上追溯祖先表和向下追溯后代表，
        生成完整的数据流转全景图。
        """
        upstream = self.trace_upstream(table_name)
        downstream = self.trace_downstream(table_name)
        return {
            "table": table_name,
            "upstream_depth": upstream["total_nodes"],
            "downstream_affected": downstream["total_affected"],
            "upstream_chain": upstream["upstream_tree"],
            "downstream_chain": downstream["downstream_tree"],
            "orphan_risk": upstream["total_nodes"] == 0 and downstream["total_affected"] > 0,
        }

    def impact_analysis(self, entry_id: int) -> dict[str, Any]:
        """变更影响分析：当某个血缘条目的转换逻辑发生变更时，
        评估对下游的级联影响范围和受影响的数据质量规则。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "血缘条目不存在"}

        target_table = entry.get("目标表", "")
        downstream = self.trace_downstream(target_table)

        quality_rows = store.rows("data_quality_rule")
        affected_quality_rules = [
            q for q in quality_rows
            if q.get("目标表") in [n["table"] for n in downstream["downstream_tree"]]
        ]

        return {
            "lineage_id": entry_id,
            "source_table": target_table,
            "change_type": "transform_logic_changed",
            "direct_downstream_tables": len(downstream["downstream_tree"]),
            "cascaded_tables": downstream["total_affected"],
            "affected_quality_rule_count": len(affected_quality_rules),
            "affected_quality_rules": [
                {"rule_id": r["id"], "rule_name": r.get("规则名称"), "target_table": r.get("目标表")}
                for r in affected_quality_rules[:10]
            ],
            "recommendation": "建议在变更后立即执行受影响的质量规则校验",
        }
