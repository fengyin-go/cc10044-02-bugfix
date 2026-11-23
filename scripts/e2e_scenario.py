"""端到端校验：转换方案部署/血缘登记/ETL 启动/按运行编号血缘回查。

只用标准库（urllib），通过 HTTP 接口驱动完整业务场景。
"""
from __future__ import annotations

import json
import sys
import urllib.error
import urllib.parse
import urllib.request

BASE = "http://127.0.0.1:8018/api"

FAILURES: list[str] = []


def call(method: str, path: str, payload: dict | None = None, expect_error: bool = False):
    if "?" in path:
        base_path, query = path.split("?", 1)
        path = base_path + "?" + urllib.parse.urlencode(
            urllib.parse.parse_qsl(query)
        )
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            body = json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        body = json.loads(exc.read().decode())
        if not expect_error:
            raise
        return body
    if expect_error:
        FAILURES.append(f"{path} 预期报错但成功了")
    return body


def check(cond: bool, label: str) -> None:
    print(("  ✓ " if cond else "  ✗ ") + label)
    if not cond:
        FAILURES.append(label)


# ---------- 准备：新建一条订单流水线并启用 ----------
print("== 准备：登记订单流水线 ==")
created = call("POST", "/etl_pipeline", {"values": {
    "流水线名称": "order_lineage_e2e",
    "源端类型": "MySQL",
    "目标端类型": "Hive",
    "执行周期": "每天 02:00",
}})
check(created["ok"], "流水线登记成功")
pipeline_id = created["entry"]["id"]
call("POST", f"/etl_pipeline/{pipeline_id}/actions", {"action": "启用流水线"})

# ---------- A 版方案：raw_orders -> cleaned_orders -> enriched_orders ----------
print("== A 版方案登记/发布/部署/血缘登记 ==")
plan_a = call("POST", "/transform_plan", {"values": {
    "方案名称": "A版订单清洗",
    "步骤序列": "raw_orders->cleaned_orders(清洗去重),cleaned_orders->enriched_orders(维度关联)",
    "输入表": "raw_orders",
    "输出表": "enriched_orders",
    "依赖关系": "dim_customer",
}})
plan_a_id = plan_a["entry"]["id"]
call("POST", f"/transform_plan/{plan_a_id}/actions", {"action": "发布方案"})

deploy_a = call("POST", f"/transform_plan/{plan_a_id}/deploy", {"pipeline_id": pipeline_id})
check(deploy_a["ok"] and deploy_a["plan_id"] == plan_a_id, "A 版部署到流水线")
check(deploy_a["lineage_complete"] is False, "A 版部署时血缘尚未登记、不完整")

reg_a_1 = call("POST", f"/transform_plan/{plan_a_id}/lineage")
reg_a_2 = call("POST", f"/transform_plan/{plan_a_id}/lineage")
check(reg_a_1["ok"] and reg_a_1["registered_lineages"] == 2, "A 版登记两条血缘")
check(reg_a_2["deduplicated"] is True, "A 版重复登记命中幂等")
check(reg_a_1["lineage_ids"] == reg_a_2["lineage_ids"], "A 版重复登记返回同一组血缘编号")
a_lineage_ids = reg_a_1["lineage_ids"]

# 通用动作入口也能登记，且同样幂等
action_reg = call("POST", f"/transform_plan/{plan_a_id}/actions", {"action": "登记血缘"})
check(action_reg["ok"], "通用动作入口支持登记血缘")
plan_a_row = call("GET", f"/transform_plan/{plan_a_id}")
check(plan_a_row["status"] == "已发布", "登记血缘不改变方案状态")

eff = call("GET", f"/etl_pipeline/{pipeline_id}/effective-plan")
check(eff["effective_plan_id"] == plan_a_id and eff["deployed_lineage_complete"] is True,
      "血缘齐全后 A 版具备生效资格")

# ---------- 第一轮：A 版生效 ----------
print("== 第一轮提交（A 版生效）==")
run1 = call("POST", f"/etl_pipeline/{pipeline_id}/execute", {})
run1_id = run1["run_id"]
check(run1["ok"], "第一轮启动成功")
check(run1["effective_plan_id"] == plan_a_id, "第一轮生效方案为 A 版")
check(run1["applied_source"] == "deployed_plan", "第一轮来源标记为部署方案")
check(run1["lineage_ids"] == a_lineage_ids, "第一轮血缘快照即 A 版两条血缘")

detail1 = call("GET", f"/pipeline_monitor/runs/{run1_id}")
check([s["id"] for s in detail1["transform_steps"]] == ["step_1", "step_2"], "运行详情含两步转换")
check(detail1["transform_steps"][1]["depends_on"] == ["step_1"], "第二步依赖第一步")
m1 = {(m["input"], m["output"]) for m in detail1["table_mappings"]}
check(m1 == {("raw_orders", "cleaned_orders"), ("cleaned_orders", "enriched_orders")},
      "第一轮表映射：raw_orders→cleaned_orders→enriched_orders")

# ---------- B 版方案：raw_orders -> normalized_orders -> enriched_orders ----------
print("== B 版发布并部署，但值班员尚未登记 B 版血缘 ==")
plan_b = call("POST", "/transform_plan", {"values": {
    "方案名称": "B版订单标准化",
    "步骤序列": "raw_orders->normalized_orders(清洗标准化),normalized_orders->enriched_orders(维度关联)",
    "输入表": "raw_orders",
    "输出表": "enriched_orders",
    "依赖关系": "dim_customer",
}})
plan_b_id = plan_b["entry"]["id"]
call("POST", f"/transform_plan/{plan_b_id}/actions", {"action": "发布方案"})
deploy_b = call("POST", f"/transform_plan/{plan_b_id}/deploy", {"pipeline_id": pipeline_id})
check(deploy_b["ok"], "B 版部署成功")
check(deploy_b["lineage_complete"] is False, "B 版血缘不完整")

b_lineage_view = call("GET", f"/transform_plan/{plan_b_id}/lineage")
check(b_lineage_view["lineage_ids"] == [] and b_lineage_view["lineage_complete"] is False,
      "B 版尚无任何血缘登记")

eff2 = call("GET", f"/etl_pipeline/{pipeline_id}/effective-plan")
check(eff2["deployed_plan_id"] == plan_b_id, "待生效部署指向 B 版")
check(eff2["deployed_lineage_complete"] is False, "B 版血缘不完整、不具备生效资格")
check(eff2["effective_plan_id"] == plan_a_id, "切换期间新一轮沿用 A 版而非 B 版")
check(eff2["applied_source"] == "previous_effective_plan", "来源标记为上一轮生效方案")

# 未发布方案不能部署
draft = call("POST", "/transform_plan", {"values": {
    "方案名称": "草稿方案", "步骤序列": "a->b(x)", "输入表": "a", "输出表": "b",
}})
bad_deploy = call("POST", f"/transform_plan/{draft['entry']['id']}/deploy",
                  {"pipeline_id": pipeline_id}, expect_error=True)
check("detail" in bad_deploy, "未发布方案部署被拒绝")

# ---------- 第二轮：B 已部署但血缘未登记，仍按 A 版启动 ----------
print("== 第二轮提交（B 已部署、血缘未登记）==")
run2 = call("POST", f"/etl_pipeline/{pipeline_id}/execute", {})
run2_id = run2["run_id"]
check(run2["ok"], "切换期间流水线仍可启动")
check(run2["deployed_plan_id"] == plan_b_id, "第二轮登记的部署方案为 B 版")
check(run2["effective_plan_id"] == plan_a_id, "第二轮实际生效仍为 A 版")
check(run2["lineage_ids"] == a_lineage_ids, "第二轮未用 A 版血缘之外的任何关系，也未给 B 版补血缘")
detail2 = call("GET", f"/pipeline_monitor/runs/{run2_id}")
m2 = {(m["input"], m["output"]) for m in detail2["table_mappings"]}
check(("cleaned_orders", "enriched_orders") in m2 and ("normalized_orders", "enriched_orders") not in m2,
      "第二轮中间表仍是 cleaned_orders，没有 normalized_orders")
b_lineage_view2 = call("GET", f"/transform_plan/{plan_b_id}/lineage")
check(b_lineage_view2["lineage_ids"] == [], "确认未用 A 版血缘给 B 版补齐")

# ---------- 值班员从登记入口办理两次 B 版 ----------
print("== 登记入口两次办理 B 版血缘 ==")
reg_b_1 = call("POST", f"/transform_plan/{plan_b_id}/lineage")
reg_b_2 = call("POST", f"/transform_plan/{plan_b_id}/lineage")
check(reg_b_1["registered_lineages"] == 2, "B 版登记两条血缘")
check(reg_b_2["deduplicated"] is True, "B 版第二次登记命中幂等")
check(reg_b_1["lineage_ids"] == reg_b_2["lineage_ids"], "两次返回同一组血缘编号")
b_lineage_ids = reg_b_1["lineage_ids"]
check(set(a_lineage_ids).isdisjoint(set(b_lineage_ids)), "A/B 两版血缘编号互不重叠")
# 同一目标表 enriched_orders 现在存在 A/B 两版血缘（按方案版本区分，不混源）
all_lineage = call("GET", "/lineage_graph?size=200")
enriched_rows = [r for r in all_lineage["items"] if r["目标表"] == "enriched_orders"]
enriched_by_plan = {str(r.get("转换方案ID")): r for r in enriched_rows}
check(str(plan_a_id) in enriched_by_plan and "cleaned_orders" in enriched_by_plan[str(plan_a_id)]["来源表列表"],
      "enriched_orders 的 A 版血缘来源为 cleaned_orders")
check(str(plan_b_id) in enriched_by_plan and "normalized_orders" in enriched_by_plan[str(plan_b_id)]["来源表列表"],
      "enriched_orders 的 B 版血缘来源为 normalized_orders")
check(enriched_by_plan[str(plan_a_id)]["id"] != enriched_by_plan[str(plan_b_id)]["id"],
      "两版血缘是相互独立的条目")

eff3 = call("GET", f"/etl_pipeline/{pipeline_id}/effective-plan")
check(eff3["effective_plan_id"] == plan_b_id and eff3["deployed_lineage_complete"] is True,
      "登记齐全后 B 版成为生效方案")

# ---------- 第三轮：B 版生效 ----------
print("== 第三轮提交（B 版生效）==")
run3 = call("POST", f"/etl_pipeline/{pipeline_id}/execute", {})
run3_id = run3["run_id"]
check(run3["effective_plan_id"] == plan_b_id, "第三轮生效方案为 B 版")
check(run3["lineage_ids"] == b_lineage_ids, "第三轮血缘快照即 B 版两条血缘")
detail3 = call("GET", f"/pipeline_monitor/runs/{run3_id}")
m3 = {(m["input"], m["output"]) for m in detail3["table_mappings"]}
check(m3 == {("raw_orders", "normalized_orders"), ("normalized_orders", "enriched_orders")},
      "第三轮表映射：raw_orders→normalized_orders→enriched_orders")
check(detail3["transform_steps"][1]["depends_on"] == ["step_1"], "B 版第二步同样依赖第一步")

# ---------- 运行历史与按运行编号血缘回查 ----------
print("== 运行历史 + 按运行编号血缘追溯 ==")
history = call("GET", "/pipeline_monitor?流水线名称=order_lineage_e2e&size=50")
run_ids_hist = [r["id"] for r in history["items"] if r.get("pipeline_id") == pipeline_id]
check(run_ids_hist == [run1_id, run2_id, run3_id], "运行历史含三轮且顺序正确")

# 每轮的转换步骤/表映射必须与该轮已登记的血缘逐对一致
for rid in (run1_id, run2_id, run3_id):
    d = call("GET", f"/pipeline_monitor/runs/{rid}")
    lineage_pool = call("GET", "/lineage_graph?size=200")["items"]
    scoped = {r["id"]: r for r in lineage_pool if r["id"] in d["lineage_ids"]}
    check(len(scoped) == len(d["lineage_ids"]), f"运行 {rid} 血缘编号全部可回查到实体")
    check(len(scoped) == len(d["table_mappings"]), f"运行 {rid} 血缘条数与步骤数一致")
    for mapping in d["table_mappings"]:
        hit = any(
            row["目标表"] == mapping["output"]
            and mapping["input"] in [s.strip() for s in row["来源表列表"].split(",")]
            for row in scoped.values()
        )
        check(hit, f"运行 {rid} 映射 {mapping['input']}→{mapping['output']} 有对应血缘")
    # 血缘范围内不得出现该轮表映射之外的目标表
    mapped_outputs = {m["output"] for m in d["table_mappings"]}
    check(set(r["目标表"] for r in scoped.values()) == mapped_outputs,
          f"运行 {rid} 血缘目标表集合与步骤输出表完全一致")

trace1 = call("GET", f"/pipeline_monitor/runs/{run1_id}/lineage?target_table=enriched_orders")
tables1 = {n["table"] for n in trace1["upstream_tree"]}
check(tables1 == {"enriched_orders", "cleaned_orders"}, "第一轮链路节点：enriched_orders/cleaned_orders")
check(trace1["root_tables"] == ["raw_orders"], "第一轮继续追溯到 raw_orders")
check("normalized_orders" not in json.dumps(trace1), "第一轮血缘不混入 B 版 normalized_orders")
check(trace1["lineage_ids"] == a_lineage_ids, "第一轮追溯范围锁定 A 版血缘编号")

trace3 = call("GET", f"/pipeline_monitor/runs/{run3_id}/lineage?target_table=enriched_orders")
tables3 = {n["table"] for n in trace3["upstream_tree"]}
check(tables3 == {"enriched_orders", "normalized_orders"}, "第三轮链路节点：enriched_orders/normalized_orders")
check(trace3["root_tables"] == ["raw_orders"], "第三轮继续追溯到 raw_orders")
check("cleaned_orders" not in json.dumps(trace3), "第三轮血缘不混入 A 版 cleaned_orders")
check(trace3["lineage_ids"] == b_lineage_ids, "第三轮追溯范围锁定 B 版血缘编号")

trace2 = call("GET", f"/pipeline_monitor/runs/{run2_id}/lineage?target_table=enriched_orders")
check("cleaned_orders" in json.dumps(trace2) and "normalized_orders" not in json.dumps(trace2),
      "第二轮（沿用 A 版）追溯同样经过 cleaned_orders")

missing = call("GET", "/pipeline_monitor/runs/999999/lineage?target_table=enriched_orders", expect_error=True)
check("detail" in missing, "不存在的运行编号回查返回 404")

# ---------- 未部署转换方案的流水线沿用默认规则 ----------
print("== 未部署方案的流水线沿用默认规则 ==")
plain = call("POST", "/etl_pipeline", {"values": {
    "流水线名称": "plain_default_pipeline", "源端类型": "MySQL", "目标端类型": "Iceberg",
}})
plain_id = plain["entry"]["id"]
call("POST", f"/etl_pipeline/{plain_id}/actions", {"action": "启用流水线"})
plain_eff = call("GET", f"/etl_pipeline/{plain_id}/effective-plan")
check(plain_eff["effective_plan_id"] is None and plain_eff["applied_source"] == "default",
      "未部署方案时无生效方案、标记默认规则")
plain_run = call("POST", f"/etl_pipeline/{plain_id}/execute", {})
check(plain_run["ok"] and plain_run["effective_plan_id"] is None, "默认规则下流水线可正常启动")
plain_detail = call("GET", f"/pipeline_monitor/runs/{plain_run['run_id']}")
transform_stage = plain_detail["stages_plan"]["stages"][2]
check(len(transform_stage["transform_rules"]) >= 1 and transform_stage["table_mappings"] == [],
      "转换阶段沿用内置默认规则、无方案表映射")
check(plain_run["lineage_ids"] == [], "默认规则运行不登记方案血缘")

# ---------- 启动入口补齐血缘的能力 ----------
print("== 启动入口补齐已发布方案血缘 ==")
plan_c = call("POST", "/transform_plan", {"values": {
    "方案名称": "C版启动补齐",
    "步骤序列": "raw_orders->c_staging(清洗),c_staging->enriched_orders(关联)",
    "输入表": "raw_orders", "输出表": "enriched_orders",
}})
plan_c_id = plan_c["entry"]["id"]
call("POST", f"/transform_plan/{plan_c_id}/actions", {"action": "发布方案"})
call("POST", f"/transform_plan/{plan_c_id}/deploy", {"pipeline_id": plain_id})
run_c = call("POST", f"/etl_pipeline/{plain_id}/execute", {"register_lineage": True})
check(run_c["lineage_registration"] is not None and run_c["lineage_registration"]["registered_lineages"] == 2,
      "启动时自动补齐 C 版两条血缘")
check(run_c["effective_plan_id"] == plan_c_id, "补齐后同轮启动即按 C 版生效")
run_c_again = call("POST", f"/etl_pipeline/{plain_id}/execute", {"register_lineage": True})
check(run_c_again["lineage_registration"]["deduplicated"] is True,
      "启动入口重复补齐同样幂等，只保留一套关系")

print()
if FAILURES:
    print(f"失败 {len(FAILURES)} 项：")
    for item in FAILURES:
        print(" - " + item)
    sys.exit(1)
print("全部场景校验通过")
