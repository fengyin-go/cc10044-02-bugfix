/**
 * 跨模块领域接口封装：转换方案部署/血缘登记、流水线启动/运行详情、按运行编号血缘回查。
 * 与 backend/app/routers/domain.py 的接口一一对应。
 */
import { fetchJson, request } from './client'

export type EffectivePlan = {
  ok: boolean
  pipeline_id: number
  deployed_plan_id: number | null
  deployed_lineage_complete: boolean
  effective_plan_id: number | null
  applied_source: 'default' | 'deployed_plan' | 'previous_effective_plan'
}

export type LineageRegistration = {
  ok: boolean
  plan_id: number
  deduplicated: boolean
  registered_lineages: number
  lineage_ids: number[]
  message: string
}

export type ExecuteResult = {
  ok: boolean
  message: string
  run_id: number
  stages: number
  effective_plan_id: number | null
  deployed_plan_id: number | null
  applied_source: string
  lineage_ids: number[]
  lineage_registration: LineageRegistration | null
}

export type RunDetail = {
  ok: boolean
  run_id: number
  pipeline_id: number
  pipeline_name: string
  运行时间: string
  status: string
  effective_plan_id: number | null
  deployed_plan_id: number | null
  applied_source: string
  transform_steps: Array<{
    id: string
    input: string
    output: string
    transform: string
    depends_on: string[]
  }>
  table_mappings: Array<{
    step_id: string
    input: string
    output: string
    depends_on: string[]
    transform: string
  }>
  lineage_ids: number[]
}

export type RunLineageTrace = {
  ok: boolean
  run_id: number
  pipeline_id: number
  effective_plan_id: number | null
  target_table: string
  scoped: boolean
  lineage_ids: number[]
  total_nodes: number
  upstream_tree: Array<{
    table: string
    depth: number
    sources: string[]
    transform: string
    lineage_id: number
    plan_id: string | null
  }>
  root_tables: string[]
}

/** 把已发布的转换方案部署到指定流水线。 */
export async function deployTransformPlan(planId: number, pipelineId: number) {
  const response = await request(`/api/transform_plan/${planId}/deploy`, {
    method: 'POST',
    body: JSON.stringify({ pipeline_id: pipelineId }),
  })
  return await response.json()
}

/** 登记入口：为转换方案登记血缘；同一版重复提交幂等，返回同一组血缘编号。 */
export function registerPlanLineage(planId: number): Promise<LineageRegistration> {
  return fetchJson(`/api/transform_plan/${planId}/lineage`, { method: 'POST' })
}

/** 查询某一版方案已登记的血缘清单与完整性结论。 */
export function fetchPlanLineage(planId: number) {
  return fetchJson<{
    plan_id: number
    lineage_complete: boolean
    lineage_ids: number[]
    items: Array<Record<string, string | number | boolean>>
  }>(`/api/transform_plan/${planId}/lineage`)
}

/** 查询本轮启动将采用哪一版转换方案。 */
export function fetchEffectivePlan(pipelineId: number): Promise<EffectivePlan> {
  return fetchJson(`/api/etl_pipeline/${pipelineId}/effective-plan`)
}

/** 查询流水线阶段规划（转换阶段与当轮生效方案取同一份规则）。 */
export function fetchPipelineStages(pipelineId: number) {
  return fetchJson(`/api/etl_pipeline/${pipelineId}/stages`)
}

/** 启动一轮流水线；registerLineage=true 时允许在启动入口补齐已发布方案血缘。 */
export function executePipeline(
  pipelineId: number,
  options: { forceFull?: boolean; registerLineage?: boolean } = {},
): Promise<ExecuteResult> {
  return fetchJson(`/api/etl_pipeline/${pipelineId}/execute`, {
    method: 'POST',
    body: JSON.stringify({
      force_full: options.forceFull ?? false,
      register_lineage: options.registerLineage ?? false,
    }),
  })
}

/** 按运行编号回查运行详情：生效方案、转换步骤、表映射与血缘快照。 */
export function fetchRunDetail(runId: number): Promise<RunDetail> {
  return fetchJson(`/api/pipeline_monitor/runs/${runId}`)
}

/** 按运行编号回查当轮完整血缘链路（只在该轮登记的血缘范围内追溯）。 */
export function traceRunLineage(
  runId: number,
  targetTable: string,
  maxDepth = 5,
): Promise<RunLineageTrace> {
  const query = new URLSearchParams({ target_table: targetTable, max_depth: String(maxDepth) })
  return fetchJson(`/api/pipeline_monitor/runs/${runId}/lineage?${query.toString()}`)
}
