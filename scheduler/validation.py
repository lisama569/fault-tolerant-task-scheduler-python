"""受理阶段校验：规格合法性、重复标识、依赖缺失与循环依赖检测。

校验顺序（每条非法任务产生一条带可区分 code 的 Rejection）：
  1. INVALID_SPEC      任务自身规格非法（空标识、重试上限/租约时间非法）
  2. DUPLICATE_TASK_ID 批次内出现重复标识（保留首次出现，后续判重）
  3. MISSING_DEPENDENCY 依赖指向批次内不存在或已被拒绝的标识
  4. CYCLIC_DEPENDENCY 处于环上，或传递依赖到某个环（永远无法启动）
"""

from __future__ import annotations

from .models import Rejection, TaskSpec


def validate_specs(specs: list[TaskSpec]) -> list[Rejection]:
    """校验一个待提交批次，按输入顺序返回每条拒绝记录。"""
    # (首次出现的下标, Rejection)
    rejections: dict[str, tuple[int, Rejection]] = {}
    valid: dict[str, TaskSpec] = {}
    seen_ids: set[str] = set()

    def reject(idx: int, tid: str, code: str, reason: str) -> None:
        # 同一标识只保留第一个被判定出的原因
        if tid not in rejections:
            rejections[tid] = (
                idx,
                Rejection(task_id=tid, code=code, reason=reason, index=idx),
            )

    # 1 & 2. 单条规格合法性 + 批次内重复标识
    for idx, spec in enumerate(specs):
        tid = spec.task_id
        if not isinstance(tid, str) or not tid.strip():
            reject(idx, tid or "", "INVALID_SPEC", "任务标识不能为空")
            continue
        if spec.max_attempts < 1:
            reject(idx, tid, "INVALID_SPEC",
                   f"max_attempts 必须 >= 1，实际为 {spec.max_attempts}")
            continue
        if spec.lease_timeout_ms <= 0:
            reject(idx, tid, "INVALID_SPEC",
                   f"lease_timeout_ms 必须 > 0，实际为 {spec.lease_timeout_ms}")
            continue
        if tid in seen_ids:
            reject(idx, tid, "DUPLICATE_TASK_ID",
                   f"任务标识 {tid!r} 在同一批次中重复出现")
            continue
        seen_ids.add(tid)
        valid[tid] = spec
    # 3. 依赖缺失（依赖不在“可受理任务”集合中，即不存在或已被拒绝）
    index_of = {spec.task_id: i for i, spec in enumerate(specs)}
    has_all_deps: dict[str, bool] = {}
    for tid, spec in valid.items():
        missing = sorted({d for d in spec.depends_on if d not in valid})
        if missing:
            reject(index_of[tid], tid, "MISSING_DEPENDENCY",
                   f"任务 {tid!r} 依赖的任务不存在或未被受理: {missing}")
            has_all_deps[tid] = False
        else:
            has_all_deps[tid] = True

    # 4. 循环依赖：在依赖完整的任务组成的图上求强连通分量
    graph = {
        tid: list(spec.depends_on)
        for tid, spec in valid.items()
        if has_all_deps.get(tid)
    }
    for tid in _cyclic_nodes(graph):
        reject(index_of[tid], tid, "CYCLIC_DEPENDENCY",
               f"任务 {tid!r} 位于循环依赖中（或传递依赖到循环）")

    return [r for _, r in sorted(rejections.values(), key=lambda x: x[0])]


def _cyclic_nodes(graph: dict[str, list[str]]) -> set[str]:
    """返回所有处在环上、以及沿依赖边能到达环的节点。"""
    index_counter = [0]
    stack: list[str] = []
    on_stack: set[str] = set()
    indices: dict[str, int] = {}
    lowlink: dict[str, int] = {}
    on_cycle: set[str] = set()

    def strongconnect(v: str) -> None:
        indices[v] = index_counter[0]
        lowlink[v] = index_counter[0]
        index_counter[0] += 1
        stack.append(v)
        on_stack.add(v)

        for w in graph.get(v, []):
            if w not in indices:
                strongconnect(w)
                lowlink[v] = min(lowlink[v], lowlink[w])
            elif w in on_stack:
                lowlink[v] = min(lowlink[v], indices[w])

        if lowlink[v] == indices[v]:
            component: list[str] = []
            while True:
                w = stack.pop()
                on_stack.discard(w)
                component.append(w)
                if w == v:
                    break
            if len(component) > 1 or (len(component) == 1 and v in graph.get(v, [])):
                on_cycle.update(component)

    import sys

    sys.setrecursionlimit(max(1000, len(graph) * 4 + 100))
    for node in graph:
        if node not in indices:
            strongconnect(node)

    # 沿依赖边传播：能到达环的节点同样永远无法启动
    reaches_cycle: set[str] = set(on_cycle)
    changed = True
    while changed:
        changed = False
        for node, deps in graph.items():
            if node in reaches_cycle:
                continue
            if any(d in reaches_cycle for d in deps):
                reaches_cycle.add(node)
                changed = True
    return reaches_cycle
