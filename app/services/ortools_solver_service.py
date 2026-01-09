import logging
import json
import os
import socket
import urllib.request
from urllib.error import URLError
from typing import Any, Dict, List, Tuple, Optional
from collections import defaultdict
from ortools.constraint_solver import pywrapcp, routing_enums_pb2

logger = logging.getLogger(__name__)

DEFAULT_FIXED_VEHICLE_COST = 1_000_000
MAX_SOLVE_SECONDS = 30

_DEFAULT_TIMEOUT = int(os.environ.get("MAKE_HTTP_TIMEOUT_SECONDS", "120"))
_MAKE_WEBHOOK_URL = os.environ.get("MAKE_OR_TOOLS_RESULT_WEBHOOK")

def _parse_tasks(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Expected task shape:
    {
    "task_id": int,
    "task_type": "PICK" | "DROP",
    "node_index": int,          # index into payload.time_matrix (physical node)
    "user_id": int,
    "pair_key": str,            # required for multi-task users
    "window": [start_unix, end_unix]
    }
    """
    out: List[Dict[str, Any]] = []
    for t in payload.get("tasks", []) or []:
        w = t.get("window") or [None, None]
        if w[0] is None or w[1] is None:
            continue

        out.append({
            "task_id": int(t["task_id"]),
            "task_type": str(t["task_type"]).upper(),  # PICK / DROP
            "node_index": int(t["node_index"]),        # physical node index in matrix
            "user_id": int(t["user_id"]),
            "pair_key": t.get("pair_key"),
            "window_start": int(w[0]),
            "window_end": int(w[1]),
        })
    return out

def _relative_time_base(tasks: List[Dict[str, Any]], buckets: List[int]) -> int:
    """
    Use earliest task window as baseline; fallback to first bucket; fallback 0.
    """
    if tasks:
        return min(t["window_start"] for t in tasks) - 3600  # 1h buffer
    if buckets:
        return int(buckets[0])
    return 0

def _task_delta(task_type: str) -> int:
    if task_type == "PICK":
        return 1
    if task_type == "DROP":
        return -1
    return 0

def _validate_inputs(time_matrix: List[List[int]], vehicles: List[dict], tasks: List[dict]) -> Optional[str]:
    if not time_matrix:
        return "time_matrix missing"

    n = len(time_matrix)
    for row in time_matrix:
        if len(row) != n:
            return "time_matrix must be NxN"

    if not vehicles:
        return "vehicles missing"

    if not tasks:
        return "tasks missing"

    # Validate indices
    for v in vehicles:
        si = int(v.get("start_index", -1))
        ei = int(v.get("end_index", -1))
        if si < 0 or si >= n or ei < 0 or ei >= n:
            return f"vehicle start/end index out of range: start_index={si}, end_index={ei}, n={n}"

    for t in tasks:
        ni = int(t["node_index"])
        if ni < 0 or ni >= n:
            return f"task node_index out of range: task_id={t['task_id']} node_index={ni} n={n}"

    # pair_key strongly required when multiple tasks per user
    users = defaultdict(lambda: {"PICK": 0, "DROP": 0})
    for t in tasks:
        users[int(t["user_id"])][t["task_type"]] += 1

    for uid, c in users.items():
        if c["PICK"] > 1 or c["DROP"] > 1:
            if not all(tt.get("pair_key") for tt in tasks if int(tt["user_id"]) == uid):
                return (
                    f"user_id={uid} has multiple PICK/DROP tasks, but pair_key is missing. "
                    "pair_key is required to pair correctly."
                )

    return None

def _build_pairs_by_pair_key(tasks: List[Dict[str, Any]], tasknode_of_task_id: Dict[int, int]) -> List[Tuple[int, int]]:
    """
    Return list of (pickup_task_node, drop_task_node) in *routing-node space*.

    Pairing is done by pair_key:
    - each pair_key must have exactly one PICK and one DROP to be paired.
    """
    by_key: Dict[str, Dict[str, int]] = defaultdict(dict)

    for t in tasks:
        key = t.get("pair_key")
        if not key:
            continue
        tt = t["task_type"]
        if tt not in ("PICK", "DROP"):
            continue
        by_key[key][tt] = int(t["task_id"])

    pairs: List[Tuple[int, int]] = []
    for key, d in by_key.items():
        if "PICK" not in d or "DROP" not in d:
            logger.warning(f"[OR-Tools] pair_key={key} incomplete; needs both PICK and DROP. Skipping.")
            continue

        pick_task_id = int(d["PICK"])
        drop_task_id = int(d["DROP"])

        if pick_task_id not in tasknode_of_task_id or drop_task_id not in tasknode_of_task_id:
            logger.warning(f"[OR-Tools] pair_key={key} task_id missing in tasknode map. Skipping.")
            continue

        pairs.append((tasknode_of_task_id[pick_task_id], tasknode_of_task_id[drop_task_id]))

    return pairs


def solve_ortools(payload: Dict[str, Any], run_id: Optional[int] = None) -> Dict[str, Any]:
    """
    OR-Tools Solver (Task-based routing)

    - Each TASK becomes its own OR-Tools node.
    - Travel time is computed by mapping task-nodes to physical node_index in the original matrix.
    - Time windows are applied per task-node (not per physical node).
    - Pickup/Delivery constraints are applied per pair_key using task-nodes.
    - passengers are computed manually from TASK order (PICK +1, DROP -1).
    """
    time_matrix: List[List[int]] = payload.get("time_matrix") or []
    vehicles = payload.get("vehicles") or []
    tasks = _parse_tasks(payload)

    err = _validate_inputs(time_matrix, vehicles, tasks)
    if err:
        return {"status": "error", "message": err}

    buckets = payload.get("buckets") or []
    base_time = _relative_time_base(tasks, buckets)

    # Build routing-node universe
    # Create routing nodes as:
    #   - depot nodes: one per distinct physical depot index used by vehicles
    #   - task nodes: one per task (PICK/DROP), each mapped to an underlying physical node_index
    phys_starts = [int(v["start_index"]) for v in vehicles]
    phys_ends = [int(v["end_index"]) for v in vehicles]
    depot_phys_nodes: List[int] = sorted(set(phys_starts + phys_ends))

    depot_rnode_of_phys: Dict[int, int] = {phys: i for i, phys in enumerate(depot_phys_nodes)}
    depot_count = len(depot_phys_nodes)

    # Task routing node index starts after depot nodes
    task_rnode_of_task_id: Dict[int, int] = {}
    routing_to_phys: List[int] = []

    # routing_to_phys[routing_node] = physical node_index into time_matrix
    routing_to_phys.extend(depot_phys_nodes)

    for i, t in enumerate(tasks):
        rnode = depot_count + i
        task_rnode_of_task_id[int(t["task_id"])] = rnode
        routing_to_phys.append(int(t["node_index"]))

    task_id_of_rnode: Dict[int, int] = {rnode: task_id for task_id, rnode in task_rnode_of_task_id.items()}

    total_nodes = depot_count + len(tasks)

    # Map vehicles start/end to routing nodes
    starts = [depot_rnode_of_phys[int(v["start_index"])] for v in vehicles]
    ends = [depot_rnode_of_phys[int(v["end_index"])] for v in vehicles]
    num_vehicles = len(vehicles)

    manager = pywrapcp.RoutingIndexManager(total_nodes, num_vehicles, starts, ends)
    routing = pywrapcp.RoutingModel(manager)

    # Cost / Transit
    def time_cb(from_index: int, to_index: int) -> int:
        frm = manager.IndexToNode(from_index)
        to = manager.IndexToNode(to_index)
        phys_from = routing_to_phys[int(frm)]
        phys_to = routing_to_phys[int(to)]
        return int(time_matrix[phys_from][phys_to])

    transit_cb_index = routing.RegisterTransitCallback(time_cb)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_cb_index)

    # Time dimension
    rel_windows: Dict[int, Tuple[int, int]] = {
        int(t["task_id"]): (int(t["window_start"] - base_time), int(t["window_end"] - base_time))
        for t in tasks
    }
    latest_end = max(w[1] for w in rel_windows.values())
    horizon = int(latest_end + 3600)  # 1h buffer after latest window end

    routing.AddDimension(
        transit_cb_index,
        6 * 3600,   # waiting slack
        horizon,
        False,      # don't force start at 0
        "Time",
    )
    time_dim = routing.GetDimensionOrDie("Time")

    # Apply time windows PER TASK NODE (this is the key fix)
    for t in tasks:
        task_id = int(t["task_id"])
        ws, we = rel_windows[task_id]
        rnode = task_rnode_of_task_id[task_id]
        ridx = manager.NodeToIndex(rnode)
        time_dim.CumulVar(ridx).SetRange(int(ws), int(we))

    # Capacity dimension (per TASK NODE)
    # Demand is 0 at depots, +/-1 at task nodes based on PICK/DROP.
    task_by_rnode: Dict[int, Dict[str, Any]] = {task_rnode_of_task_id[int(t["task_id"])]: t for t in tasks}

    def demand_cb(from_index: int) -> int:
        rnode = manager.IndexToNode(from_index)
        t = task_by_rnode.get(int(rnode))
        if not t:
            return 0
        return _task_delta(t["task_type"])

    demand_cb_index = routing.RegisterUnaryTransitCallback(demand_cb)
    capacities = [int(v.get("capacity", 0)) for v in vehicles]

    routing.AddDimensionWithVehicleCapacity(
        demand_cb_index,
        0,          # slack
        capacities,
        True,       # start cumul at 0
        "Capacity",
    )

    # Pickup & delivery pairing (by pair_key -> task nodes)
    pairs = _build_pairs_by_pair_key(tasks, task_rnode_of_task_id)
    for pick_rnode, drop_rnode in pairs:
        p = manager.NodeToIndex(int(pick_rnode))
        d = manager.NodeToIndex(int(drop_rnode))
        routing.AddPickupAndDelivery(p, d)
        routing.solver().Add(routing.VehicleVar(p) == routing.VehicleVar(d))
        routing.solver().Add(time_dim.CumulVar(p) <= time_dim.CumulVar(d))

    # Encourage fewer vehicles (optional)
    for v_i in range(num_vehicles):
        routing.SetFixedCostOfVehicle(DEFAULT_FIXED_VEHICLE_COST, v_i)

    params = pywrapcp.DefaultRoutingSearchParameters()
    params.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PARALLEL_CHEAPEST_INSERTION
    params.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    params.time_limit.FromSeconds(int(MAX_SOLVE_SECONDS))

    solution = routing.SolveWithParameters(params)
    if solution is None:
        return {"status": "error", "message": "no feasible solution"}

    # Build output
    routes_out: List[Dict[str, Any]] = []

    # task_id -> task dict for output + passenger calc
    tasks_by_id: Dict[int, Dict[str, Any]] = {int(t["task_id"]): t for t in tasks}

    for v_i, v in enumerate(vehicles):
        vehicle_id = int(v["vehicle_id"])

        # Extract TASK stops directly from routing order (no visit_task_ids / task_ptr)
        stops: List[Dict[str, Any]] = []
        seq = 0
        current_passengers = 0
        task_ids_in_route: List[int] = []

        # DEPART anchor will use first TASK later
        start_rnode = int(manager.IndexToNode(routing.Start(v_i)))
        start_phys = int(routing_to_phys[start_rnode])
        t0 = int(solution.Value(time_dim.CumulVar(routing.Start(v_i))))

        stops.append({
            "sequence": seq,
            "event_type": "DEPART",
            "node_index": start_phys,
            "task_id": None,  # will be set after first TASK is known
            "arrival_at": int(base_time + t0),
            "departure_at": int(base_time + t0),
            "passengers": int(current_passengers),
        })

        index = routing.Start(v_i)

        while not routing.IsEnd(index):
            nxt = solution.Value(routing.NextVar(index))
            if routing.IsEnd(nxt):
                index = nxt
                break

            rnode = int(manager.IndexToNode(nxt))
            if rnode >= depot_count:
                seq += 1
                tt = int(solution.Value(time_dim.CumulVar(nxt)))

                task_id = int(task_id_of_rnode[int(rnode)])
                task_ids_in_route.append(task_id)
                task = tasks_by_id[task_id]

                current_passengers += _task_delta(task["task_type"])

                stops.append({
                    "sequence": seq,
                    "event_type": "TASK",
                    "node_index": int(task["node_index"]),
                    "task_id": int(task_id),
                    "arrival_at": int(base_time + tt),
                    "departure_at": int(base_time + tt),
                    "passengers": int(current_passengers),
                })

            index = nxt

        if not task_ids_in_route:
            continue  # unused vehicle

        first_task_id = int(task_ids_in_route[0])
        last_task_id = int(task_ids_in_route[-1])

        stops[0]["task_id"] = first_task_id

        # ARRIVE
        seq += 1
        end_rnode = int(manager.IndexToNode(index))
        end_phys = int(routing_to_phys[end_rnode])
        te = int(solution.Value(time_dim.CumulVar(index)))

        stops.append({
            "sequence": seq,
            "event_type": "ARRIVE",
            "node_index": end_phys,
            "task_id": last_task_id,
            "arrival_at": int(base_time + te),
            "departure_at": int(base_time + te),
            "passengers": int(current_passengers),
        })

        routes_out.append({
            "vehicle_id": vehicle_id,
            "vehicle_name": v.get("vehicle_name"),
            "stops": stops,
        })

    return {
        "status": "ok",
        "run_id": run_id,
        "facility_name": payload.get("facility_name"),
        "date": payload.get("date"),
        "base_time": int(base_time),
        "routes": routes_out,
        "node_ids": payload.get("node_ids"),
        "node_index": payload.get("node_index"),
    }

def post_solver_result_to_make(
    payload: Dict[str, Any],
    timeout_sec: int = _DEFAULT_TIMEOUT,
) -> Tuple[int, str]:
    """
    POST OR-Tools solver output to Make Scenario 2 webhook.
    """
    if not _MAKE_WEBHOOK_URL:
        raise RuntimeError("MAKE_OR_TOOLS_RESULT_WEBHOOK is not set")

    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    req = urllib.request.Request(
        _MAKE_WEBHOOK_URL,
        data=body,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            status = resp.getcode()
            text = resp.read().decode("utf-8")
            logger.info(f"[OR-Tools] Posted solver result to Make (status={status})")
            return status, text
    except (socket.timeout, URLError) as e:
        logger.error("[OR-Tools] Make webhook request failed", exc_info=e)
        raise TimeoutError("Make webhook request timed out") from e
