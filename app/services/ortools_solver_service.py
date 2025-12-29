import logging
from typing import Any, Dict, List, Tuple, Optional
from collections import defaultdict
from ortools.constraint_solver import pywrapcp, routing_enums_pb2

logger = logging.getLogger(__name__)

DEFAULT_FIXED_VEHICLE_COST = 1_000_000
MAX_SOLVE_SECONDS = 30

def _parse_tasks(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    tasks: List[Dict[str, Any]] = []
    for t in payload.get("tasks", []) or []:
        w = t.get("window") or [None, None]
        if w[0] is None or w[1] is None:
            continue
        tasks.append({
            "task_id": t["task_id"],
            "task_type": str(t["task_type"]).upper(),  # PICK / DROP
            "node_index": int(t["node_index"]),
            "user_id": t["user_id"],
            "window_start": int(w[0]),
            "window_end": int(w[1]),
        })
    return tasks

def _relative_time_base(tasks: List[Dict[str, Any]], buckets: List[int]) -> int:
    """
    Use earliest task window as baseline; fallback to first bucket; fallback 0.
    """
    if tasks:
        return min(t["window_start"] for t in tasks) - 3600  # 1h buffer
    if buckets:
        return int(buckets[0])
    return 0

def _task_delta(task: Dict[str, Any]) -> int:
    """
    Passenger delta for a task.
    """
    t = task.get("task_type")
    if t == "PICK":
        return 1
    if t == "DROP":
        return -1
    return 0

def solve_ortools(payload: Dict[str, Any], run_id: Optional[int] = None) -> Dict[str, Any]:
    """
    Output: OR-Tools solution (routes + stops).
    - event_type: DEPART / TASK / ARRIVE
    - task_id: always set (FK to run.routing_tasks.id)
      - DEPART: first task_id
      - ARRIVE: last task_id
      - TASK: mapped task_id for that visited node (node-based assumption)
    - passengers: computed manually from TASK sequence (PICK +1, DROP -1)
    """
    time_matrix: List[List[int]] = payload.get("time_matrix") or []
    if not time_matrix:
        return {"status": "error", "message": "time_matrix missing"}

    n = len(time_matrix)
    for row in time_matrix:
        if len(row) != n:
            return {"status": "error", "message": "time_matrix must be NxN"}

    vehicles = payload.get("vehicles") or []
    if not vehicles:
        return {"status": "error", "message": "vehicles missing"}

    tasks = _parse_tasks(payload)
    if not tasks:
        return {"status": "error", "message": "tasks missing"}

    # Build quick lookup for manual passenger tracking
    tasks_by_id: Dict[int, Dict[str, Any]] = {int(t["task_id"]): t for t in tasks}

    buckets = payload.get("buckets") or []
    base_time = _relative_time_base(tasks, buckets)

    # task_id -> relative window
    rel_windows: Dict[int, Tuple[int, int]] = {}
    for t in tasks:
        rel_windows[int(t["task_id"])] = (
            int(t["window_start"] - base_time),
            int(t["window_end"] - base_time),
        )

    # node_index -> ordered task_ids (stable deterministic order)
    node_to_task_ids: Dict[int, List[int]] = defaultdict(list)
    for t in sorted(tasks, key=lambda x: int(x["task_id"])):
        node_to_task_ids[int(t["node_index"])].append(int(t["task_id"]))

    starts = [int(v["start_index"]) for v in vehicles]
    ends = [int(v["end_index"]) for v in vehicles]
    num_vehicles = len(vehicles)

    manager = pywrapcp.RoutingIndexManager(n, num_vehicles, starts, ends)
    routing = pywrapcp.RoutingModel(manager)

    def time_cb(from_index: int, to_index: int) -> int:
        f = manager.IndexToNode(from_index)
        tnode = manager.IndexToNode(to_index)
        return int(time_matrix[f][tnode])

    transit_cb_index = routing.RegisterTransitCallback(time_cb)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_cb_index)

    # Time dimension (allows waiting)
    latest_end = max(w[1] for w in rel_windows.values())
    horizon = int(latest_end + 3600)  # 1h buffer after latest window end
    routing.AddDimension(
        transit_cb_index,
        6 * 3600,   # waiting slack
        horizon,    # horizon
        False,      # don't force start at 0
        "Time",
    )
    time_dim = routing.GetDimensionOrDie("Time")

    # Capacity dimension (kept for feasibility), but DO NOT use it for passengers output
    def demand_for_node(node: int) -> int:
        d = 0
        for t in tasks:
            if int(t["node_index"]) != node:
                continue
            if t["task_type"] == "PICK":
                d += 1
            elif t["task_type"] == "DROP":
                d -= 1
        return d

    def demand_cb(from_index: int) -> int:
        node = manager.IndexToNode(from_index)
        return int(demand_for_node(node))

    demand_cb_index = routing.RegisterUnaryTransitCallback(demand_cb)
    capacities = [int(v.get("capacity", 0)) for v in vehicles]

    routing.AddDimensionWithVehicleCapacity(
        demand_cb_index,
        0,
        capacities,
        True,   # start at 0
        "Capacity",
    )
    # NOTE: cap_dim is intentionally not used for output passengers.
    cap_dim = routing.GetDimensionOrDie("Capacity")

    # Pickup & delivery pairing by user_id (node-based)
    user_pick: Dict[int, int] = {}
    user_drop: Dict[int, int] = {}
    for t in tasks:
        uid = int(t["user_id"])
        node_idx = int(t["node_index"])
        if t["task_type"] == "PICK":
            user_pick[uid] = node_idx
        elif t["task_type"] == "DROP":
            user_drop[uid] = node_idx

    for user_id in set(user_pick) & set(user_drop):
        p = manager.NodeToIndex(int(user_pick[user_id]))
        d = manager.NodeToIndex(int(user_drop[user_id]))
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

    routes_out: List[Dict[str, Any]] = []

    for v_i, v in enumerate(vehicles):
        vehicle_id = int(v["vehicle_id"])
        index = routing.Start(v_i)

        # Build visit list (exclude end)
        visited_nodes: List[int] = []
        temp = index
        while not routing.IsEnd(temp):
            nxt = solution.Value(routing.NextVar(temp))
            if routing.IsEnd(nxt):
                break
            visited_nodes.append(manager.IndexToNode(nxt))
            temp = nxt

        # Assign task_ids in visit order, consuming from node_to_task_ids
        consumption_cursor: Dict[int, int] = defaultdict(int)
        visit_task_ids: List[int] = []

        for node in visited_nodes:
            ids = node_to_task_ids.get(int(node), [])
            cur = consumption_cursor[int(node)]
            if cur < len(ids):
                visit_task_ids.append(int(ids[cur]))
                consumption_cursor[int(node)] += 1

        if not visit_task_ids:
            continue  # unused vehicle

        first_task_id = int(visit_task_ids[0])
        last_task_id = int(visit_task_ids[-1])

        seq = 0
        stops: List[Dict[str, Any]] = []

        # Manual passenger tracking
        current_passengers = 0

        # DEPART (anchored to first task_id)
        start_node = manager.IndexToNode(index)
        t0 = int(solution.Value(time_dim.CumulVar(index)))
        stops.append({
            "sequence": seq,
            "event_type": "DEPART",
            "vehicle_id": vehicle_id,
            "node_index": int(start_node),
            "task_id": first_task_id,
            "arrival_at": int(base_time + t0),
            "departure_at": int(base_time + t0),
            "passengers": int(current_passengers),
        })

        # TASK stops: only when we consumed a task_id for that visit
        index = routing.Start(v_i)
        task_ptr = 0

        while not routing.IsEnd(index):
            nxt = solution.Value(routing.NextVar(index))
            if routing.IsEnd(nxt):
                index = nxt
                break

            node = int(manager.IndexToNode(nxt))
            ids = node_to_task_ids.get(node, [])

            used_count_for_node = sum(
                1 for s in stops
                if s["event_type"] == "TASK" and int(s["node_index"]) == node
            )

            if used_count_for_node < len(ids):
                seq += 1
                tt = int(solution.Value(time_dim.CumulVar(nxt)))
                task_id = int(visit_task_ids[task_ptr])
                task_ptr += 1

                # Update passengers based on the actual task
                task = tasks_by_id.get(task_id)
                if task is None:
                    return {"status": "error", "message": f"task_id not found in tasks_by_id: {task_id}"}

                current_passengers += _task_delta(task)

                stops.append({
                    "sequence": seq,
                    "event_type": "TASK",
                    "vehicle_id": vehicle_id,
                    "node_index": node,
                    "task_id": task_id,
                    "arrival_at": int(base_time + tt),
                    "departure_at": int(base_time + tt),
                    "passengers": int(current_passengers),
                })

            index = nxt

        # ARRIVE (anchored to last task_id)
        seq += 1
        end_node = manager.IndexToNode(index)
        te = int(solution.Value(time_dim.CumulVar(index)))
        stops.append({
            "sequence": seq,
            "event_type": "ARRIVE",
            "vehicle_id": vehicle_id,
            "node_index": int(end_node),
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
