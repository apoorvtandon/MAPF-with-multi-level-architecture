// RHCR-master/src/MultiGoalSIPPAdapter.cpp
#include "MultiGoalSIPPAdapter.h"

#include "MultiGoalAdapter.h"       // our generic adapter
#include <algorithm>
#include <cstdlib>
#include <cmath>

// Now pull real project headers.
#include "BasicGraph.h"             // must provide: int get_Manhattan_distance(int,int) const;
#include "ReservationTable.h"       // reservation table used by StateTimeAStar
#include "StateTimeAStar.h"         // must provide: Path run(const BasicGraph&, const State&, const vector<pair<int,int>>&, ReservationTable&)
#include "States.h"                 // defines State (location, timestep, orientation, etc.)

namespace mgmapf {

// ---- tiny helpers to convert between Path <-> SegmentOutput ----
static SegmentOutput make_segment_from_statetime_astar(const BasicGraph& G,
                                                       ReservationTable& rt,
                                                       int from, int to, int t0)
{
    SegmentOutput out;

    // Build a start State from (from, t0). Orientation: we set to -1 (or 0) if not used.
    State start;
    start.location  = from;
    start.timestep  = t0;
    // If your State has orientation (it does), set a neutral value that your solver accepts:
    start.orientation = -1; // many solvers treat -1 as "don't-care" when the graph is 4-neighbor grid.
    // If you require a valid orientation, you may choose 0 and let neighbors expand it.

    // Single-goal vector: (goal_vertex, earliest_arrival_time)
    std::vector<std::pair<int,int>> goal_locs;
    goal_locs.emplace_back(to, t0); // reach 'to' no earlier than t0 (StateTimeAStar handles waiting)

    // Run the time-aware A*; it returns a Path (vector<State>) or similar
    StateTimeAStar lowlevel;
    Path path = lowlevel.run(G, start, goal_locs, rt);

    if (path.size() == 0) {
        // No path found -> mark failure
        out.cost = -1;
        return out;
    }

    // cost: in RHCR codebase, common convention is "path_cost = goal->getFVal()" inside run().
    // If your Path doesn't carry cost, use path length minus one (steps) as a safe fallback.
    // We read cost from the solver via its last run; if not exposed, approximate.
    // Here, we approximate as step count:
    int steps = static_cast<int>(path.size() > 0 ? path.size() - 1 : 0);
    out.cost = steps;

    // convert to (v,t)
    out.path.reserve(path.size());
    for (const auto& s : path) {
        SegmentPathEntry pe;
        pe.v = s.location;
        pe.t = s.timestep;
        out.path.push_back(pe);
    }

    return out;
}

MGRunResult MultiGoalSIPPAdapterRunner::plan_bundle(const BasicGraph& G,
                                                    ReservationTable& rt,
                                                    int start_v,
                                                    int start_t,
                                                    const std::vector<int>& goals)
{
    MGRunResult R;

    // DistanceFn: use BasicGraph's manhattan grid metric (always available in Kiva maps)
    MultiGoalAdapter::DistanceFn dist = [&](int u, int v) -> int {
        return G.get_Manhattan_distance(u, v);
    };

    // SegmentFn: call StateTimeAStar::run (time-aware, reservation-aware)
    MultiGoalAdapter::SegmentFn seg = [&](int from, int to, int t0) -> SegmentOutput {
        return make_segment_from_statetime_astar(G, rt, from, to, t0);
    };

    MultiGoalAdapter::Config cfg;
    cfg.stitch_segments = true;

    MultiGoalAdapter mg(dist, seg, cfg);

    auto out = mg.plan_bundle(start_v, start_t, goals);
    R.success     = out.success;
    R.total_cost  = out.total_cost;
    R.goal_order  = out.goal_order;
    R.path        = out.path;
    return R;
}

} // namespace mgmapf
