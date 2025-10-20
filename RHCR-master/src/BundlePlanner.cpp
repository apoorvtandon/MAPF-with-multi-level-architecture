// RHCR-master/src/BundlePlanner.cpp
#include "BundlePlanner.h"

#include "MultiGoalSIPPAdapter.h"   // our concrete runner (BasicGraph + StateTimeAStar)
#include "BasicGraph.h"
#include "ReservationTable.h"
#include "States.h"                 // defines State (location, timestep, orientation, etc.)

namespace mgmapf {

BundlePlan plan_bundle_once(const BasicGraph& G,
                            ReservationTable& rt,
                            int start_v,
                            int start_t,
                            const std::vector<int>& goals)
{
    BundlePlan R;

    // guard trivial
    if (start_v < 0 || start_t < 0) return R;

    MultiGoalSIPPAdapterRunner runner;
    auto res = runner.plan_bundle(G, rt, start_v, start_t, goals);

    R.success          = res.success;
    R.total_cost       = res.total_cost;
    R.goal_order_idx   = res.goal_order;

    if (res.success) {
        R.path.reserve(res.path.size());
        for (const auto& e : res.path) {
            R.path.push_back({e.v, e.t});
        }
    }
    return R;
}

void convert_vt_to_states(const std::vector<VT>& in, std::vector<State>& out)
{
    out.clear();
    out.reserve(in.size());
    // orientation: if your low-level uses it, you can set a neutral default (e.g., -1)
    int last_orient = -1;
    for (const auto& e : in) {
        State s;
        s.location    = e.v;
        s.timestep    = e.t;
        s.orientation = last_orient; // safe default; adjust later if needed
        out.push_back(s);
    }
}

} // namespace mgmapf
