#pragma once
// RHCR-master/inc/BundlePlanner.h
//
// A thin facade to plan a multi-goal bundle for ONE agent using
//   - BasicGraph distances (Manhattan)
//   - StateTimeAStar segments with a ReservationTable
// It returns (a) the visiting order and (b) a stitched time-stamped path.
//
// This does NOT mutate global state; it's a pure function over inputs.
// You can call it from KivaSystem or any orchestration code.

#include <vector>
#include <utility>

class BasicGraph;
class ReservationTable;
struct State;

namespace mgmapf {

// (v, t) pair for stitched output
struct VT {
    int v = -1;
    int t = -1;
};

// Result for a single-agent bundle plan
struct BundlePlan {
    bool success = false;
    int total_cost = -1;                 // sum of segment costs
    std::vector<int> goal_order_idx;     // indices into input goals
    std::vector<VT> path;                // stitched time-stamped path
};

// Plan one agent's bundle.
//
// G   : warehouse graph
// rt  : reservation table (conflict/time-aware segments)
// start_v, start_t : agent's current vertex and time
// goals : vector of vertex ids to visit (unordered set => vector)
//
// returns: success flag, total cost, order, and full time-stamped path
BundlePlan plan_bundle_once(const BasicGraph& G,
                            ReservationTable& rt,
                            int start_v,
                            int start_t,
                            const std::vector<int>& goals);

// Convenience: convert stitched (v,t) pairs into your project State list.
// (We don't include project headers here to keep dependencies light.)
void convert_vt_to_states(const std::vector<VT>& in, std::vector<State>& out);

} // namespace mgmapf
