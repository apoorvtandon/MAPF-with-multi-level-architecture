#pragma once
// RHCR-master/inc/MultiGoalSIPPAdapter.h
//
// Bridges MultiGoalAdapter ↔ your real project types.
// - Distances: BasicGraph.get_Manhattan_distance(u,v) (safe + always available)
// - Segments:  StateTimeAStar::run(G, start_state, { {goal, earliest_t} }, rt)
//
// No changes to existing code are required; you only include and call this.

#include <vector>
#include <utility>
#include <cstdint>
#include <cassert>

#include "MultiGoalAdapter.h"

// We only forward-declare to avoid accidental header explosions here.
// The .cpp will include the real headers.
class BasicGraph;
class ReservationTable;
struct State;

namespace mgmapf {

struct MGRunResult {
    bool success = false;
    int total_cost = -1;                         // sum of stitched segment costs
    std::vector<int> goal_order;                 // indices into input 'goals'
    std::vector<SegmentPathEntry> path;          // full time-stamped path (v,t)
};

// A minimal runner that owns no resources; it just references G and RT at call time.
class MultiGoalSIPPAdapterRunner {
public:
    MultiGoalSIPPAdapterRunner() = default;

    // Plan visiting order + stitched path using:
    // - BasicGraph& G   for metric distances (manhattan or heuristics)
    // - ReservationTable& rt for time-space constraints
    //
    // start_v: vertex id for agent
    // start_t: current time for that agent
    // goals:   vertex ids to visit (unordered set -> vector)
    MGRunResult plan_bundle(const BasicGraph& G,
                            ReservationTable& rt,
                            int start_v,
                            int start_t,
                            const std::vector<int>& goals);
};

} // namespace mgmapf
