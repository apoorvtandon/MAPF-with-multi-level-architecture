#pragma once
// RHCR-master/inc/MultiGoalAdapter.h
//
// Thin adapter that binds MultiGoalPlannerDVS to your project:
//  - Pulls metric distances via a callback (BasicGraph or heuristics)
//  - Calls your single-agent planner (SIPP / StateTimeAStar) via a callback
//
// No project headers included to avoid dependency tangles.

#include <vector>
#include <functional>
#include <utility>
#include <cstdint>
#include <limits>
#include <cassert>
#include <algorithm>

#include "MultiGoalPlannerDVS.h"   // header you added earlier

namespace mgmapf {

struct SegmentPathEntry {
    int v = -1; // vertex id
    int t = -1; // time
};

struct SegmentOutput {
    int cost = -1;                         // >=0 on success
    std::vector<SegmentPathEntry> path;    // optional; can be empty
    bool ok() const { return cost >= 0; }
};

class MultiGoalAdapter {
public:
    // Distance in edges/steps between vertices (no time)
    using DistanceFn = std::function<int(int /*u*/, int /*v*/)>;

    // Single-agent planner (SIPP / StateTimeAStar):
    // from vertex, to vertex, start time t0  -> cost + optional (v,t) path
    using SegmentFn  = std::function<SegmentOutput(int /*from*/, int /*to*/, int /*t0*/)>;

    struct Result {
        bool success = false;
        int total_cost = -1;
        std::vector<int> goal_order;              // indices into input goals
        std::vector<SegmentPathEntry> path;       // stitched time-stamped path
    };

    struct Config {
        bool stitch_segments; // if false, only visit order + heuristic cost
    };

    // Constructor WITHOUT Config: sets safe defaults (stitch on if seg_fn is provided)
    MultiGoalAdapter(DistanceFn dist_fn, SegmentFn seg_fn)
        : dist_fn_(std::move(dist_fn)), seg_fn_(std::move(seg_fn))
    {
        assert(static_cast<bool>(dist_fn_) && "DistanceFn must be provided");
        cfg_.stitch_segments = true; // default on; has effect only if seg_fn_ is non-empty
    }

    // Constructor WITH Config
    MultiGoalAdapter(DistanceFn dist_fn, SegmentFn seg_fn, const Config& cfg)
        : dist_fn_(std::move(dist_fn)), seg_fn_(std::move(seg_fn)), cfg_(cfg)
    {
        assert(static_cast<bool>(dist_fn_) && "DistanceFn must be provided");
    }

    // Plan for a bundle of goals
    Result plan_bundle(int start_v, int start_t, const std::vector<int>& goals) {
        Result R;

        // 1) Build a DVS planner using the provided callbacks
        MultiGoalPlannerDVS::DistanceOracle dist = [&](int u, int v) {
            return dist_safe(u, v);
        };

        // DVSConfig is in the mgmapf namespace (not nested in the class)
        DVSConfig dvs_cfg;
        dvs_cfg.use_segment_planner = cfg_.stitch_segments && static_cast<bool>(seg_fn_);

        MultiGoalPlannerDVS::SegmentPlanner seg;
        if (dvs_cfg.use_segment_planner) {
            // SegmentResult is also in the mgmapf namespace
            seg = [&](int from, int to, int t0) -> SegmentResult {
                SegmentResult sr;
                SegmentOutput out = seg_fn_(from, to, t0);
                if (!out.ok()) { sr.cost = -1; return sr; }
                sr.cost = out.cost;
                if (!out.path.empty()) {
                    sr.path.reserve(out.path.size());
                    for (const auto& pe : out.path) {
                        sr.path.push_back({pe.v, pe.t});
                    }
                }
                return sr;
            };
        }

        MultiGoalPlannerDVS dvs(dist, seg, dvs_cfg);

        // 2) Run the plan
        auto P = dvs.plan(start_v, start_t, goals);
        if (!P.success) {
            R.success = false;
            return R;
        }

        R.success = true;
        R.total_cost = P.total_cost;
        R.goal_order = P.goal_order;
        if (dvs_cfg.use_segment_planner) {
            R.path.reserve(P.full_path.size());
            for (const auto& e : P.full_path) R.path.push_back({e.v, e.t});
        }
        return R;
    }

private:
    int dist_safe(int u, int v) const {
        if (u < 0 || v < 0) return INF();
        int d = dist_fn_(u, v);
        if (d < 0) return INF();
        return d;
    }
    static constexpr int INF() { return std::numeric_limits<int>::max() / 8; }

private:
    DistanceFn dist_fn_;
    SegmentFn  seg_fn_;
    Config     cfg_{/*stitch_segments*/true};
};

} // namespace mgmapf
