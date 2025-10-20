// RHCR-master/tests/test_dvs_adapter.cpp
#include "MultiGoalAdapter.h"
#include <iostream>
#include <vector>
#include <cassert>
#include <cstdlib>
#include <cmath>

using namespace mgmapf;

// --- TEMP stand-ins ---
// 1) DistanceFn: "grid line" metric for quick testing: dist(u,v) = |u - v|
static int toy_distance(int u, int v) { return std::abs(u - v); }

// 2) SegmentFn: straight line with 1 step/time per move; returns a time-stamped path
static SegmentOutput toy_segment(int from, int to, int t0) {
    SegmentOutput o;
    int step = (to >= from) ? 1 : -1;
    int cur = from;
    int t   = t0;
    o.path.push_back({cur, t});
    while (cur != to) { cur += step; ++t; o.path.push_back({cur, t}); }
    o.cost = std::abs(to - from);
    return o;
}

int main() {
    MultiGoalAdapter::DistanceFn dist = toy_distance;
    MultiGoalAdapter::SegmentFn  seg  = toy_segment;

    MultiGoalAdapter::Config cfg;
    cfg.stitch_segments = true;

    MultiGoalAdapter mg(dist, seg, cfg);

    int start_v = 0, start_t = 0;
    std::vector<int> goals = {7, 3, 10};

    auto R = mg.plan_bundle(start_v, start_t, goals);
    if (!R.success) {
        std::cerr << "Adapter plan failed\n";
        return 1;
    }

    std::cout << "Visiting order (by vertex):";
    for (int gi : R.goal_order) std::cout << ' ' << goals[gi];
    std::cout << "\nTotal cost: " << R.total_cost << "\n";
    std::cout << "Stitched path length: " << R.path.size() << "\n";

    // Safety checks
    assert(!R.goal_order.empty());
    assert(R.total_cost >= 0);
    assert(!R.path.empty());
    assert(R.path.front().v == start_v);
    assert(R.path.front().t == start_t);

    std::cout << "DVS adapter test OK\n";
    return 0;
}
