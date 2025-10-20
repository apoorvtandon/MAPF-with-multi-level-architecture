// tests/test_dvs.cpp
#include "MultiGoalPlannerDVS.h"
#include <iostream>
#include <cassert>
using namespace mgmapf;

int main() {
    // A toy "grid line" metric: dist = |u - v|
    MultiGoalPlannerDVS::DistanceOracle dist = [](int u, int v){ return std::abs(u - v); };

    // Dummy segment planner that just returns a straight-line time-stamped path.
    MultiGoalPlannerDVS::SegmentPlanner seg = [](int from, int to, int t0){
        SegmentResult r;
        int step = (to >= from) ? 1 : -1;
        int t = t0;
        r.path.push_back({from, t});
        int cur = from;
        while (cur != to) {
            cur += step;
            ++t;
            r.path.push_back({cur, t});
        }
        r.cost = std::abs(to - from);
        return r;
    };

    mgmapf::DVSConfig cfg; cfg.use_segment_planner = true;
    mgmapf::MultiGoalPlannerDVS planner(dist, seg, cfg);

    int start_v = 0, start_t = 0;
    std::vector<int> goals = {7, 3, 10};

    auto plan = planner.plan(start_v, start_t, goals);
    assert(plan.success);
    std::cout << "Order:";
    for (int i : plan.goal_order) std::cout << ' ' << goals[i];
    std::cout << "\nTotal cost: " << plan.total_cost << "\n";
    std::cout << "Path len: " << plan.full_path.size() << "\n";

    // Basic safety checks
    assert(!plan.goal_order.empty());
    assert(plan.total_cost >= 0);
    assert(!plan.full_path.empty());
    assert(plan.full_path.front().v == start_v);
    assert(plan.full_path.front().t == start_t);
    assert(plan.full_path.back().v == goals[ plan.goal_order.back() ]);
    return 0;
}
