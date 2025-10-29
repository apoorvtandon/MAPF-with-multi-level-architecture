#include "DSS.h"
#include "KivaGraph.h"          // KivaGrid
#include "ReservationTable.h"   // type only – we don’t call internals

#include <algorithm>
#include <limits>

namespace dss {

static inline int mdist(const KivaGrid& G, int a, int b) {
    return G.get_Manhattan_distance(a,b);
}

std::unordered_map<int, std::vector<TimeWindow>>
compute_coarse_time_windows(const KivaGrid& /*G*/,
                            const ReservationTable& /*rt*/,
                            int /*horizon*/)
{
    // Keep it minimal and safe: we’ll assign [0,horizon] lazily per goal below.
    return {};
}

TISTree build_tis(const KivaGrid& /*G*/,
                  const std::unordered_map<int, std::vector<TimeWindow>>& /*tws*/,
                  const GoalTW& goal,
                  int /*horizon*/)
{
    // Single node representing the goal window
    TISTree T;
    T.push_back(TISNode{goal.v, goal.tw.tL, goal.tw.tR, 0});
    return T;
}

int query_arrival_cost(const TISTree& /*T*/,
                       const KivaGrid& G,
                       int start_v,
                       int start_t,
                       const GoalTW& goal)
{
    const int travel = mdist(G, start_v, goal.v);
    const int eta    = start_t + travel;
    if (eta < goal.tw.tL) return goal.tw.tL - start_t;            // wait
    if (eta > goal.tw.tR) return std::numeric_limits<int>::max(); // miss window
    return travel;                                                // arrive in window
}

std::vector<int> order_goals_dss(const KivaGrid& G,
                                 const ReservationTable& rt,
                                 int start_v,
                                 int start_t,
                                 const std::vector<int>& goals,
                                 int horizon)
{
    auto tws = compute_coarse_time_windows(G, rt, horizon);

    struct Item { int v; int cost; };
    std::vector<Item> items; items.reserve(goals.size());

    for (int g : goals) {
        GoalTW gw{g, TimeWindow{0, std::max(1,horizon)}};
        auto T = build_tis(G, tws, gw, horizon);
        int c  = query_arrival_cost(T, G, start_v, start_t, gw);
        items.push_back(Item{g, c});
    }

    std::sort(items.begin(), items.end(),
              [](const Item& a, const Item& b){
                  if (a.cost == b.cost) return a.v < b.v;
                  return a.cost < b.cost;
              });

    std::vector<int> out; out.reserve(items.size());
    for (auto &it : items) out.push_back(it.v);
    return out;
}

} // namespace dss
