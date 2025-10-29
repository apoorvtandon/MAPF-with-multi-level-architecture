#pragma once
#include <vector>
#include <unordered_map>

class KivaGrid;
class ReservationTable;

namespace dss {

// strictly separate from your project's "Interval" typedef
struct TimeWindow { int tL = 0; int tR = 0; };
struct GoalTW     { int v = -1; TimeWindow tw; };

// lightweight stand-in for a time-indexed graph
struct TISNode { int v = -1; int tL = 0; int tR = 0; int gCost = 0; };
using TISTree = std::vector<TISNode>;

// Coarse windows: intentionally do NOT touch RT internals
std::unordered_map<int, std::vector<TimeWindow>>
compute_coarse_time_windows(const KivaGrid& G,
                            const ReservationTable& rt,
                            int horizon);

// Build tiny TIS for the goal window
TISTree build_tis(const KivaGrid& G,
                  const std::unordered_map<int, std::vector<TimeWindow>>& tws,
                  const GoalTW& goal,
                  int horizon);

// Crude arrival cost lower bound
int query_arrival_cost(const TISTree& T,
                       const KivaGrid& G,
                       int start_v,
                       int start_t,
                       const GoalTW& goal);

// DSS ordering (returns a permutation of input goals)
std::vector<int> order_goals_dss(const KivaGrid& G,
                                 const ReservationTable& rt,
                                 int start_v,
                                 int start_t,
                                 const std::vector<int>& goals,
                                 int horizon);

} // namespace dss
