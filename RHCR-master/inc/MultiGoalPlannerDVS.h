// inc/MultiGoalPlannerDVS.hpp
// Capacity-aware Multi-Goal planner (DVS variant) – header-only, no external deps.
// It chooses a visiting order via an MST-informed A* over goal subsets,
// and (optionally) asks a user-provided "segment planner" to stitch time-stamped paths.
//
// Usage:
//   - Provide a DistanceOracle: dist(u,v) non-negative metric cost.
//   - (Optional) Provide a SegmentPlanner: plan(u, v, t0) -> returns {cost, pathEntries}.
//   - Call plan(startV, startT, goals).
//
// Safety: no raw pointers, bounds-checked where relevant, and defensive checks.
//

#pragma once
#include <vector>
#include <queue>
#include <limits>
#include <cstdint>
#include <functional>
#include <utility>
#include <algorithm>
#include <unordered_map>
#include <cassert>

// ------------------------------- Public types -------------------------------

namespace mgmapf {

// A single time-stamped vertex along a path (you can remap this to your PathEntry).
struct TimedVertex {
    int v = -1;
    int t = -1;
};

// Result of segment planning between two vertices (with start time).
struct SegmentResult {
    int cost = -1;                    // non-negative on success
    std::vector<TimedVertex> path;    // inclusive path with timestamps (may be empty if you only need cost)
    bool ok() const { return cost >= 0; }
};

// Config switches (keep it simple for now)
struct DVSConfig {
    bool use_segment_planner = false;  // if false, we’ll only return visiting order (no stitched path)
};

// Output of the full multi-goal planning
struct DVSPlan {
    // visiting order of goals (indices into input `goals`)
    std::vector<int> goal_order;

    // cumulative cost (sum of segment costs), or sum of heuristic distances if segment planner not used
    int total_cost = -1;

    // stitched path if segment planner was provided and used
    std::vector<TimedVertex> full_path;

    bool success = false;
};

// ------------------------------- Planner class ------------------------------

class MultiGoalPlannerDVS {
public:
    using DistanceOracle = std::function<int(int,int)>; // metric distance >= 0 (e.g., shortest path length)
    using SegmentPlanner = std::function<SegmentResult(int from, int to, int t0)>; // returns cost + time-stamped path

    explicit MultiGoalPlannerDVS(DistanceOracle dist, SegmentPlanner seg = nullptr, DVSConfig cfg = {})
    : dist_(std::move(dist)), seg_(std::move(seg)), cfg_(cfg) {}

    // Entry point
    // start_v:   starting vertex id
    // start_t:   starting time (>=0)
    // goals:     list of goal vertex ids (unique recommended)
    DVSPlan plan(int start_v, int start_t, const std::vector<int>& goals) {
        DVSPlan out;
        out.total_cost = 0;
        out.success = false;

        if (start_v < 0 || start_t < 0) return out;
        if (goals.empty()) { // trivial
            out.success = true;
            out.goal_order = {};
            if (cfg_.use_segment_planner && seg_) {
                out.full_path.push_back({start_v, start_t});
            }
            return out;
        }

        // Build an index map for goals → [0..m-1]
        const int m = static_cast<int>(goals.size());
        std::vector<int> idx_to_goal = goals;                   // goal_idx -> vertex
        std::unordered_map<int,int> goal_to_idx; goal_to_idx.reserve(m*2);
        for (int i = 0; i < m; ++i) goal_to_idx[idx_to_goal[i]] = i;

        // Precompute distances needed by MST & expansions: dist(current, goal) and inter-goal distances
        std::vector<int> d_start(m, INF());
        for (int i = 0; i < m; ++i) d_start[i] = safe_dist(start_v, idx_to_goal[i]);

        std::vector<std::vector<int>> d_goal(m, std::vector<int>(m, 0));
        for (int i = 0; i < m; ++i)
            for (int j = 0; j < m; ++j)
                d_goal[i][j] = (i==j) ? 0 : safe_dist(idx_to_goal[i], idx_to_goal[j]);

        // A* over states: (last_goal_index or -1 for start, visited_mask)
        struct Node {
            int last;      // -1 means "at start_v", else index into goals [0..m-1]
            int mask;      // visited subset bitmask
            int g;         // cost so far
            int f;         // g + h
        };
        auto cmp = [](const Node& a, const Node& b){ return a.f > b.f; };
        std::priority_queue<Node, std::vector<Node>, decltype(cmp)> open(cmp);

        // parent reconstruction
        struct Parent { int plast=-2; int pmask=0; int took=-1; };
        // key = (last,mask) flattened
        auto key = [m](int last, int mask)->uint64_t {
            return (static_cast<uint64_t>(static_cast<uint32_t>(last+1)) << 32) |
                   static_cast<uint32_t>(mask);
        };
        std::unordered_map<uint64_t, Parent> parent;
        std::unordered_map<uint64_t, int> best_g;

        // initial node
        Node s{-1, 0, 0, 0};
        s.f = s.g + mst_heuristic(-1, 0, d_start, d_goal);
        open.push(s);
        best_g[key(s.last, s.mask)] = 0;

        const int goal_mask_all = (m==32? -1 : ((1<<m)-1)); // safe for m<=31; for m>=32 you’d use 64-bit mask
        bool found = false;
        Node goal_node{};

        while (!open.empty()) {
            Node cur = open.top(); open.pop();
            // prune by recorded best g
            auto kcur = key(cur.last, cur.mask);
            auto itg = best_g.find(kcur);
            if (itg != best_g.end() && cur.g > itg->second) continue;

            if (cur.mask == goal_mask_all) {
                found = true;
                goal_node = cur;
                break;
            }

            // expand to any unvisited goal
            for (int nxt = 0; nxt < m; ++nxt) if (!(cur.mask & (1<<nxt))) {
                int step_cost = (cur.last == -1) ? d_start[nxt] : d_goal[cur.last][nxt];
                if (step_cost >= INF()) continue; // unreachable; skip safely

                Node nxtn;
                nxtn.last = nxt;
                nxtn.mask = cur.mask | (1<<nxt);
                nxtn.g = cur.g + step_cost;
                nxtn.f = nxtn.g + mst_heuristic(nxtn.last, nxtn.mask, d_start, d_goal);

                auto kn = key(nxtn.last, nxtn.mask);
                auto it = best_g.find(kn);
                if (it == best_g.end() || nxtn.g < it->second) {
                    best_g[kn] = nxtn.g;
                    parent[kn] = Parent{cur.last, cur.mask, nxt};
                    open.push(nxtn);
                }
            }
        }

        if (!found) return out; // impossible

        // Reconstruct visiting order (goal indices), reverse
        std::vector<int> order_rev;
        {
            int last = goal_node.last;
            int mask = goal_node.mask;
            while (!(last == -1 && mask == 0)) {
                auto itp = parent.find(key(last, mask));
                assert(itp != parent.end());
                order_rev.push_back(itp->second.took);
                last = itp->second.plast;
                mask = itp->second.pmask;
            }
        }
        std::reverse(order_rev.begin(), order_rev.end());
        out.goal_order = order_rev;
        out.total_cost = goal_node.g;

        // If no segment planner requested/provided, we’re done
        if (!cfg_.use_segment_planner || !seg_) {
            out.success = true;
            return out;
        }

        // Stitch time-stamped path using segment planner
        int cur_v = start_v;
        int cur_t = start_t;
        out.full_path.clear();
        // seed with start
        if (out.full_path.empty() || out.full_path.back().v != cur_v || out.full_path.back().t != cur_t)
            out.full_path.push_back({cur_v, cur_t});

        int stitched_cost = 0;
        for (int gi : out.goal_order) {
            int target_v = idx_to_goal[gi];
            SegmentResult segres = seg_(cur_v, target_v, cur_t);
            if (!segres.ok()) { // fail safely
                out.success = false;
                return out;
            }
            // append but avoid duplicating the first node if same as last
            if (!segres.path.empty()) {
                if (!out.full_path.empty() &&
                    out.full_path.back().v == segres.path.front().v &&
                    out.full_path.back().t == segres.path.front().t)
                {
                    out.full_path.insert(out.full_path.end(), segres.path.begin()+1, segres.path.end());
                } else {
                    out.full_path.insert(out.full_path.end(), segres.path.begin(), segres.path.end());
                }
                // update current
                cur_v = segres.path.back().v;
                cur_t = segres.path.back().t;
            } else {
                // no explicit path returned; advance time conservatively
                cur_v = target_v;
                cur_t += segres.cost;
                out.full_path.push_back({cur_v, cur_t});
            }
            stitched_cost += segres.cost;
        }

        // prefer stitched cost if available and consistent
        if (stitched_cost >= 0) out.total_cost = stitched_cost;
        out.success = true;
        return out;
    }

private:
    // Heuristic = MST over remaining goals ∪ {anchor}, where anchor is either start or last goal.
    // We approximate MST cost using a cheap Prim over precomputed distances.
    static int mst_heuristic(int last, int mask,
                             const std::vector<int>& d_start,
                             const std::vector<std::vector<int>>& d_goal)
    {
        const int m = static_cast<int>(d_start.size());
        if (mask == ((m==32? -1 : ((1<<m)-1)))) return 0; // all visited

        // Collect nodes: remaining goal indices; choose anchor source
        std::vector<int> rem;
        rem.reserve(m);
        for (int i = 0; i < m; ++i) if (!(mask & (1<<i))) rem.push_back(i);
        if (rem.empty()) return 0;

        // Prim’s MST starting from an anchor: last or "virtual start"
        auto dist_edge = [&](int a, int b)->int {
            // a,b are goal indices in [0..m-1]
            if (a < 0) { // -1 denotes "start"
                return d_start[b];
            }
            return d_goal[a][b];
        };

        // Choose best anchor: min(d_start to any remaining) or from 'last' if provided
        int anchor = last; // goal index or -1 for start
        if (anchor == -1) {
            int best = INF();
            for (int g : rem) best = std::min(best, d_start[g]);
            if (best >= INF()) return INF(); // unreachable case
        }

        // Prim: keep a set S; track min edge to S
        const int R = static_cast<int>(rem.size());
        std::vector<bool> inS(R, false);
        std::vector<int> keyEdge(R, INF());

        // initialize: distances from anchor to each rem
        for (int i = 0; i < R; ++i) {
            int gi = rem[i];
            keyEdge[i] = dist_edge(anchor, gi);
        }

        int mst_cost = 0;
        for (int it = 0; it < R; ++it) {
            // pick u with min key
            int u = -1, ku = INF();
            for (int i = 0; i < R; ++i) if (!inS[i] && keyEdge[i] < ku) { ku = keyEdge[i]; u = i; }
            if (u == -1 || ku >= INF()) return INF(); // disconnected
            inS[u] = true;
            mst_cost += ku;

            // relax neighbors
            for (int v = 0; v < R; ++v) if (!inS[v]) {
                int gu = rem[u], gv = rem[v];
                int w = d_goal[gu][gv];
                if (w < keyEdge[v]) keyEdge[v] = w;
            }
        }
        return mst_cost;
    }

    int safe_dist(int u, int v) const {
        if (u < 0 || v < 0) return INF();
        int d = dist_(u, v);
        if (d < 0) return INF();
        return d;
    }

    static constexpr int INF() { return std::numeric_limits<int>::max() / 8; }

private:
    DistanceOracle dist_;
    SegmentPlanner seg_;
    DVSConfig cfg_;
};

} // namespace mgmapf
