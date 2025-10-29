#include "KivaSystem.h"
#include "WHCAStar.h"
#include "ECBS.h"
#include "LRAStar.h"
#include "PBS.h"

#include "SIPP.h"
#include "ReservationTable.h"

#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <random>
#include <iostream>
#include <fstream>
#include <list>
#include <queue>
#include <deque>
#include <sstream>
#include <tuple>
#include <climits>

using std::cout;
using std::endl;

// ============================================================================
// File-local flags / knobs (no header changes needed)
// ============================================================================
static bool  dss_debug_print           = false;   // flip for SII prints
static const int REPLAN_COOLDOWN_TICKS = 3;       // per-agent replan hysteresis
static const int SII_REBUILD_PERIOD    = 3;       // lazy global SII rebuild cadence
static const bool LIGHTWEIGHT_MODE     = true;    // global cheap mode

// Chokepoint-lite knobs
static const int  HOT_CHOKE_THRESH     = 3;       // hot if ≥ traversals within horizon
static const int  CHOKE_PENALTY_W      = 10;      // added to goal score per heat unit
static const int  CHOKE_LOOKAHEAD_CAP  = 2;       // only penalize first 2 hot chokes on the greedy path

static std::vector<int> g_replan_cooldown;        // sized in initialize()

static inline int effective_planning_window(int base_window, int agents)
{
    if (!LIGHTWEIGHT_MODE) return base_window;
    if      (agents >= 256) return std::max(8,   base_window / 3);
    else if (agents >= 128) return std::max(12,  base_window / 2);
    else if (agents >= 64)  return std::max(16,  (base_window * 2) / 3);
    return base_window;
}

// ============================================================================
// Small utilities (all safe-by-construction)
// ============================================================================
static inline int grid_vertex_count(const KivaGrid& G)
{
    const int R = G.get_rows();
    const int C = G.get_cols();
    if (R <= 0 || C <= 0) return 0;
    return R * C;
}

static inline int clamp_vertex(const KivaGrid& G, int v)
{
    const int N = grid_vertex_count(G);
    if (N <= 0) return 0;
    if (v < 0) return 0;
    if (v >= N) return N - 1;
    return v;
}

static inline int pick_random_endpoint_except(const KivaGrid& G, int avoid)
{
    if (G.endpoints.empty())
        return clamp_vertex(G, avoid);

    int goal = G.endpoints[std::max(0, rand() % (int)G.endpoints.size())];
    int guard = 16;
    while (guard-- > 0 && goal == avoid && !G.endpoints.empty())
        goal = G.endpoints[rand() % (int)G.endpoints.size()];

    if (goal == avoid) {
        for (int e : G.endpoints) { if (e != avoid) { goal = e; break; } }
        if (goal == avoid) goal = G.endpoints.front();
    }
    return clamp_vertex(G, goal);
}

static inline const State& safe_path_at(std::vector<std::vector<State>>& paths,
                                        const KivaGrid& G,
                                        int k, int t,
                                        bool consider_rotation)
{
    if (k < 0) k = 0;
    if (k >= (int)paths.size()) paths.resize(k + 1);

    if (paths[k].empty()) {
        int home = (!G.agent_home_locations.empty() && k < (int)G.agent_home_locations.size())
                 ? clamp_vertex(G, G.agent_home_locations[k])
                 : (G.endpoints.empty() ? 0 : clamp_vertex(G, G.endpoints[k % (int)G.endpoints.size()]));
        int ori  = consider_rotation ? 0 : -1;
        paths[k].push_back(State(home, 0, ori));
    }
    while ((int)paths[k].size() <= t) {
        const State& last = paths[k].back();
        const int loc = clamp_vertex(G, last.location);
        paths[k].push_back(State(loc, last.timestep + 1, last.orientation));
    }
    return paths[k][t];
}

static inline bool is_endpoint_safe(const KivaGrid& G, int v)
{
    if (v < 0) return false;
    for (int e : G.endpoints) if (e == v) return true;
    return false;
}

static inline void clean_goals(const KivaGrid& G, int start_v, std::vector<int>& goals)
{
    for (int& g : goals) g = clamp_vertex(G, g);
    const int sv = clamp_vertex(G, start_v);
    goals.erase(std::remove(goals.begin(), goals.end(), sv), goals.end());
    std::sort(goals.begin(), goals.end());
    goals.erase(std::unique(goals.begin(), goals.end()), goals.end());
}

static inline std::vector<std::pair<int,int>>
safe_step_path(const KivaGrid& G, int v0, int t0, int v_goal)
{
    std::vector<std::pair<int,int>> out;
    v0     = clamp_vertex(G, v0);
    v_goal = clamp_vertex(G, v_goal);

    out.emplace_back(v0, t0);
    if (v0 == v_goal) return out;

    int t = t0;
    int v = v0;

    auto step_towards = [&](int target){
        int best = -1;
        int best_d = G.get_Manhattan_distance(v, target);
        for (int nb : G.get_neighbors(v)) {
            nb = clamp_vertex(G, nb);
            int d = G.get_Manhattan_distance(nb, target);
            if (d < best_d) { best_d = d; best = nb; }
        }
        if (best == -1) return false;
        v = best;
        ++t;
        out.emplace_back(v, t);
        return true;
    };

    const int CAP = 20000;
    int guard = 0;
    while (v != v_goal && guard++ < CAP) {
        if (!step_towards(v_goal)) break;
    }
    return out;
}

// ============================================================================
// Corridor Tokens (NEW): lightweight directed edge reservations on chokepoints
// ============================================================================
struct CorridorToken {
    int u, v, t, aid;
};
static std::vector<CorridorToken> g_corridor_tokens;

static inline void tokens_gc(int now_t, int keep_ahead_T)
{
    // Keep tokens within [now_t-1, now_t+keep_ahead_T]
    const int lo = std::max(0, now_t - 1);
    const int hi = std::max(lo, now_t + keep_ahead_T);
    size_t w = 0;
    for (size_t i = 0; i < g_corridor_tokens.size(); ++i) {
        int tt = g_corridor_tokens[i].t;
        if (tt >= lo && tt <= hi) {
            g_corridor_tokens[w++] = g_corridor_tokens[i];
        }
    }
    g_corridor_tokens.resize(w);
}

static inline void tokens_add_from_path(const KivaGrid& G, const std::vector<State>& path, int aid)
{
    if (path.size() < 2) return;
    for (size_t i = 1; i < path.size(); ++i) {
        int u = clamp_vertex(G, path[i-1].location);
        int v = clamp_vertex(G, path[i].location);
        int t = path[i].timestep;
        if (u == v) continue;
        // only on corridor-like degree-2 vertices to stay light
        if (!(u >= 0 && v >= 0)) continue;
        bool chokey = false;
        // we'll mark if either endpoint is a chokepoint (set later once g_is_choke is built)
        extern std::vector<char> g_is_choke; // forward ref to file-local static
        if (u < (int)g_is_choke.size() && g_is_choke[u]) chokey = true;
        if (v < (int)g_is_choke.size() && g_is_choke[v]) chokey = true;
        if (!chokey) continue;

        g_corridor_tokens.push_back({u, v, t, aid});
    }
}

static inline void tokens_collect_initial_reservations(int now_t, int horizonT, int current_agent,
                                                       std::list<std::tuple<int,int,int>>& out)
{
    for (const auto& tok : g_corridor_tokens) {
        if (tok.aid == current_agent) continue; // don't block self
        if (tok.t < now_t || tok.t > horizonT) continue;
        out.emplace_back(tok.u, tok.v, tok.t);
    }
}

// ============================================================================
// Build Reservation Table from teammates + tokens
// ============================================================================
static inline void build_rt_from_teammates_safe(
    const KivaGrid& G,
    const std::vector<Path>& paths,
    int current_agent,
    int horizon_t,
    bool crop_to_horizon,
    bool consider_rotation,
    ReservationTable& rt)
{
    std::vector<Path> others(paths.size());
    for (int i = 0; i < (int)paths.size(); ++i) {
        if (i == current_agent) continue;
        Path tmp;

        if (crop_to_horizon && horizon_t >= 0) {
            for (const auto& s : paths[i]) {
                if (s.timestep <= horizon_t) tmp.push_back(s);
                else break;
            }
            if (tmp.empty() && !paths[i].empty())
                tmp.push_back(paths[i].front());
        } else {
            tmp = paths[i];
        }

        if (tmp.empty()) {
            int home = (!G.agent_home_locations.empty())
                     ? clamp_vertex(G, G.agent_home_locations.front())
                     : 0;
            int ori = consider_rotation ? 0 : -1;
            tmp.push_back(State(home, 0, ori));
        } else {
            tmp[0].location = clamp_vertex(G, tmp[0].location);
            for (size_t t = 1; t < tmp.size(); ++t) {
                tmp[t].location = clamp_vertex(G, tmp[t].location);
                if (tmp[t].timestep <= tmp[t-1].timestep) tmp[t].timestep = tmp[t-1].timestep + 1;
            }
        }

        others[i] = std::move(tmp);
    }

    // Corridor tokens as initial reservations (u,v,t)
    std::list<std::tuple<int,int,int>> initial_res;
    tokens_collect_initial_reservations(std::max(0, horizon_t - 1024), horizon_t, current_agent, initial_res);
    rt.build(others, initial_res, current_agent);
}

// ============================================================================
// DSS: Safe Interval Index (per-vertex)
// ============================================================================
struct SIInterval { int t0, t1; }; // inclusive

class SafeIntervalIndex {
public:
    void init(int n_vertices, int horizon) {
        horizon_ = horizon;
        si_.assign(n_vertices, {});
    }
    void set_intervals_for_vertex(int v, std::vector<SIInterval>&& ivals) {
        if (v >= 0 && v < (int)si_.size()) si_[v] = std::move(ivals);
    }
    const std::vector<SIInterval>& at(int v) const {
        static const std::vector<SIInterval> kEmpty;
        if (v < 0 || v >= (int)si_.size()) return kEmpty;
        return si_[v];
    }
    int horizon() const { return horizon_; }
    int vertex_count() const { return (int)si_.size(); }
private:
    int horizon_ = 0;
    std::vector<std::vector<SIInterval>> si_;
};

static void debug_print_intervals(const KivaGrid& /*G*/,
                                  const SafeIntervalIndex& sii,
                                  const std::vector<int>& vertices,
                                  int max_show_per_vertex = 6)
{
    if (!dss_debug_print) return;
    std::cout << "[DSS/SII] horizon=" << sii.horizon()
              << " vertices=" << vertices.size() << "\n";
    for (int v : vertices) {
        std::cout << "  v=" << v
                  << " intervals=" << sii.at(v).size() << "  ";
        int show = 0;
        for (const auto& I : sii.at(v)) {
            if (show++ >= max_show_per_vertex) { std::cout << "..."; break; }
            std::cout << "[" << I.t0 << "," << I.t1 << "] ";
        }
        std::cout << "\n";
    }
}

// ======== GLOBAL (PER-TICK) SAFE INTERVAL INDEX (VERTEX), LAZY REBUILD ========
static SafeIntervalIndex g_sii_tick;
static int g_sii_horizonT = 0;
static int g_sii_built_at = -1000000; // a time in the past

// Merge [a,b] into vector of intervals (occupied), assuming arbitrary order
static inline void push_occ(std::vector<SIInterval>& occ, int a, int b)
{
    if (b < a) return;
    occ.push_back({a,b});
}

// Merge and normalize intervals: sort by t0, merge overlaps/adjacent
static inline void normalize_intervals(std::vector<SIInterval>& v)
{
    if (v.empty()) return;
    std::sort(v.begin(), v.end(), [](const SIInterval& A, const SIInterval& B){
        if (A.t0 != B.t0) return A.t0 < B.t0;
        return A.t1 < B.t1;
    });
    int w = 0;
    for (int i = 0; i < (int)v.size(); ++i) {
        if (w == 0) { v[w++] = v[i]; continue; }
        if (v[i].t0 <= v[w-1].t1 + 1) {
            v[w-1].t1 = std::max(v[w-1].t1, v[i].t1);
        } else {
            v[w++] = v[i];
        }
    }
    v.resize(w);
}

// Invert [0..T] \ union(occ) into free intervals
static inline std::vector<SIInterval> invert_to_free(const std::vector<SIInterval>& occ, int T)
{
    std::vector<SIInterval> freev;
    if (T < 0) return freev;
    if (occ.empty()) { freev.push_back({0,T}); return freev; }

    int cur = 0;
    for (const auto& I : occ) {
        if (cur <= I.t0 - 1) freev.push_back({cur, I.t0 - 1});
        cur = I.t1 + 1;
        if (cur > T) break;
    }
    if (cur <= T) freev.push_back({cur, T});
    return freev;
}

// Build global vertex SII (compressed) up to horizon T
static void build_global_vertex_sii_compressed(const KivaGrid& G,
                                               const std::vector<Path>& paths,
                                               int T,
                                               SafeIntervalIndex& out_sii)
{
    const int V = std::max(0, G.get_rows() * G.get_cols());
    T = std::max(0, T);
    out_sii.init(V, T);
    if (V == 0) return;

    std::unordered_map<int, std::vector<SIInterval>> occ; // v -> occupied intervals
    occ.reserve(V / 4 + 32);

    for (int a = 0; a < (int)paths.size(); ++a) {
        if (paths[a].empty()) continue;

        std::vector<State> p = paths[a];
        p[0].location = clamp_vertex(G, p[0].location);
        for (size_t i = 1; i < p.size(); ++i) {
            p[i].location = clamp_vertex(G, p[i].location);
            if (p[i].timestep <= p[i-1].timestep) p[i].timestep = p[i-1].timestep + 1;
        }

        for (size_t i = 0; i + 1 < p.size(); ++i) {
            int vi = p[i].location;
            int ti = std::max(0, p[i].timestep);
            int vj = p[i+1].location;
            int tj = std::max(0, p[i+1].timestep);

            if (ti <= T && tj-1 >= 0) {
                int a0 = std::max(0, ti);
                int b0 = std::min(T, tj - 1);
                if (a0 <= b0) push_occ(occ[vi], a0, b0);
            }

            if (tj <= T) push_occ(occ[vj], tj, tj);
        }

        int vlast = p.back().location;
        int tlast = std::max(0, p.back().timestep);
        if (tlast <= T) push_occ(occ[vlast], tlast, T);
    }

    const int Vtot = G.get_rows() * G.get_cols();
    for (int v = 0; v < Vtot; ++v) {
        auto it = occ.find(v);
        if (it == occ.end()) {
            std::vector<SIInterval> allfree = { {0, T} };
            out_sii.set_intervals_for_vertex(v, std::move(allfree));
        } else {
            normalize_intervals(it->second);
            auto freev = invert_to_free(it->second, T);
            out_sii.set_intervals_for_vertex(v, std::move(freev));
        }
    }
}

static inline bool sii_has_window_at(const SafeIntervalIndex& sii, int v, int q0, int q1)
{
    const auto& ivs = sii.at(v);
    if (ivs.empty() || q0 > q1) return false;
    for (const auto& I : ivs) {
        const int a0 = std::max(I.t0, q0);
        const int a1 = std::min(I.t1, q1);
        if (a0 <= a1) return true;
    }
    return false;
}

static inline int dss_interference_score(const KivaGrid& G,
                                         const SafeIntervalIndex& sii,
                                         int v,
                                         int start_v,
                                         int now_t,
                                         int horizon)
{
    const auto& ivs = sii.at(v);
    int best = INT_MAX;

    for (const auto& I : ivs) {
        if (I.t1 < now_t) continue;
        if (I.t0 > now_t + horizon) break;
        int wait = std::max(0, I.t0 - now_t);
        int len  = std::max(1, I.t1 - std::max(now_t, I.t0) + 1);
        int dist = G.get_Manhattan_distance(start_v, v);
        int score = wait * 8 + (256 / len) + dist;
        if (score < best) best = score;
    }

    if (best == INT_MAX) {
        int dist = G.get_Manhattan_distance(start_v, v);
        best = 100000 + dist;
    }
    return best;
}

// ============================================================================
// Edge-aware SII (directed edges) — compressed, lazy rebuilt
// ============================================================================
struct EdgeKey {
    int u, v;
    bool operator==(const EdgeKey& o) const noexcept { return u==o.u && v==o.v; }
};
struct EdgeKeyHash {
    size_t operator()(const EdgeKey& k) const noexcept {
        return (size_t)k.u * 1315423911u ^ (size_t)k.v * 2654435761u;
    }
};

static std::unordered_map<EdgeKey, std::vector<SIInterval>, EdgeKeyHash> g_edge_free_sii;
static int g_edge_sii_horizonT = 0;
static int g_edge_sii_built_at = -1000000;

static void build_global_edge_sii_compressed(const KivaGrid& G,
                                             const std::vector<Path>& paths,
                                             int T)
{
    g_edge_free_sii.clear();
    T = std::max(0, T);

    std::unordered_map<EdgeKey, std::vector<SIInterval>, EdgeKeyHash> occ;

    for (int a = 0; a < (int)paths.size(); ++a) {
        if (paths[a].empty()) continue;

        std::vector<State> p = paths[a];
        p[0].location = clamp_vertex(G, p[0].location);
        for (size_t i = 1; i < p.size(); ++i) {
            p[i].location = clamp_vertex(G, p[i].location);
            if (p[i].timestep <= p[i-1].timestep) p[i].timestep = p[i-1].timestep + 1;
        }

        for (size_t i = 0; i + 1 < p.size(); ++i) {
            int u  = p[i].location;
            int v  = p[i+1].location;
            int tj = std::max(0, p[i+1].timestep);
            if (u == v) continue;
            if (tj <= T) {
                EdgeKey ek{u,v};
                push_occ(occ[ek], tj, tj);
            }
        }
    }

    for (auto &kv : occ) {
        auto &row = kv.second;
        normalize_intervals(row);
        g_edge_free_sii[kv.first] = invert_to_free(row, T);
    }
}

static inline bool edge_has_window_at(int u, int v, int q0, int q1)
{
    if (q0 > q1) return false;
    EdgeKey ek{u,v};
    auto it = g_edge_free_sii.find(ek);
    if (it == g_edge_free_sii.end()) return true; // unseen → free
    const auto& ivs = it->second;
    for (const auto& I : ivs) {
        const int a0 = std::max(I.t0, q0);
        const int a1 = std::min(I.t1, q1);
        if (a0 <= a1) return true;
    }
    return false;
}

static inline int min_outgoing_edge_wait(const KivaGrid& G, int start_v, int t0, int horizon)
{
    int best = INT_MAX;
    for (int nb : G.get_neighbors(start_v)) {
        nb = clamp_vertex(G, nb);
        for (int w = 0; w <= horizon; ++w) {
            const int step_t = t0 + 1 + w;
            if (edge_has_window_at(start_v, nb, step_t, step_t)) {
                best = std::min(best, w);
                break;
            }
        }
    }
    return best;
}

static inline int min_incoming_edge_wait(const KivaGrid& G, int goal_v, int eta, int delta)
{
    int best = INT_MAX;
    for (int nb : G.get_neighbors(goal_v)) {
        nb = clamp_vertex(G, nb);
        for (int w = 0; w <= delta; ++w) {
            const int t = eta + w;
            if (edge_has_window_at(nb, goal_v, t, t)) {
                best = std::min(best, w);
                break;
            }
        }
    }
    return best;
}

// ============================================================================
// ==== CHOKEPOINTS (LITE) ====
// Identify corridor-like degree-2 vertices once; per tick, score their heat.
// ============================================================================
std::vector<char> g_is_choke;            // size = V, 1 if chokepoint (exposed for tokens)
static std::unordered_map<int,int> g_choke_heat; // vertex -> traversal count
static int g_choke_built_at = -1000000;
static int g_choke_horizonT = 0;

static inline void build_chokepoints_once(const KivaGrid& G)
{
    const int V = grid_vertex_count(G);
    g_is_choke.assign(V, 0);
    for (int v = 0; v < V; ++v) {
        const auto& nbs = G.get_neighbors(v);
        if ((int)nbs.size() == 2) { // corridor interior
            g_is_choke[v] = 1;
        }
    }
}

// Cheap heatmap: count how many agents traverse a choke within [t..T].
static inline void build_choke_heat(const KivaGrid& G,
                                    const std::vector<Path>& paths,
                                    int now_t,
                                    int horizonT)
{
    g_choke_heat.clear();
    for (int a = 0; a < (int)paths.size(); ++a) {
        if (paths[a].empty()) continue;
        const auto& p = paths[a];
        for (size_t i = 0; i < p.size(); ++i) {
            int t = p[i].timestep;
            if (t < now_t || t > horizonT) continue;
            int v = clamp_vertex(G, p[i].location);
            if (v >= 0 && v < (int)g_is_choke.size() && g_is_choke[v]) {
                g_choke_heat[v] += 1;
            }
        }
    }
}

// Sum penalty along a greedy route start->goal (clamped to time window & few chokes)
static inline int greedy_choke_penalty(const KivaGrid& G,
                                       int start_v, int start_t,
                                       int goal_v, int horizonT)
{
    auto seq = safe_step_path(G, start_v, start_t, goal_v);
    int seen = 0;
    int penalty = 0;
    for (size_t i = 0; i < seq.size(); ++i) {
        int v = seq[i].first;
        int t = seq[i].second;
        if (t > horizonT) break;
        if (v >= 0 && v < (int)g_is_choke.size() && g_is_choke[v]) {
            auto it = g_choke_heat.find(v);
            if (it != g_choke_heat.end() && it->second > 0) {
                penalty += CHOKE_PENALTY_W * it->second;
                if (++seen >= CHOKE_LOOKAHEAD_CAP) break;
            }
        }
    }
    return penalty;
}

// Is the first step from start going into a hot choke?
static inline bool starts_into_hot_choke(const KivaGrid& G,
                                         int start_v, int start_t, int goal_v)
{
    auto seq = safe_step_path(G, start_v, start_t, goal_v);
    if (seq.size() < 2) return false;
    int next_v = seq[1].first;
    if (next_v >= 0 && next_v < (int)g_is_choke.size() && g_is_choke[next_v]) {
        auto it = g_choke_heat.find(next_v);
        if (it != g_choke_heat.end() && it->second >= HOT_CHOKE_THRESH)
            return true;
    }
    return false;
}

// ============================================================================
// KivaSystem core
// ============================================================================
KivaSystem::KivaSystem(const KivaGrid& G_, MAPFSolver& solver)
  : BasicSystem(G_, solver), G(G_) {}

KivaSystem::~KivaSystem() {}

// ------------------------------- initialize ---------------------------------
void KivaSystem::initialize()
{
    initialize_solvers();

    starts.resize(num_of_drives);
    goal_locations.resize(num_of_drives);
    paths.resize(num_of_drives);
    finished_tasks.resize(num_of_drives);

    g_replan_cooldown.assign(num_of_drives, 0);

    bundle.assign(num_of_drives, {});
    rest.assign(num_of_drives, {});
    bundle_dirty.assign(num_of_drives, true); // force stitch at t=0

    bool succ = load_records();
    if (!succ)
    {
        timestep = 0;
        succ = load_locations();
        if (!succ)
        {
            cout << "Randomly generating initial locations" << endl;
            initialize_start_locations();
            initialize_goal_locations();
        }
    }

    // build static chokepoints once
    build_chokepoints_once(G);

    bundle_configure(num_of_drives, default_agent_capacity, randomize_sequences, rng_seed);
    if (!given_goals.empty())
        bundle_initialize_from_given(given_goals);
}

void KivaSystem::initialize_start_locations()
{
    for (int k = 0; k < num_of_drives; k++)
    {
        int orientation = consider_rotation ? (rand() % 4) : -1;
        int home = (!G.agent_home_locations.empty() && k < (int)G.agent_home_locations.size())
                 ? clamp_vertex(G, G.agent_home_locations[k]) : 0;

        starts[k] = State(home, 0, orientation);
        paths[k].emplace_back(starts[k]);
        finished_tasks[k].emplace_back(home, 0);
    }
}

void KivaSystem::initialize_goal_locations()
{
    if (hold_endpoints || useDummyPaths) return;

    for (int k = 0; k < num_of_drives; k++)
    {
        int curr = safe_path_at(paths, G, k, 0, consider_rotation).location;
        int goal = pick_random_endpoint_except(G, curr);
        goal_locations[k].emplace_back(goal, 0);
    }
}

// -------------------------------- helpers -----------------------------------
void KivaSystem::ensure_goal_exists(int k, int curr)
{
    if (k < 0 || k >= (int)goal_locations.size()) return;
    if (!goal_locations[k].empty()) return;
    int g = pick_random_endpoint_except(G, curr);
    goal_locations[k].emplace_back(g, 0);
}

int KivaSystem::cap_of(int k) const
{
    if (!per_agent_capacity.empty() && k >= 0 && k < (int)per_agent_capacity.size()) {
        int c = per_agent_capacity[k];
        return c > 0 ? c : 1;
    }
    return default_agent_capacity > 0 ? default_agent_capacity : 1;
}

void KivaSystem::bundle_configure(int, int capacity, bool randomize, unsigned seed)
{
    default_agent_capacity = (capacity > 0 ? capacity : 1);
    randomize_sequences = randomize;
    rng_seed            = seed;
}

void KivaSystem::bundle_initialize_from_given(const std::vector<std::vector<int>>& gg)
{
    std::mt19937 rng(rng_seed);
    int n = std::min<int>((int)gg.size(), num_of_drives);
    for (int k = 0; k < n; ++k)
    {
        std::vector<int> seq = gg[k];
        if (randomize_sequences) std::shuffle(seq.begin(), seq.end(), rng);

        const int cap = cap_of(k);
        for (size_t i = 0; i < seq.size(); ++i) {
            int v = clamp_vertex(G, seq[i]);
            std::pair<int,int> g = { v, 0 };
            if ((int)bundle[k].size() < cap) bundle[k].push_back(g);
            else                              rest[k].push_back(g);
        }
        bundle_dirty[k] = true;
    }
}

bool KivaSystem::bundle_on_goal_reached(int k)
{
    if (k < 0 || k >= (int)bundle.size()) return false;
    if (bundle[k].empty()) return false;

    const int curr = safe_path_at(paths, G, k, timestep, consider_rotation).location;
    const auto& front = bundle[k].front();
    if (curr == clamp_vertex(G, front.first) && timestep >= front.second) {
        bundle[k].pop_front();
        bundle_dirty[k] = true;
        m_restitches_total++;
        return true;
    }
    return false;
}

std::unordered_set<int> KivaSystem::collect_claimed_active_endpoints(int except_agent) const
{
    std::unordered_set<int> claimed;
    if (!capacity_mode) return claimed;
    for (int i = 0; i < (int)bundle.size(); ++i) {
        if (i == except_agent) continue;
        for (const auto& g : bundle[i]) claimed.insert(clamp_vertex(G, g.first));
    }
    return claimed;
}

bool KivaSystem::bundle_maybe_top_up(int k)
{
    if (k < 0 || k >= (int)bundle.size()) return false;
    bool changed = false;

    std::unordered_set<int> claimed = avoid_dup_goals ? collect_claimed_active_endpoints(k)
                                                      : std::unordered_set<int>{};

    const int cap = cap_of(k);
    int guard = 0;
    while ((int)bundle[k].size() < cap && !rest[k].empty() && guard++ < 2000) {
        auto g = rest[k].front(); rest[k].pop_front();
        g.first = clamp_vertex(G, g.first);

        if (avoid_dup_goals && claimed.count(g.first)) {
            rest[k].push_back(g);
            bool all_claimed = true;
            for (const auto& r : rest[k]) { if (!claimed.count(clamp_vertex(G, r.first))) { all_claimed = false; break; } }
            if (all_claimed) break;
            continue;
        }

        bundle[k].push_back(g);
        changed = true;
    }
    if (changed) { bundle_dirty[k] = true; m_restitches_total++; }
    return changed;
}

void KivaSystem::bundle_assert_capacity_ok(int k) const
{
    if (k < 0 || k >= (int)bundle.size()) return;
    if ((int)bundle[k].size() > cap_of(k)) {
        std::cerr << "[BUG] bundle size exceeds capacity for agent " << k << std::endl;
        std::abort();
    }
}

void KivaSystem::bundle_mirror_to_engine()
{
    for (int k = 0; k < num_of_drives; ++k) {
        goal_locations[k].clear();
        for (const auto& g : bundle[k]) {
            int v = clamp_vertex(G, g.first);
            goal_locations[k].push_back({v, g.second});
        }
        if (goal_locations[k].empty()) {
            int curr = safe_path_at(paths, G, k, timestep, consider_rotation).location;
            ensure_goal_exists(k, curr);
        }
        bundle_assert_capacity_ok(k);
    }
}

// --------------------------- DVS reorder (light) -----------------------------
void KivaSystem::reorder_bundle_by_dvs(int k)
{
    if (!safety_mode) return;
    if (k < 0 || k >= (int)bundle.size()) return;
    if (bundle[k].size() <= 1) return;

    const int start_v =
        safe_path_at(paths, G, k, timestep, consider_rotation).location;

    std::vector<std::pair<int,int>> items;
    items.reserve(bundle[k].size());
    for (auto &g : bundle[k])
        items.emplace_back(clamp_vertex(G, g.first), g.second);

    std::vector<std::tuple<int,int,int>> scored;  // (dist, jitter, idx)
    scored.reserve(items.size());
    for (int i = 0; i < (int)items.size(); ++i) {
        int d = G.get_Manhattan_distance(start_v, items[i].first);
        int jitter = rand() & 0xF;
        scored.emplace_back(d, jitter, i);
    }

    std::sort(scored.begin(), scored.end());

    std::deque<std::pair<int,int>> reordered;
    for (auto &s : scored)
        reordered.push_back(items[std::get<2>(s)]);

    bundle[k] = std::move(reordered);
    bundle_dirty[k] = true;
    m_restitches_total++;
}

int KivaSystem::generate_endpoint_for(int, int avoid_v) const
{
    return pick_random_endpoint_except(G, avoid_v);
}

void KivaSystem::maybe_autorefill_rest(int k)
{
    if (!auto_refill) return;
    if (k < 0 || k >= (int)rest.size()) return;

    if (rest[k].empty()) {
        int curr = safe_path_at(paths, G, k, timestep, consider_rotation).location;

        std::unordered_set<int> claimed = collect_claimed_active_endpoints(k);
        claimed.insert(clamp_vertex(G, curr));

        const int cap = cap_of(k);
        for (int i = 0; i < cap; ++i) {
            int v = generate_endpoint_for(k, curr);
            int tries = 16;
            while (avoid_dup_goals && claimed.count(v) && tries-- > 0) {
                v = generate_endpoint_for(k, curr);
            }
            v = clamp_vertex(G, v);
            rest[k].push_back({v, 0});
            claimed.insert(v);
        }
    }
}

// ------------------------------- Stitching ----------------------------------
void KivaSystem::suppress_replan_for(int k)
{
    for (auto it = new_agents.begin(); it != new_agents.end(); ) {
        if (*it == k) it = new_agents.erase(it);
        else ++it;
    }
}

void KivaSystem::plan_stitched_for_agent(int k)
{
    if (k < 0 || k >= num_of_drives) return;
    if (!capacity_mode) return;
    if (bundle[k].empty()) return;

    // cooldown
    if (!bundle_dirty[k] && g_replan_cooldown.size() == (size_t)num_of_drives && g_replan_cooldown[k] > 0) {
        metrics_after_stitch(false,false,false,true);
        return;
    }

    if (restitch_on_change && !bundle_dirty[k] && timestep > 0) {
        metrics_after_stitch(false, false, false, true);
        return;
    }

    const int start_v = safe_path_at(paths, G, k, timestep, consider_rotation).location;
    const int start_t = timestep;

    const int PLWIN = effective_planning_window(planning_window, num_of_drives);

    std::vector<int> goals;
    goals.reserve(bundle[k].size());
    for (const auto& g : bundle[k]) goals.push_back(clamp_vertex(G, g.first));
    clean_goals(G, start_v, goals);
    if (goals.empty()) {
        bundle_dirty[k] = false;
        metrics_after_stitch(false, false, false, true);
        return;
    }

    const int depth_cap = LIGHTWEIGHT_MODE ? std::min(stitch_depth, 2) : stitch_depth;
    if ((int)goals.size() > depth_cap) goals.resize(depth_cap);

    // ===== DSS: rank goals by vertex interference + incoming edge hazard =====
    // + Chokepoint-lite penalty along greedy path
    std::vector<std::pair<int,int>> ranked; // (score, v)
    ranked.reserve(goals.size());
    for (int g : goals) {
        int v = clamp_vertex(G, g);
        int dist = G.get_Manhattan_distance(start_v, v);
        int eta  = start_t + dist;

        int sc_v = dss_interference_score(G, g_sii_tick, v, start_v, start_t, PLWIN);
        int delta = std::max(2, PLWIN / 3);
        int wait_in = min_incoming_edge_wait(G, v, eta, delta);
        if (wait_in == INT_MAX) wait_in = 64;

        int choke_pen = greedy_choke_penalty(G, start_v, start_t, v, start_t + PLWIN);

        int sc = sc_v + wait_in * 6 + choke_pen;
        ranked.emplace_back(sc, v);
    }
    std::sort(ranked.begin(), ranked.end());
    goals.clear();
    for (auto &p : ranked) goals.push_back(p.second);

    // ===== DSS: deferral — if front has no vertex window in [t, t+PLWIN], move to rest =====
    {
        const int q0 = start_t, q1 = start_t + PLWIN;
        if (!goals.empty()) {
            int g0 = goals.front();
            if (!sii_has_window_at(g_sii_tick, g0, q0, q1)) {
                if (k >= 0 && k < (int)rest.size()) rest[k].push_back({g0, 0});
                goals.erase(goals.begin());
                bundle_dirty[k] = true;
                m_restitches_total++;
                bundle_maybe_top_up(k);
            }
        }
        if (goals.empty()) {
            metrics_after_stitch(false, false, false, true);
            return;
        }
    }

    // ===== Chokepoint-lite gating: avoid goals that begin with a HOT choke =====
    {
        std::vector<int> kept;
        kept.reserve(goals.size());
        for (int v : goals) {
            if (starts_into_hot_choke(G, start_v, start_t, v)) continue; // skip for now
            kept.push_back(v);
        }
        if (!kept.empty()) goals.swap(kept);
    }

    // ===== DSS: gate chain by incoming edge feasibility near ETA =====
    {
        std::vector<int> kept;
        kept.reserve(goals.size());
        for (int v : goals) {
            int dist = G.get_Manhattan_distance(start_v, v);
            int eta  = start_t + dist;
            int win  = std::max(2, PLWIN / 3);
            int w_in = min_incoming_edge_wait(G, v, eta, win);
            if (w_in != INT_MAX) kept.push_back(v);
        }
        if (kept.empty()) {
            bundle_dirty[k] = false;
            metrics_after_stitch(false, false, false, true);
            if (g_replan_cooldown.size() == (size_t)num_of_drives)
                g_replan_cooldown[k] = REPLAN_COOLDOWN_TICKS;
            return;
        }
        goals.swap(kept);
    }

    std::vector<State> new_suffix;
    bool used_sipp = false, sipp_ok = false, fell_back = false;

    if (stitch_use_sipp) {
        used_sipp = true;

        // Edge-aware start snapping (vertex snap + outgoing edge check)
        int best_w_vertex = 0;
        {
            // tiny local SII near start & first goal
            SafeIntervalIndex sii_local;

            auto build_sii_sparse_local = [&](int horizon_t,
                                              int around_v0,
                                              int around_g,
                                              SafeIntervalIndex& out_sii)
            {
                const int V = G.get_rows() * G.get_cols();
                const int T = std::max(0, horizon_t);
                out_sii.init(V, T);

                auto near_any = [&](int v)->bool{
                    if (G.get_Manhattan_distance(v, around_v0) <= 8) return true;
                    if (G.get_Manhattan_distance(v, around_g ) <= 8) return true;
                    return false;
                };

                std::unordered_map<int, std::vector<SIInterval>> occ;
                for (int a = 0; a < (int)paths.size(); ++a) {
                    if (paths[a].empty() || a == k) continue;
                    std::vector<State> p = paths[a];
                    p[0].location = clamp_vertex(G, p[0].location);
                    for (size_t i = 1; i < p.size(); ++i) {
                        p[i].location = clamp_vertex(G, p[i].location);
                        if (p[i].timestep <= p[i-1].timestep) p[i].timestep = p[i-1].timestep + 1;
                    }

                    for (size_t i = 0; i + 1 < p.size(); ++i) {
                        int vi = p[i].location, ti = std::max(0, p[i].timestep);
                        int vj = p[i+1].location, tj = std::max(0, p[i+1].timestep);
                        if (near_any(vi)) { int a0=std::max(0,ti), b0=std::min(T,tj-1); if (a0<=b0) push_occ(occ[vi], a0,b0); }
                        if (near_any(vj) && tj <= T) push_occ(occ[vj], tj, tj);
                    }
                    int vlast=p.back().location, tlast=std::max(0,p.back().timestep);
                    if (near_any(vlast) && tlast<=T) push_occ(occ[vlast], tlast, T);
                }

                for (auto &kv : occ) {
                    normalize_intervals(kv.second);
                    auto freev = invert_to_free(kv.second, T);
                    out_sii.set_intervals_for_vertex(kv.first, std::move(freev));
                }
            };

            int first_goal = goals.front();
            const int localH = start_t + PLWIN;
            build_sii_sparse_local(localH, clamp_vertex(G, start_v), clamp_vertex(G, first_goal), sii_local);

            const auto& ivs = sii_local.at(clamp_vertex(G, start_v));
            bool snapped = false;
            for (const auto& I : ivs) {
                if (I.t1 < start_t) continue;
                best_w_vertex = std::max(0, I.t0 - start_t);
                snapped = true;
                break;
            }
            if (!snapped) {
                bundle_dirty[k] = false;
                metrics_after_stitch(false, false, false, true);
                if (g_replan_cooldown.size() == (size_t)num_of_drives)
                    g_replan_cooldown[k] = REPLAN_COOLDOWN_TICKS;
                return;
            }
        }

        int extra_w_edge = min_outgoing_edge_wait(G, clamp_vertex(G, start_v),
                                                  start_t + best_w_vertex, PLWIN);
        if (extra_w_edge == INT_MAX) {
            bundle_dirty[k] = false;
            metrics_after_stitch(false, false, false, true);
            if (g_replan_cooldown.size() == (size_t)num_of_drives)
                g_replan_cooldown[k] = REPLAN_COOLDOWN_TICKS;
            return;
        }
        int best_w = best_w_vertex + extra_w_edge;

        // Build RT once, try with snapped edge-aware offset (includes corridor tokens)
        ReservationTable rt(G);
        build_rt_from_teammates_safe(G, paths, k,
                                     start_t + PLWIN,
                                     /*crop=*/stitch_crop_horizon,
                                     consider_rotation,
                                     rt);

        State s0(clamp_vertex(G, start_v), start_t + best_w, -1);
        std::vector<std::pair<int,int>> gl; gl.reserve(goals.size());
        for (int g : goals) gl.emplace_back(g, 0);

        SIPP sipp;
        auto path = sipp.run(G, s0, gl, rt);
        if (!path.empty()) {
            if (best_w > 0) {
                new_suffix.reserve(best_w + path.size());
                for (int i = 0; i < best_w; ++i) new_suffix.emplace_back(start_v, start_t + i, -1);
                new_suffix.insert(new_suffix.end(), path.begin(), path.end());
                for (size_t i = 1; i < new_suffix.size(); ++i)
                    if (new_suffix[i].timestep <= new_suffix[i-1].timestep)
                        new_suffix[i].timestep = new_suffix[i-1].timestep + 1;
            } else {
                new_suffix = std::move(path);
            }
            sipp_ok = true;
        }
    }

    if (!sipp_ok) {
        // deterministic, never fails
        std::vector<std::pair<int,int>> seq;
        seq.emplace_back(start_v, start_t);
        int v = start_v, t = start_t;
        for (int g : goals) {
            auto seg = safe_step_path(G, v, t, g);
            if (seg.size() > 1) {
                seq.insert(seq.end(), seg.begin() + 1, seg.end());
                v = g; t = seq.back().second;
            }
        }
        new_suffix.clear();
        new_suffix.reserve(seq.size());
        for (auto &p : seq) new_suffix.emplace_back(p.first, p.second, -1);
        fell_back = true;
    }

    if (new_suffix.empty()) {
        bundle_dirty[k] = false;
        metrics_after_stitch(used_sipp, sipp_ok, fell_back, false);
        return;
    }

    // prefix + suffix
    std::vector<State> new_path;
    new_path.reserve(paths[k].size() + new_suffix.size());
    for (int i = 0; i < (int)paths[k].size() && paths[k][i].timestep < timestep; ++i)
        new_path.push_back(paths[k][i]);
    new_path.insert(new_path.end(), new_suffix.begin(), new_suffix.end());
    paths[k] = std::move(new_path);

    // Add corridor tokens for this committed plan (only chokepoint edges)
    tokens_add_from_path(G, new_suffix, k);

    suppress_replan_for(k);
    bundle_dirty[k] = false;
    metrics_after_stitch(used_sipp, sipp_ok, fell_back, false);

    if (g_replan_cooldown.size() == (size_t)num_of_drives)
        g_replan_cooldown[k] = REPLAN_COOLDOWN_TICKS;
}

std::vector<int> KivaSystem::compute_batch_order() const
{
    std::vector<int> pool;
    if (stitch_target == -1) {
        pool.reserve(num_of_drives);
        for (int k = 0; k < num_of_drives; ++k) pool.push_back(k);
    } else if (stitch_target >= 0 && stitch_target < num_of_drives) {
        pool.push_back(stitch_target);
    } else {
        return pool;
    }

    pool.erase(std::remove_if(pool.begin(), pool.end(), [&](int k){
        if (!capacity_mode) return true;
        if (k < 0 || k >= (int)bundle.size()) return true;
        if (bundle[k].empty()) return true;
        if (restitch_on_change && !bundle_dirty[k] && timestep > 0) return true;
        return false;
    }), pool.end());

    switch (stitch_batch_order) {
    case StitchOrder::ByIndex:
        break;
    case StitchOrder::ShortestRemaining: {
        std::sort(pool.begin(), pool.end(), [&](int a, int b){
            int sa = safe_path_at(const_cast<std::vector<std::vector<State>>&>(paths), G, a, timestep, consider_rotation).location;
            int sb = safe_path_at(const_cast<std::vector<std::vector<State>>&>(paths), G, b, timestep, consider_rotation).location;
            auto cost = [&](int k, int s){
                int c = 0, cur = s;
                if (k >= 0 && k < (int)bundle.size())
                    for (auto &g : bundle[k]) { c += G.get_Manhattan_distance(cur, clamp_vertex(G, g.first)); cur = clamp_vertex(G, g.first); }
                return c;
            };
            return cost(a, sa) < cost(b, sb);
        });
        break;
    }
    case StitchOrder::ClosestNextGoal: {
        std::sort(pool.begin(), pool.end(), [&](int a, int b){
            int sa = safe_path_at(const_cast<std::vector<std::vector<State>>&>(paths), G, a, timestep, consider_rotation).location;
            int sb = safe_path_at(const_cast<std::vector<std::vector<State>>&>(paths), G, b, timestep, consider_rotation).location;
            int ca = (a >= 0 && a < (int)bundle.size() && !bundle[a].empty())
                   ? G.get_Manhattan_distance(sa, clamp_vertex(G, bundle[a].front().first))
                   : INT_MAX;
            int cb = (b >= 0 && b < (int)bundle.size() && !bundle[b].empty())
                   ? G.get_Manhattan_distance(sb, clamp_vertex(G, bundle[b].front().first))
                   : INT_MAX;
            return ca < cb;
        });
        break;
    }
    }
    return pool;
}

void KivaSystem::plan_stitched_batch()
{
    auto to_go = compute_batch_order();

    // decay cooldown counters once per tick
    if (g_replan_cooldown.size() == (size_t)num_of_drives) {
        for (int i = 0; i < num_of_drives; ++i) if (g_replan_cooldown[i] > 0) --g_replan_cooldown[i];
    }

    // ===== Lazy Rebuild GLOBAL SII (vertex + edge) for this tick =====
    const bool need_rebuild = (timestep - g_sii_built_at) >= SII_REBUILD_PERIOD;
    const bool need_edge_rebuild = (timestep - g_edge_sii_built_at) >= SII_REBUILD_PERIOD;
    const int PLWIN = effective_planning_window(planning_window, num_of_drives);
    const int horizonT = timestep + PLWIN;

    if (need_rebuild) {
        g_sii_horizonT = horizonT;
        build_global_vertex_sii_compressed(G, paths, g_sii_horizonT, g_sii_tick);
        g_sii_built_at = timestep;
    }
    if (need_edge_rebuild) {
        g_edge_sii_horizonT = horizonT;
        build_global_edge_sii_compressed(G, paths, g_edge_sii_horizonT);
        g_edge_sii_built_at = timestep;
    }

    // ===== Chokepoint heat (lite) each tick =====
    if ((timestep - g_choke_built_at) >= 1) {
        g_choke_horizonT = horizonT;
        build_choke_heat(G, paths, timestep, g_choke_horizonT);
        g_choke_built_at = timestep;
    }

    // ===== Corridor tokens GC =====
    tokens_gc(timestep, PLWIN);

    // planning budget per tick to avoid spikes
    const int base_budget = std::max(4, num_of_drives / 5); // ~20%
    const int BUDGET = LIGHTWEIGHT_MODE ? std::max(3, base_budget / 2) : base_budget;
    if ((int)to_go.size() > BUDGET) to_go.resize(BUDGET);

    for (int k : to_go) {
        plan_stitched_for_agent(k);
    }
}

// ------------------------------- Metrics ------------------------------------
void KivaSystem::metrics_begin_tick()
{
    m_tick_stitched_agents = 0;
    m_tick_sipp_success    = 0;
    m_tick_sipp_fallback   = 0;
    m_tick_skipped_clean   = 0;
}

void KivaSystem::metrics_after_stitch(bool used_sipp, bool sipp_ok, bool fell_back, bool skipped_clean)
{
    if (skipped_clean) { m_tick_skipped_clean++; m_skipped_clean_total++; return; }

    m_stitch_attempts_total++;
    m_tick_stitched_agents++;
    m_stitch_agents_ticks++;

    if (used_sipp) {
        if (sipp_ok) { m_tick_sipp_success++; m_sipp_success_total++; }
        else         { m_sipp_fail_total++; }
    }
    if (fell_back)  { m_tick_sipp_fallback++; m_sipp_fallback_total++; }
}

void KivaSystem::metrics_end_tick_and_maybe_log()
{
    if (metrics_verbose) {
        cout << "[t=" << timestep << "] stitched_agents=" << m_tick_stitched_agents
             << " sipp_ok=" << m_tick_sipp_success
             << " fallbacks=" << m_tick_sipp_fallback
             << " skipped_clean=" << m_tick_skipped_clean
             << endl;
    }
    if (metrics_csv_enabled && !metrics_csv_path.empty()) {
        std::ofstream f(metrics_csv_path, std::ios::app);
        if (f.tellp() == 0) {
            f << "t,stitched_agents,sipp_ok,fallbacks,skipped_clean\n";
        }
        f << timestep << ","
          << m_tick_stitched_agents << ","
          << m_tick_sipp_success << ","
          << m_tick_sipp_fallback << ","
          << m_tick_skipped_clean << "\n";
    }
}

void KivaSystem::metrics_print_summary() const
{
    cout << "\n=== Stitching/Capacity Summary ===\n";
    cout << "stitch_attempts_total:   " << m_stitch_attempts_total   << "\n";
    cout << "stitched_agents_ticks:   " << m_stitch_agents_ticks     << "\n";
    cout << "sipp_success_total:      " << m_sipp_success_total      << "\n";
    cout << "sipp_fallback_total:     " << m_sipp_fallback_total     << "\n";
    cout << "sipp_fail_total:         " << m_sipp_fail_total         << "\n";
    cout << "skipped_clean_total:     " << m_skipped_clean_total     << "\n";
    cout << "restitches_total:        " << m_restitches_total        << "\n";
    cout << "=================================\n";
}

// ------------------------------- Debug print --------------------------------
void KivaSystem::debug_print_capacity_state() const
{
    if (!capacity_mode) return;

    std::ostringstream oss;
    oss << "[t=" << timestep << "] CAPACITY STATE\n";
    for (int k = 0; k < num_of_drives; ++k) {
        oss << "  agent " << k
            << " cap=" << cap_of(k)
            << " bundle=[";
        if (k >= 0 && k < (int)bundle.size()) {
            for (size_t i = 0; i < bundle[k].size(); ++i) {
                oss << clamp_vertex(G, bundle[k][i].first);
                if (i + 1 < bundle[k].size()) oss << ",";
            }
        }
        oss << "] rest=("
            << ((k >= 0 && k < (int)rest.size()) ? rest[k].size() : 0)
            << ")";

        if (k >= 0 && k < (int)bundle_dirty.size())
            oss << " dirty=" << (bundle_dirty[k] ? "Y" : "N");

        oss << "\n";
    }

    oss << "  replans: [";
    bool first = true;
    for (int a : new_agents) {
        if (!first) oss << ",";
        first = false;
        oss << a;
    }
    oss << "]\n";

    std::cout << oss.str();
}

// -------------------------------- update ------------------------------------
void KivaSystem::update_goal_locations()
{
    if (!LRA_called)
        new_agents.clear();

    if (capacity_mode)
    {
        for (int k = 0; k < num_of_drives; ++k) {
            bool popped = bundle_on_goal_reached(k);
            size_t size_before = bundle[k].size();
            maybe_autorefill_rest(k);
            bool topped = bundle_maybe_top_up(k);
            bool changed = popped || topped;

            if (safety_mode) { reorder_bundle_by_dvs(k); changed = true; }

            if (changed) bundle_dirty[k] = true;

            if (timestep == 0 || changed || bundle[k].size() != size_before) {
                new_agents.emplace_back(k);
            }
        }
        bundle_mirror_to_engine();
        if (capacity_debug) debug_print_capacity_state();
        return;
    }

    // legacy update
    if (hold_endpoints)
    {
        unordered_map<int, int> held_locations;
        for (int k = 0; k < num_of_drives; k++)
        {
            int curr = safe_path_at(paths, G, k, timestep, consider_rotation).location;
            if (goal_locations[k].empty())
            {
                int next = pick_random_endpoint_except(G, curr);
                while (next == curr || held_endpoints.find(next) != held_endpoints.end())
                {
                    next = pick_random_endpoint_except(G, curr);
                }
                goal_locations[k].emplace_back(next, 0);
                held_endpoints.insert(next);
            }
            if (paths[k].back().location == clamp_vertex(G, goal_locations[k].back().first) &&
                paths[k].back().timestep >= goal_locations[k].back().second)
            {
                int agent = k;
                int loc = clamp_vertex(G, goal_locations[k].back().first);
                auto it = held_locations.find(loc);
                while (it != held_locations.end())
                {
                    int removed_agent = it->second;
                    new_agents.remove(removed_agent);
                    cout << "Agent " << removed_agent << " has to wait for agent " << agent
                         << " because of location " << loc << endl;
                    held_locations[loc] = agent;
                    agent = removed_agent;
                    loc = safe_path_at(paths, G, agent, timestep, consider_rotation).location;
                    it = held_locations.find(loc);
                }
                held_locations[loc] = agent;
            }
            else
            {
                if (held_locations.find(clamp_vertex(G, goal_locations[k].back().first)) == held_locations.end())
                {
                    held_locations[clamp_vertex(G, goal_locations[k].back().first)] = k;
                    new_agents.emplace_back(k);
                    continue;
                }
                int agent = k;
                int loc = curr;
                cout << "Agent " << agent
                     << " has to wait for agent "
                     << held_locations[clamp_vertex(G, goal_locations[k].back().first)]
                     << " because of location "
                     << clamp_vertex(G, goal_locations[k].back().first)
                     << endl;

                auto it = held_locations.find(loc);
                while (it != held_locations.end())
                {
                    int removed_agent = it->second;
                    new_agents.remove(removed_agent);
                    cout << "Agent " << removed_agent << " has to wait for agent "
                         << agent << " because of location " << loc << endl;
                    held_locations[loc] = agent;
                    agent = removed_agent;
                    loc = safe_path_at(paths, G, agent, timestep, consider_rotation).location;
                    it = held_locations.find(loc);
                }
                held_locations[loc] = agent;
            }
        }
    }
    else
    {
        for (int k = 0; k < num_of_drives; k++)
        {
            int curr = safe_path_at(paths, G, k, timestep, consider_rotation).location;
            if (useDummyPaths)
            {
                if (goal_locations[k].empty())
                {
                    int home = (!G.agent_home_locations.empty() && k < (int)G.agent_home_locations.size())
                             ? clamp_vertex(G, G.agent_home_locations[k]) : 0;
                    goal_locations[k].emplace_back(home, 0);
                }
                if (goal_locations[k].size() == 1)
                {
                    int next = pick_random_endpoint_except(G, curr);
                    goal_locations[k].emplace(goal_locations[k].begin(), next, 0);
                    new_agents.emplace_back(k);
                }
            }
            else
            {
                std::pair<int, int> goal = goal_locations[k].empty()
                    ? std::make_pair(curr, 0)
                    : std::make_pair(clamp_vertex(G, goal_locations[k].back().first), goal_locations[k].back().second);

                double min_timesteps = G.get_Manhattan_distance(goal.first, curr);
                while (min_timesteps <= simulation_window)
                {
                    std::pair<int, int> next;
                    if (is_endpoint_safe(G, goal.first))
                    {
                        next = std::make_pair(pick_random_endpoint_except(G, curr), 0);
                    }
                    else
                    {
                        std::cout << "WARN update_goal_locations(): non-endpoint goal " << goal.first
                                  << " – sampling an endpoint instead.\n";
                        next = std::make_pair(pick_random_endpoint_except(G, curr), 0);
                    }
                    goal_locations[k].emplace_back(next);
                    min_timesteps += G.get_Manhattan_distance(next.first, goal.first);
                    goal = next;
                }
            }
        }
    }
}

// ------------------------------- simulate -----------------------------------
void KivaSystem::simulate(int simulation_time)
{
    std::cout << "*** Simulating " << seed << " ***" << std::endl;
    this->simulation_time = simulation_time;
    initialize();

    for (; timestep < simulation_time; timestep += simulation_window)
    {
        std::cout << "Timestep " << timestep << std::endl;

        metrics_begin_tick();

        update_start_locations();
        update_goal_locations();

        if (capacity_mode && stitch_mode) {
            plan_stitched_batch();
        }

        // run MAPF
        solve();

        // apply moves
        auto new_finished_tasks = move();
        std::cout << new_finished_tasks.size() << " tasks has been finished" << std::endl;

        for (auto task : new_finished_tasks)
        {
            int id, loc, t;
            std::tie(id, loc, t) = task;
            finished_tasks[id].emplace_back(loc, t);
            num_of_tasks++;
            if (hold_endpoints)
                held_endpoints.erase(loc);
        }

        if (congested())
        {
            cout << "***** Too many traffic jams ***" << endl;
            break;
        }

        metrics_end_tick_and_maybe_log();
    }

    update_start_locations();
    std::cout << std::endl << "Done!" << std::endl;
    metrics_print_summary();
    save_results();
}
