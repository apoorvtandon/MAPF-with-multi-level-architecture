#include "KivaSystem.h"
#include "WHCAStar.h"
#include "ECBS.h"
#include "LRAStar.h"
#include "PBS.h"

#include "SIPP.h"
#include "ReservationTable.h"

// NOTE: I intentionally avoid DVS adapter headers to keep this file lean & robust.
// The “safety_mode” reorder uses a simple nearest-neighbor heuristic on Manhattan distance.

#include <unordered_map>
#include <algorithm>
#include <random>
#include <iostream>
#include <fstream>
#include <list>
#include <queue>
#include <deque>
#include <sstream>
#include <tuple>

using std::cout;
using std::endl;

// -----------------------------------------------------------------------------
// Small utilities (all safe-by-construction)
// -----------------------------------------------------------------------------

static inline int grid_vertex_count(const KivaGrid& G)
{
    // KivaGrid exposes rows/cols; total discrete vertices = rows * cols
    const int R = G.get_rows();
    const int C = G.get_cols();
    if (R <= 0 || C <= 0) return 0;
    return R * C;
}

static inline bool valid_vertex(const KivaGrid& G, int v)
{
    const int N = grid_vertex_count(G);
    return (N > 0) && v >= 0 && v < N;
}

static inline int clamp_vertex(const KivaGrid& G, int v)
{
    const int N = grid_vertex_count(G);
    if (N <= 0) return 0;
    if (v < 0) return 0;
    if (v >= N) return N - 1;
    return v;
}

// Return a random endpoint different from `avoid` (best effort)
static inline int pick_random_endpoint_except(const KivaGrid& G, int avoid)
{
    if (G.endpoints.empty())
        return clamp_vertex(G, avoid);

    // try a few times
    int goal = G.endpoints[std::max(0, rand() % (int)G.endpoints.size())];
    int guard = 16;
    while (guard-- > 0 && goal == avoid && !G.endpoints.empty())
        goal = G.endpoints[rand() % (int)G.endpoints.size()];

    if (goal == avoid) {
        // fallback: first endpoint that isn’t avoid; else first anyway
        for (int e : G.endpoints) { if (e != avoid) { goal = e; break; } }
        if (goal == avoid) goal = G.endpoints.front();
    }
    return clamp_vertex(G, goal);
}

// Ensure paths[k][t] exists; if not, extend by waiting in place at the last location.
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

// Safe endpoint predicate without indexing G.types
static inline bool is_endpoint_safe(const KivaGrid& G, int v)
{
    if (v < 0) return false;
    for (int e : G.endpoints) if (e == v) return true;
    return false;
}

// Clean SIPP goal list: clamp, drop self, dedup
static inline void clean_goals(const KivaGrid& G, int start_v, std::vector<int>& goals)
{
    for (int& g : goals) g = clamp_vertex(G, g);
    const int sv = clamp_vertex(G, start_v);
    goals.erase(std::remove(goals.begin(), goals.end(), sv), goals.end());
    std::sort(goals.begin(), goals.end());
    goals.erase(std::unique(goals.begin(), goals.end()), goals.end());
}

// Simple greedy segment path (collision-ignorant) for fallback
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

// Build an RT that always has at least one (valid) state per teammate path.
// Optionally crop to [0, horizon_t].
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
            // drop-in placeholder
            int home = (!G.agent_home_locations.empty())
                     ? clamp_vertex(G, G.agent_home_locations.front())
                     : 0;
            int ori = consider_rotation ? 0 : -1;
            tmp.push_back(State(home, 0, ori));
        } else {
            // sanitize
            tmp[0].location = clamp_vertex(G, tmp[0].location);
            for (size_t t = 1; t < tmp.size(); ++t) {
                tmp[t].location = clamp_vertex(G, tmp[t].location);
                if (tmp[t].timestep <= tmp[t-1].timestep) tmp[t].timestep = tmp[t-1].timestep + 1;
            }
        }

        others[i] = std::move(tmp);
    }
    std::list<std::tuple<int,int,int>> empty_initial;
    rt.build(others, empty_initial, current_agent);
}

// Nearest-neighbor reorder on Manhattan distance (kept very light)
static inline void reorder_bundle_nearest(const KivaGrid& G,
                                          const int start_v,
                                          std::deque<std::pair<int,int>>& bundle) // (v,release)
{
    if (bundle.size() <= 1) return;

    // collect goals as raw vertices
    std::vector<int> pool;
    pool.reserve(bundle.size());
    for (auto &g : bundle) pool.push_back(clamp_vertex(G, g.first));

    std::vector<int> order;
    order.reserve(pool.size());
    int cur = clamp_vertex(G, start_v);

    std::vector<char> used(pool.size(), 0);
    for (size_t it = 0; it < pool.size(); ++it) {
        int best = -1, best_d = INT_MAX;
        for (size_t i = 0; i < pool.size(); ++i) {
            if (used[i]) continue;
            const int d = G.get_Manhattan_distance(cur, pool[i]);
            if (d < best_d) { best_d = d; best = (int)i; }
        }
        if (best == -1) break;
        used[best] = 1;
        order.push_back(pool[best]);
        cur = pool[best];
    }

    // rebuild bundle following 'order'
    std::deque<std::pair<int,int>> re;
    for (int v : order) {
        for (auto it = bundle.begin(); it != bundle.end(); ++it) {
            if (clamp_vertex(G, it->first) == v) {
                re.push_back(*it);
                bundle.erase(it);
                break;
            }
        }
    }
    for (auto &g : bundle) re.push_back(g);
    bundle = std::move(re);
}

// -----------------------------------------------------------------------------
// KivaSystem core
// -----------------------------------------------------------------------------

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

void KivaSystem::reorder_bundle_by_dvs(int k)
{
    if (!safety_mode) return;
    if (k < 0 || k >= (int)bundle.size()) return;
    if (bundle[k].size() <= 1) return;

    const int start_v = safe_path_at(paths, G, k, timestep, consider_rotation).location;
    reorder_bundle_nearest(G, start_v, bundle[k]);
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

    if (restitch_on_change && !bundle_dirty[k] && timestep > 0) {
        metrics_after_stitch(false, false, false, true); // skipped clean
        return;
    }

    const int start_v = safe_path_at(paths, G, k, timestep, consider_rotation).location;
    const int start_t = timestep;

    std::vector<int> goals;
    goals.reserve(bundle[k].size());
    for (const auto& g : bundle[k]) goals.push_back(clamp_vertex(G, g.first));
    clean_goals(G, start_v, goals);

    if (goals.empty()) {
        bundle_dirty[k] = false;
        metrics_after_stitch(false, false, false, true); // nothing to stitch
        return;
    }

    if ((int)goals.size() > stitch_depth) goals.resize(stitch_depth);

    std::vector<State> new_suffix;
    bool used_sipp = false, sipp_ok = false, fell_back = false;

    if (stitch_use_sipp) {
        used_sipp = true;

        static const int OFFSETS[] = {0, 1, 2, 3, 5};
        for (int w : OFFSETS) {
            ReservationTable rt(G);
            build_rt_from_teammates_safe(G, paths, k,
                                         timestep + planning_window,
                                         /*crop=*/stitch_crop_horizon,
                                         consider_rotation,
                                         rt);

            State s0(clamp_vertex(G, start_v), start_t + w, -1);

            std::vector<std::pair<int,int>> gl; gl.reserve(goals.size());
            for (int g : goals) gl.emplace_back(g, 0);

            SIPP sipp;
            auto path = sipp.run(G, s0, gl, rt);
            if (path.empty()) continue;

            if (w > 0) {
                new_suffix.reserve(w + path.size());
                for (int i = 0; i < w; ++i) new_suffix.emplace_back(start_v, start_t + i, -1);
                new_suffix.insert(new_suffix.end(), path.begin(), path.end());
                for (size_t i = 1; i < new_suffix.size(); ++i)
                    if (new_suffix[i].timestep <= new_suffix[i-1].timestep)
                        new_suffix[i].timestep = new_suffix[i-1].timestep + 1;
            } else {
                new_suffix = std::move(path);
            }

            sipp_ok = true;
            break;
        }
    }

    if (!sipp_ok) {
        // Greedy fallback (never fails)
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

    suppress_replan_for(k);
    bundle_dirty[k] = false;
    metrics_after_stitch(used_sipp, sipp_ok, fell_back, false);
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
    for (int k : to_go) {
        plan_stitched_for_agent(k);
        // Next agent’s RT will now see the updated path[k]
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

    // Legacy behavior (unchanged, but SAFE: no types[] indexing)
    if (hold_endpoints)
    {
        unordered_map<int, int> held_locations; // <location, agent id>
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

        // MAPF solver for any agents left in new_agents
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
