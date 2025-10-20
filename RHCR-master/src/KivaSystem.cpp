#include "KivaSystem.h"
#include "WHCAStar.h"
#include "ECBS.h"
#include "LRAStar.h"
#include "PBS.h"

#include "MultiGoalPlannerDVS.h"
#include "SIPP.h"
#include "ReservationTable.h"

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

// Quick bounds check
static inline bool valid_vertex(const KivaGrid& G, int v)
{
    const int N = G.get_rows() * G.get_cols();
    return v >= 0 && v < N;
}

// Ensure paths[k][t] exists
static inline const State& safe_path_at(std::vector<std::vector<State>>& paths,
                                        const KivaGrid& G,
                                        int k, int t,
                                        bool consider_rotation)
{
    if (k < 0) k = 0;
    if (k >= (int)paths.size()) paths.resize(k + 1);

    if (paths[k].empty()) {
        int home = (k < (int)G.agent_home_locations.size())
                 ? G.agent_home_locations[k]
                 : (!G.endpoints.empty() ? G.endpoints[k % (int)G.endpoints.size()] : 0);
        if (!valid_vertex(G, home)) home = 0;
        int ori  = consider_rotation ? 0 : -1;
        paths[k].push_back(State(home, 0, ori));
    }
    while ((int)paths[k].size() <= t) {
        const State& last = paths[k].back();
        int loc = valid_vertex(G, last.location) ? last.location : 0;
        paths[k].push_back(State(loc, last.timestep + 1, last.orientation));
    }
    return paths[k][t];
}

static inline int random_endpoint_not(const KivaGrid& G, int curr)
{
    if (G.endpoints.empty()) return valid_vertex(G, curr) ? curr : 0;
    int goal = G.endpoints[rand() % (int)G.endpoints.size()];
    int tries = 12;
    while (tries-- && goal == curr) {
        goal = G.endpoints[rand() % (int)G.endpoints.size()];
    }
    if (!valid_vertex(G, goal)) goal = 0;
    return goal;
}

// Greedy stepper fallback
static inline std::vector<std::pair<int,int>>
safe_step_path(const KivaGrid& G, int v0, int t0, int v_goal)
{
    std::vector<std::pair<int,int>> out;
    if (!valid_vertex(G, v0)) v0 = 0;
    if (!valid_vertex(G, v_goal)) v_goal = 0;

    out.emplace_back(v0, t0);
    if (v0 == v_goal) return out;

    int t = t0;
    int v = v0;

    auto step_towards = [&](int target){
        if (!valid_vertex(G, v)) return false;
        int best = -1;
        int best_d = G.get_Manhattan_distance(v, target);
        for (int nb : G.get_neighbors(v)) {
            if (!valid_vertex(G, nb)) continue;
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

KivaSystem::KivaSystem(const KivaGrid& G_, MAPFSolver& solver): BasicSystem(G_, solver), G(G_) {}
KivaSystem::~KivaSystem() {}

// -------------------------------- initialize --------------------------------

void KivaSystem::initialize()
{
    initialize_solvers();

    starts.resize(num_of_drives);
    goal_locations.resize(num_of_drives);
    paths.resize(num_of_drives);
    finished_tasks.resize(num_of_drives);

    bundle.assign(num_of_drives, {});
    rest.assign(num_of_drives, {});
    bundle_dirty.assign(num_of_drives, true); // stitch at t=0

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
        int orientation = -1;
        if (consider_rotation) orientation = rand() % 4;

        int home = (k < (int)G.agent_home_locations.size()) ? G.agent_home_locations[k] : 0;
        if (!valid_vertex(G, home)) home = 0;

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
        int goal = random_endpoint_not(G, curr);
        goal_locations[k].emplace_back(goal, 0);
    }
}

// -------------------------------- helpers -----------------------------------

void KivaSystem::ensure_goal_exists(int k, int curr)
{
    if (k < 0 || k >= (int)goal_locations.size()) return;
    if (!goal_locations[k].empty()) return;
    int g = random_endpoint_not(G, curr);
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
            int v = valid_vertex(G, seq[i]) ? seq[i] : 0;
            Goal g = { v, 0 };
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

    int curr = safe_path_at(paths, G, k, timestep, consider_rotation).location;
    const Goal& front = bundle[k].front();
    if (curr == front.first && timestep >= front.second) {
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
        for (const auto& g : bundle[i]) claimed.insert(g.first);
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

        if (avoid_dup_goals && claimed.count(g.first)) {
            rest[k].push_back(g);
            bool all_claimed = true;
            for (const auto& r : rest[k]) { if (!claimed.count(r.first)) { all_claimed = false; break; } }
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
            int v = valid_vertex(G, g.first) ? g.first : 0;
            goal_locations[k].push_back({v, g.second});
        }
        if (goal_locations[k].empty()) {
            int curr = safe_path_at(paths, G, k, timestep, consider_rotation).location;
            ensure_goal_exists(k, curr);
        }
        bundle_assert_capacity_ok(k);
    }
}

// DVS reorder (optional)
void KivaSystem::reorder_bundle_by_dvs(int k)
{
    if (!safety_mode) return;
    if (k < 0 || k >= (int)bundle.size()) return;
    if (bundle[k].size() <= 1) return;

    std::vector<int> goals;
    goals.reserve(bundle[k].size());
    for (auto& g : bundle[k]) goals.push_back(valid_vertex(G, g.first) ? g.first : 0);

    mgmapf::MultiGoalPlannerDVS::DistanceOracle dist = [&](int u, int v){
        if (!valid_vertex(G, u) || !valid_vertex(G, v)) return 0;
        return G.get_Manhattan_distance(u, v);
    };

    mgmapf::DVSConfig cfg;
    cfg.use_segment_planner = false;
    mgmapf::MultiGoalPlannerDVS dvs(dist, nullptr, cfg);

    int start_v = safe_path_at(paths, G, k, timestep, consider_rotation).location;
    auto plan = dvs.plan(start_v, 0, goals);
    if (!plan.success || plan.goal_order.empty()) return;

    std::deque<Goal> reordered;
    for (int idx : plan.goal_order) {
        int v = goals[idx];
        for (auto it = bundle[k].begin(); it != bundle[k].end(); ++it) {
            if (it->first == v) {
                reordered.push_back(*it);
                bundle[k].erase(it);
                break;
            }
        }
    }
    for (auto& g : bundle[k]) reordered.push_back(g);
    bundle[k] = std::move(reordered);
    bundle_dirty[k] = true;
    m_restitches_total++;
}

int KivaSystem::generate_endpoint_for(int, int avoid_v) const
{
    return random_endpoint_not(G, avoid_v);
}

void KivaSystem::maybe_autorefill_rest(int k)
{
    if (!auto_refill) return;
    if (k < 0 || k >= (int)rest.size()) return;

    if (rest[k].empty()) {
        int curr = safe_path_at(paths, G, k, timestep, consider_rotation).location;

        std::unordered_set<int> claimed = collect_claimed_active_endpoints(k);
        claimed.insert(curr);

        int attempts_per_goal = 16;
        const int cap = cap_of(k);
        for (int i = 0; i < cap; ++i) {
            int v = generate_endpoint_for(k, curr);
            int tries = attempts_per_goal;
            while (avoid_dup_goals && claimed.count(v) && tries-- > 0) {
                v = generate_endpoint_for(k, curr);
            }
            if (!valid_vertex(G, v)) v = 0;
            rest[k].push_back({v, 0});
            claimed.insert(v);
        }
    }
}

// ------------------------------- RT helpers ---------------------------------

void KivaSystem::build_rt_from_teammates(int current_agent, ReservationTable& rt) const
{
    std::vector<Path> others(paths.size());
    for (int i = 0; i < (int)paths.size(); ++i) {
        if (i == current_agent) continue;
        others[i] = paths[i];
    }
    std::list<std::tuple<int,int,int>> empty_initial;
    rt.build(others, empty_initial, current_agent);
}

void KivaSystem::build_rt_from_teammates_with_crop(int current_agent, int horizon_t, ReservationTable& rt) const
{
    std::vector<Path> others(paths.size());
    for (int i = 0; i < (int)paths.size(); ++i) {
        if (i == current_agent) continue;
        const auto& P = paths[i];
        Path cropped;
        for (const auto& s : P) {
            if (s.timestep <= horizon_t) cropped.push_back(s);
            else break;
        }
        if (cropped.empty() && !P.empty())
            cropped.push_back(P.front());
        others[i] = std::move(cropped);
    }
    std::list<std::tuple<int,int,int>> empty_initial;
    rt.build(others, empty_initial, current_agent);
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
        else         { m_sipp_fail_total++; } // then we probably fell back
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

// ------------------------------- Stitching ----------------------------------

void KivaSystem::suppress_replan_for(int k)
{
    for (auto it = new_agents.begin(); it != new_agents.end(); ) {
        if (*it == k) it = new_agents.erase(it);
        else ++it;
    }
}

static bool build_sipp_multi_goal_path(const KivaGrid& G,
                                       int start_v, int start_t,
                                       const std::vector<int>& goals,
                                       ReservationTable& rt,
                                       std::vector<State>& out_path)
{
    if (goals.empty()) return false;

    State s0(start_v, start_t, -1);
    std::vector<std::pair<int,int>> gl; gl.reserve(goals.size());
    for (int g : goals) gl.emplace_back(g, 0);

    SIPP sipp;
    auto path = sipp.run(G, s0, gl, rt);
    if (path.empty()) return false;

    out_path = std::move(path);
    return true;
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

    int start_v = safe_path_at(paths, G, k, timestep, consider_rotation).location;
    int start_t = timestep;

    std::vector<int> goals;
    goals.reserve(bundle[k].size());
    for (const auto& g : bundle[k]) if (valid_vertex(G, g.first)) goals.push_back(g.first);
    if (!goals.empty() && goals.front() == start_v) goals.erase(goals.begin());
    if (goals.empty()) { bundle_dirty[k] = false; metrics_after_stitch(false,false,false,true); return; }

    if ((int)goals.size() > stitch_depth) goals.resize(stitch_depth);

    std::vector<State> new_suffix;
    bool used_sipp = false, sipp_ok = false, fell_back = false;

    if (stitch_use_sipp) {
        used_sipp = true;
        ReservationTable rt(G);
        if (stitch_crop_horizon)
            build_rt_from_teammates_with_crop(k, timestep + planning_window, rt);
        else
            build_rt_from_teammates(k, rt);

        sipp_ok = build_sipp_multi_goal_path(G, start_v, start_t, goals, rt, new_suffix);
    }
    if (!sipp_ok) {
        // fallback greedy
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
        new_suffix.reserve(seq.size());
        for (auto &p : seq) new_suffix.emplace_back(p.first, p.second, -1);
        fell_back = true;
    }

    if (new_suffix.empty()) { bundle_dirty[k] = false; metrics_after_stitch(used_sipp, sipp_ok, fell_back, false); return; }

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
                for (auto &g : bundle[k]) { c += G.get_Manhattan_distance(cur, g.first); cur = g.first; }
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
            int ca = bundle[a].empty() ? 1e9 : G.get_Manhattan_distance(sa, bundle[a].front().first);
            int cb = bundle[b].empty() ? 1e9 : G.get_Manhattan_distance(sb, bundle[b].front().first);
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
        // paths[k] is updated; next agent’s RT will include it via build_rt_*()
    }
}

// -------------------------------- update ------------------------------------



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
                oss << bundle[k][i].first;
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

    // Legacy behavior (unchanged)
    if (hold_endpoints)
    {
        unordered_map<int, int> held_locations;
        for (int k = 0; k < num_of_drives; k++)
        {
            int curr = safe_path_at(paths, G, k, timestep, consider_rotation).location;
            if (goal_locations[k].empty())
            {
                int next = G.endpoints[rand() % (int)G.endpoints.size()];
                while (next == curr || held_endpoints.find(next) != held_endpoints.end())
                {
                    next = G.endpoints[rand() % (int)G.endpoints.size()];
                }
                goal_locations[k].emplace_back(next, 0);
                held_endpoints.insert(next);
            }
            if (paths[k].back().location == goal_locations[k].back().first &&
                paths[k].back().timestep >= goal_locations[k].back().second)
            {
                int agent = k;
                int loc = goal_locations[k].back().first;
                auto it = held_locations.find(loc);
                while (it != held_locations.end())
                {
                    int removed_agent = it->second;
                    if (goal_locations[removed_agent].back().first != loc)
                        cout << "BUG" << endl;
                    new_agents.remove(removed_agent);
                    cout << "Agent " << removed_agent << " has to wait for agent " << agent << " because of location " << loc << endl;
                    held_locations[loc] = agent;
                    agent = removed_agent;
                    loc = safe_path_at(paths, G, agent, timestep, consider_rotation).location;
                    it = held_locations.find(loc);
                }
                held_locations[loc] = agent;
            }
            else
            {
                if (held_locations.find(goal_locations[k].back().first) == held_locations.end())
                {
                    held_locations[goal_locations[k].back().first] = k;
                    new_agents.emplace_back(k);
                    continue;
                }
                int agent = k;
                int loc = curr;
                cout << "Agent " << agent
                     << " has to wait for agent "
                     << held_locations[goal_locations[k].back().first]
                     << " because of location "
                     << goal_locations[k].back().first
                     << endl;
                auto it = held_locations.find(loc);
                while (it != held_locations.end())
                {
                    int removed_agent = it->second;
                    if (goal_locations[removed_agent].back().first != loc)
                        cout << "BUG" << endl;
                    new_agents.remove(removed_agent);
                    cout << "Agent " << removed_agent << " has to wait for agent " << agent << " because of location " << loc << endl;
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
                    goal_locations[k].emplace_back(G.agent_home_locations[k], 0);
                }
                if (goal_locations[k].size() == 1)
                {
                    int next;
                    do {
                        next = G.endpoints[rand() % (int)G.endpoints.size()];
                    } while (next == curr);
                    goal_locations[k].emplace(goal_locations[k].begin(), next, 0);
                    new_agents.emplace_back(k);
                }
            }
            else
            {
                std::pair<int, int> goal;
                if (goal_locations[k].empty())
                {
                    goal = std::make_pair(curr, 0);
                }
                else
                {
                    goal = goal_locations[k].back();
                }
                double min_timesteps = G.get_Manhattan_distance(goal.first, curr);
                while (min_timesteps <= simulation_window)
                {
                    std::pair<int, int> next;
                    if (G.types[goal.first] == "Endpoint")
                    {
                        do
                        {
                            next = std::make_pair(G.endpoints[rand() % (int)G.endpoints.size()], 0);
                        } while (next == goal);
                    }
                    else
                    {
                        std::cout << "ERROR in update_goal_function()" << std::endl;
                        std::cout << "The fiducial type should not be " << G.types[curr] << std::endl;
                        exit(-1);
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

        // solver for remaining agents
        solve();

        // move drives
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
