#include "KivaSystem.h"
#include "WHCAStar.h"
#include "ECBS.h"
#include "LRAStar.h"
#include "PBS.h"

// DVS ordering (distance oracle)
#include "MultiGoalPlannerDVS.h"

// SIPP stitching
#include "SIPP.h"
#include "ReservationTable.h"

#include <unordered_map>
#include <algorithm>
#include <random>
#include <iostream>
#include <list>
#include <queue>
#include <deque>
#include <sstream>
#include <tuple>

using std::cout;
using std::endl;

// Quick bounds check helper to avoid invalid vertex reads
static inline bool valid_vertex(const KivaGrid& G, int v)
{
    const int N = G.get_rows() * G.get_cols();
    return v >= 0 && v < N;
}

// -----------------------------------------------------------------------------
// SAFETY HELPER: ensure paths[k][t] exists; seed start if empty and extend with waits
// -----------------------------------------------------------------------------
static inline const State& safe_path_at(std::vector<std::vector<State>>& paths,
                                        const KivaGrid& G,
                                        int k, int t,
                                        bool consider_rotation)
{
    if (k < 0) k = 0;
    if (k >= (int)paths.size()) paths.resize(k + 1);

    // seed if empty
    if (paths[k].empty()) {
        int home = (k < (int)G.agent_home_locations.size())
                 ? G.agent_home_locations[k]
                 : (!G.endpoints.empty() ? G.endpoints[k % (int)G.endpoints.size()] : 0);
        if (!valid_vertex(G, home)) home = 0;
        int ori  = consider_rotation ? 0 : -1;
        paths[k].push_back(State(home, 0, ori));
    }
    // extend up to time t by waiting in place
    while ((int)paths[k].size() <= t) {
        const State& last = paths[k].back();
        int loc = valid_vertex(G, last.location) ? last.location : 0;
        paths[k].push_back(State(loc, last.timestep + 1, last.orientation));
    }
    return paths[k][t];
}

// convenience: fallback random endpoint != curr
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

// -----------------------------------------------------------------------------
// Fallback stepper: greedily walk toward target (grid-adjacent), building (v,t)
// -----------------------------------------------------------------------------
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

    const int CAP = 20000; // guard against pathological cases
    int guard = 0;
    while (v != v_goal && guard++ < CAP) {
        if (!step_towards(v_goal)) break;
    }
    return out;
}

// -----------------------------------------------------------------------------
// KivaSystem (capacity + endpoint-claiming + debug + per-agent caps + stitching)
// -----------------------------------------------------------------------------

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

    // capacity containers sized, but unused unless capacity_mode=true
    bundle.assign(num_of_drives, {});
    rest.assign(num_of_drives, {});

    bool succ = load_records(); // continue simulating from the records
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

    // initialize capacity scaffolding if the user provided goals
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
        int curr = safe_path_at(paths, G, k, /*t=*/0, consider_rotation).location;
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

// capacity getter
int KivaSystem::cap_of(int k) const
{
    if (!per_agent_capacity.empty() && k >= 0 && k < (int)per_agent_capacity.size()) {
        int c = per_agent_capacity[k];
        return c > 0 ? c : 1;
    }
    return default_agent_capacity > 0 ? default_agent_capacity : 1;
}

// ===== capacity scaffolding =====
void KivaSystem::bundle_configure(int n, int capacity, bool randomize, unsigned seed)
{
    (void)n; // already sized in initialize()
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

        // fill active bundle up to capacity; overflow to rest
        const int cap = cap_of(k);
        for (size_t i = 0; i < seq.size(); ++i) {
            int v = valid_vertex(G, seq[i]) ? seq[i] : 0;
            Goal g = { v, 0 };
            if ((int)bundle[k].size() < cap) bundle[k].push_back(g);
            else                              rest[k].push_back(g);
        }
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

    // global claimed set (excluding self)
    std::unordered_set<int> claimed = avoid_dup_goals ? collect_claimed_active_endpoints(k)
                                                      : std::unordered_set<int>{};

    // move from rest -> bundle up to capacity
    const int cap = cap_of(k);
    int guard = 0;
    while ((int)bundle[k].size() < cap && !rest[k].empty() && guard++ < 2000) {
        auto g = rest[k].front(); rest[k].pop_front();

        if (avoid_dup_goals && claimed.count(g.first)) {
            // rotate to the back; we’ll try others first
            rest[k].push_back(g);
            // if everything left is claimed, break
            bool all_claimed = true;
            for (const auto& r : rest[k]) { if (!claimed.count(r.first)) { all_claimed = false; break; } }
            if (all_claimed) break;
            continue;
        }

        bundle[k].push_back(g);
        changed = true;
    }
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
    // Mirror active bundle -> goal_locations; keep legacy structure
    for (int k = 0; k < num_of_drives; ++k) {
        goal_locations[k].clear();
        for (const auto& g : bundle[k]) {
            int v = valid_vertex(G, g.first) ? g.first : 0;
            goal_locations[k].push_back({v, g.second});
        }
        // If empty, keep at least one fallback goal so solver has a target
        if (goal_locations[k].empty()) {
            int curr = safe_path_at(paths, G, k, timestep, consider_rotation).location;
            ensure_goal_exists(k, curr);
        }
        bundle_assert_capacity_ok(k);
    }
}

// ===== DVS-based ordering, gated by safety_mode =====
void KivaSystem::reorder_bundle_by_dvs(int k)
{
    if (!safety_mode) return;
    if (k < 0 || k >= (int)bundle.size()) return;
    if (bundle[k].size() <= 1) return; // nothing to reorder

    // Build a goals vector from the current bundle
    std::vector<int> goals;
    goals.reserve(bundle[k].size());
    for (auto& g : bundle[k]) goals.push_back(valid_vertex(G, g.first) ? g.first : 0);

    // Distance oracle = Manhattan on KivaGrid
    mgmapf::MultiGoalPlannerDVS::DistanceOracle dist = [&](int u, int v){
        if (!valid_vertex(G, u) || !valid_vertex(G, v)) return 0;
        return G.get_Manhattan_distance(u, v);
    };

    mgmapf::DVSConfig cfg;
    cfg.use_segment_planner = false; // ordering only
    mgmapf::MultiGoalPlannerDVS dvs(dist, nullptr, cfg);

    int start_v = safe_path_at(paths, G, k, timestep, consider_rotation).location;

    auto plan = dvs.plan(start_v, /*start_t=*/0, goals);
    if (!plan.success || plan.goal_order.empty()) return;

    // Reorder bundle according to the returned visiting order
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
    for (auto& g : bundle[k]) reordered.push_back(g); // any leftovers
    bundle[k] = std::move(reordered);
}

// ===== generate / autorefill =====
int KivaSystem::generate_endpoint_for(int k, int avoid_v) const
{
    (void)k;
    return random_endpoint_not(G, avoid_v);
}

void KivaSystem::maybe_autorefill_rest(int k)
{
    if (!auto_refill) return;
    if (k < 0 || k >= (int)rest.size()) return;

    if (rest[k].empty()) {
        int curr = safe_path_at(paths, G, k, timestep, consider_rotation).location;

        std::unordered_set<int> claimed = collect_claimed_active_endpoints(k);
        claimed.insert(curr); // don't generate the current cell

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

// -------------------------------- RT helpers --------------------------------

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
        // crop teammate paths at horizon
        const auto& P = paths[i];
        Path cropped;
        for (const auto& s : P) {
            if (s.timestep <= horizon_t) cropped.push_back(s);
            else break;
        }
        if (cropped.empty() && !P.empty())
            cropped.push_back(P.front()); // at least something so start is reserved
        others[i] = std::move(cropped);
    }
    std::list<std::tuple<int,int,int>> empty_initial;
    rt.build(others, empty_initial, current_agent);
}

// -------------------------------- stitching (engine choice) ------------------

void KivaSystem::suppress_replan_for(int k)
{
    for (auto it = new_agents.begin(); it != new_agents.end(); ) {
        if (*it == k) it = new_agents.erase(it);
        else ++it;
    }
}

// SIPP multi-goal with provided RT
static bool build_sipp_multi_goal_path(const KivaGrid& G,
                                       int start_v, int start_t,
                                       const std::vector<int>& goals,
                                       ReservationTable& rt,
                                       std::vector<State>& out_path)
{
    if (goals.empty()) return false;

    State s0(start_v, start_t, -1);

    std::vector<std::pair<int,int>> gl;
    gl.reserve(goals.size());
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

    int start_v = safe_path_at(paths, G, k, timestep, consider_rotation).location;
    int start_t = timestep;

    std::vector<int> goals;
    goals.reserve(bundle[k].size());
    for (const auto& g : bundle[k]) {
        if (valid_vertex(G, g.first)) goals.push_back(g.first);
    }
    if (!goals.empty() && goals.front() == start_v) goals.erase(goals.begin());
    if (goals.empty()) return;

    const int MAX_STITCH = 6;
    if ((int)goals.size() > MAX_STITCH) goals.resize(MAX_STITCH);

    std::vector<State> new_suffix;
    bool ok = false;

    if (stitch_use_sipp) {
        ReservationTable rt(G);
        if (stitch_crop_horizon)
            build_rt_from_teammates_with_crop(k, timestep + planning_window, rt);
        else
            build_rt_from_teammates(k, rt);

        ok = build_sipp_multi_goal_path(G, start_v, start_t, goals, rt, new_suffix);
    }
    if (!ok) {
        std::vector<std::pair<int,int>> seq;
        seq.emplace_back(start_v, start_t);
        int v = start_v, t = start_t;
        for (int g : goals) {
            auto seg = safe_step_path(G, v, t, g);
            if (seg.size() <= 1) continue;
            seq.insert(seq.end(), seg.begin() + 1, seg.end());
            v = g; t = seq.back().second;
        }
        new_suffix.reserve(seq.size());
        for (auto &p : seq) new_suffix.emplace_back(p.first, p.second, -1);
        ok = true;
    }

    if (!ok || new_suffix.empty()) return;

    // Build new_path: prefix up to timestep-1 + stitched suffix
    std::vector<State> new_path;
    new_path.reserve(paths[k].size() + new_suffix.size());

    for (int i = 0; i < (int)paths[k].size() && paths[k][i].timestep < timestep; ++i)
        new_path.push_back(paths[k][i]);

    new_path.insert(new_path.end(), new_suffix.begin(), new_suffix.end());

    paths[k] = std::move(new_path);
    suppress_replan_for(k); // keep our stitched plan intact for this tick
}

// ---- NEW: batch stitching where each stitched agent is injected into RT for the next

std::vector<int> KivaSystem::compute_batch_order() const
{
    std::vector<int> pool;

    // who is eligible? If stitch_target == -1 => all; else just that agent if valid
    if (stitch_target == -1) {
        pool.reserve(num_of_drives);
        for (int k = 0; k < num_of_drives; ++k) pool.push_back(k);
    } else if (stitch_target >= 0 && stitch_target < num_of_drives) {
        pool.push_back(stitch_target);
    } else {
        return pool; // empty
    }

    // filter only those with non-empty bundle
    pool.erase(std::remove_if(pool.begin(), pool.end(), [&](int k){
        return !capacity_mode || bundle[k].empty();
    }), pool.end());

    // Now choose order
    switch (stitch_batch_order) {
    case StitchOrder::ByIndex:
        // already index order
        break;
    case StitchOrder::ShortestRemaining: {
        // estimate remaining steps = sum Manhattan between chain start->goal1->... (cheap)
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
    // Build a working copy of paths that we’ll keep updating as we stitch
    // (We can just write into paths directly, but keeping the logic obvious.)
    // We’ll construct RT for each agent using current "paths" so far.
    auto to_go = compute_batch_order();
    for (int k : to_go) {
        // stitch k
        int start_v = safe_path_at(paths, G, k, timestep, consider_rotation).location;
        int start_t = timestep;

        std::vector<int> goals;
        goals.reserve(bundle[k].size());
        for (const auto& g : bundle[k]) {
            if (valid_vertex(G, g.first)) goals.push_back(g.first);
        }
        if (!goals.empty() && goals.front() == start_v) goals.erase(goals.begin());
        if (goals.empty()) continue;

        const int MAX_STITCH = 6;
        if ((int)goals.size() > MAX_STITCH) goals.resize(MAX_STITCH);

        std::vector<State> new_suffix;
        bool ok = false;

        if (stitch_use_sipp) {
            ReservationTable rt(G);
            if (stitch_crop_horizon)
                build_rt_from_teammates_with_crop(k, timestep + planning_window, rt);
            else
                build_rt_from_teammates(k, rt);

            ok = build_sipp_multi_goal_path(G, start_v, start_t, goals, rt, new_suffix);
        }
        if (!ok) {
            std::vector<std::pair<int,int>> seq;
            seq.emplace_back(start_v, start_t);
            int v = start_v, t = start_t;
            for (int g : goals) {
                auto seg = safe_step_path(G, v, t, g);
                if (seg.size() <= 1) continue;
                seq.insert(seq.end(), seg.begin() + 1, seg.end());
                v = g; t = seq.back().second;
            }
            new_suffix.reserve(seq.size());
            for (auto &p : seq) new_suffix.emplace_back(p.first, p.second, -1);
            ok = true;
        }
        if (!ok || new_suffix.empty()) continue;

        // write stitched plan for k (prefix up to timestep-1 + suffix)
        std::vector<State> new_path;
        new_path.reserve(paths[k].size() + new_suffix.size());
        for (int i = 0; i < (int)paths[k].size() && paths[k][i].timestep < timestep; ++i)
            new_path.push_back(paths[k][i]);
        new_path.insert(new_path.end(), new_suffix.begin(), new_suffix.end());
        paths[k] = std::move(new_path);

        suppress_replan_for(k); // solver must not overwrite our stitched plan
        // Now, since paths[k] is updated, the next iteration’s RT build() will include it.
    }
}

// -------------------------------- update (baseline + capacity mirror) -------

void KivaSystem::update_goal_locations()
{
    if (!LRA_called)
        new_agents.clear();

    // ===== Capacity-aware branch =====
    if (capacity_mode)
    {
        for (int k = 0; k < num_of_drives; ++k) {
            bool popped = bundle_on_goal_reached(k); // front goal reached?

            size_t size_before = bundle[k].size();

            maybe_autorefill_rest(k);                // ensure backlog has content
            bool topped = bundle_maybe_top_up(k);    // pulled new goals into active bundle?

            bool changed = popped || topped;

            if (safety_mode) {                       // optional reordering
                reorder_bundle_by_dvs(k);
                changed = true;
            }

            if (timestep == 0 || changed || bundle[k].size() != size_before) {
                new_agents.emplace_back(k);
            }
        }

        // reflect active bundle -> goal_locations for the solver
        bundle_mirror_to_engine();

        if (capacity_debug) debug_print_capacity_state();

        return; // global solve() will read new_agents and produce motion
    }

    // ===== Legacy behavior (unchanged) =====
    if (hold_endpoints)
    {
        unordered_map<int, int> held_locations; // <location, agent id>
        for (int k = 0; k < num_of_drives; k++)
        {
            int curr = safe_path_at(paths, G, k, timestep, consider_rotation).location; // current location
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

// --------------------------------- debug ------------------------------------

void KivaSystem::debug_print_capacity_state() const
{
    if (!capacity_mode) return;
    std::ostringstream oss;
    oss << "[t=" << timestep << "] CAPACITY STATE\n";
    for (int k = 0; k < num_of_drives; ++k) {
        oss << "  agent " << k << " cap=" << cap_of(k) << " bundle=[";
        for (size_t i = 0; i < bundle[k].size(); ++i) {
            oss << bundle[k][i].first;
            if (i + 1 < bundle[k].size()) oss << ",";
        }
        oss << "] rest=(" << rest[k].size() << ")\n";
    }
    oss << "  replans: [";
    bool first = true;
    for (int a : new_agents) { if (!first) oss << ","; first = false; oss << a; }
    oss << "]\n";
    cout << oss.str();
}

// --------------------------------- simulate ---------------------------------

void KivaSystem::simulate(int simulation_time)
{
    std::cout << "*** Simulating " << seed << " ***" << std::endl;
    this->simulation_time = simulation_time;
    initialize();

    for (; timestep < simulation_time; timestep += simulation_window)
    {
        std::cout << "Timestep " << timestep << std::endl;

        update_start_locations();
        update_goal_locations();

        // Stitched execution
        if (capacity_mode && stitch_mode) {
            // Use batch so stitched plans are mutually consistent in one tick
            plan_stitched_batch();
        }

        // Global multi-agent solver plans remaining agents in new_agents.
        solve();

        // move drives
        auto new_finished_tasks = move();
        std::cout << new_finished_tasks.size() << " tasks has been finished" << std::endl;

        // update tasks
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
    }

    update_start_locations();
    std::cout << std::endl << "Done!" << std::endl;
    save_results();
}
