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

    // Distance oracle = Manhattan on KivaGrid (always available)
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

    // If backlog is empty, generate a few fresh goals so we can top-up smoothly
    if (rest[k].empty()) {
        int curr = safe_path_at(paths, G, k, timestep, consider_rotation).location;

        // Prepare claimed set so we avoid duplicates right at generation time
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

// -------------------------------- stitching (engine choice) ------------------

void KivaSystem::suppress_replan_for(int k)
{
    // Remove k from new_agents so solve() does not overwrite the stitched path
    for (auto it = new_agents.begin(); it != new_agents.end(); ) {
        if (*it == k) it = new_agents.erase(it);
        else ++it;
    }
}

// Build a stitched path using SIPP across the agent's active bundle.
// NOTE: this step uses an EMPTY ReservationTable for now (local collision avoidance).
// Next step we’ll inject other agents’ paths into RT for true conflict-aware stitching.
static bool build_sipp_multi_goal_path(const KivaGrid& G,
                                       int start_v, int start_t,
                                       const std::vector<int>& goals,
                                       std::vector<State>& out_path)
{
    if (goals.empty()) return false;

    // Build SIPP start state (orientation is ignored/handled inside SIPP)
    State s0(start_v, start_t, -1);

    // Build goal_location vector<pair<int,int>> expected by SIPP (vertex, release_time)
    std::vector<std::pair<int,int>> gl;
    gl.reserve(goals.size());
    for (int g : goals) gl.emplace_back(g, 0);

    ReservationTable rt(G); // empty RT in this step (no other-agent reservations yet)
    SIPP sipp;
    auto path = sipp.run(G, s0, gl, rt); // SIPP::run supports multi-goal chain 【turn5file5†SIPP.cpp†L57-L61】

    if (path.empty()) return false;
    out_path = std::move(path);
    return true;
}

void KivaSystem::plan_stitched_for_agent(int k)
{
    if (k < 0 || k >= num_of_drives) return;
    if (!capacity_mode) return;           // stitching uses active bundle as sequence
    if (bundle[k].empty()) return;

    // Build sanitized goal list from active bundle
    int start_v = safe_path_at(paths, G, k, timestep, consider_rotation).location;
    int start_t = timestep;

    std::vector<int> goals;
    goals.reserve(bundle[k].size());
    for (const auto& g : bundle[k]) {
        if (valid_vertex(G, g.first)) goals.push_back(g.first);
    }
    // if first is current location, skip it
    if (!goals.empty() && goals.front() == start_v) goals.erase(goals.begin());
    if (goals.empty()) return;

    // Limit stitched length to keep it manageable
    const int MAX_STITCH = 6;
    if ((int)goals.size() > MAX_STITCH) goals.resize(MAX_STITCH);

    std::vector<State> new_suffix;

    bool ok = false;
    if (stitch_use_sipp) {
        // Preferred: SIPP-based multi-goal path (local RT for now)
        ok = build_sipp_multi_goal_path(G, start_v, start_t, goals, new_suffix);
    }
    if (!ok) {
        // Fallback: greedy stepper concatenation (always succeeds)
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

void KivaSystem::plan_stitched_all_applicable_agents()
{
    if (!stitch_mode) return;
    if (!capacity_mode) return;

    if (stitch_target == -1) {
        for (int k = 0; k < num_of_drives; ++k) plan_stitched_for_agent(k);
    } else if (stitch_target >= 0 && stitch_target < num_of_drives) {
        plan_stitched_for_agent(stitch_target);
    } else {
        // out-of-range stitch_target: ignore
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

            // Optional: reorder (distance-only). If on, conservatively mark as changed.
            if (safety_mode) {
                reorder_bundle_by_dvs(k);
                changed = true;
            }

            // Mark agents needing planning
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
            if (paths[k].back().location == goal_locations[k].back().first &&  // agent already has paths to its goal location
                paths[k].back().timestep >= goal_locations[k].back().second) // after its release time
            {
                int agent = k;
                int loc = goal_locations[k].back().first;
                auto it = held_locations.find(loc);
                while (it != held_locations.end()) // its start location has been held by another agent
                {
                    int removed_agent = it->second;
                    if (goal_locations[removed_agent].back().first != loc)
                        cout << "BUG" << endl;
                    new_agents.remove(removed_agent); // another agent cannot move to its new goal location
                    cout << "Agent " << removed_agent << " has to wait for agent " << agent << " because of location " << loc << endl;
                    held_locations[loc] = agent; // this agent has to keep holding this location
                    agent = removed_agent;
                    loc = safe_path_at(paths, G, agent, timestep, consider_rotation).location; // another agent's start location
                    it = held_locations.find(loc);
                }
                held_locations[loc] = agent;
            }
            else // agent does not have paths to its goal location yet
            {
                if (held_locations.find(goal_locations[k].back().first) == held_locations.end()) // if the goal location has not been held by other agents
                {
                    held_locations[goal_locations[k].back().first] = k; // hold this goal location
                    new_agents.emplace_back(k); // replan paths for this agent later
                    continue;
                }
                // the goal location has already been held by other agents 
                // so this agent has to keep holding its start location instead
                int agent = k;
                int loc = curr;
                cout << "Agent " << agent
                     << " has to wait for agent "
                     << held_locations[goal_locations[k].back().first]
                     << " because of location "
                     << goal_locations[k].back().first
                     << endl;
                auto it = held_locations.find(loc);
                while (it != held_locations.end()) // its start location has been held by another agent
                {
                    int removed_agent = it->second;
                    if (goal_locations[removed_agent].back().first != loc)
                        cout << "BUG" << endl;
                    new_agents.remove(removed_agent); // another agent cannot move to its new goal location
                    cout << "Agent " << removed_agent << " has to wait for agent " << agent << " because of location " << loc << endl;
                    held_locations[loc] = agent; // this agent has to keep holding its start location
                    agent = removed_agent;
                    loc = safe_path_at(paths, G, agent, timestep, consider_rotation).location; // another agent's start location
                    it = held_locations.find(loc);
                }
                held_locations[loc] = agent;// this agent has to keep holding its start location
            }
        }
    }
    else
    {
        for (int k = 0; k < num_of_drives; k++)
        {
            int curr = safe_path_at(paths, G, k, timestep, consider_rotation).location; // current location
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
                std::pair<int, int> goal; // The last goal location
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
                    // assign a new task
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
    // new_agents is a std::list<int>
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

        // ----- NEW: stitch selected agents (keeps their plan, others go to solver) -----
        if (capacity_mode && stitch_mode) {
            plan_stitched_all_applicable_agents();
        }

        // Global multi-agent solver plans agents remaining in new_agents.
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
