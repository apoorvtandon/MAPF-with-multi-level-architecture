#include "KivaSystem.h"
#include "WHCAStar.h"
#include "ECBS.h"
#include "LRAStar.h"
#include "PBS.h"

#include <iostream>
#include <algorithm>
#include <cassert>
#include <climits>
#include <unordered_map>

using std::cout;
using std::endl;
using std::pair;
using std::make_pair;

// ------------------------- ctor / dtor -------------------------

KivaSystem::KivaSystem(const KivaGrid& g, MAPFSolver& solver)
    : BasicSystem(g, solver), G(g)
{
    // defaults (driver can overwrite via CLI)
    capacity_mode = false;   // safe baseline off; turn on via --capacity_mode=1
    agent_capacity = 2;      // default batch size when capacity_mode is on
    agent_endpoint_min_sep = 0;
}

KivaSystem::~KivaSystem() = default;

// ------------------------- small utils -------------------------

int KivaSystem::manhattan(int a, int b) const
{
    return G.get_Manhattan_distance(a, b);
}

// Build a micro-batch (<= agent_capacity) of nearest endpoints to curr_loc
void KivaSystem::build_capacity_batch_for_agent(int k, int curr_loc)
{
    goal_locations[k].clear();

    std::vector<int> cand;
    cand.reserve(G.endpoints.size());
    for (int ep : G.endpoints) {
        if (ep == curr_loc) continue;
        if (ep < 0 || ep >= (int)G.types.size()) continue;
        if (G.types[ep] == "Magic") continue;
        cand.push_back(ep);
    }
    if (cand.empty()) return;

    std::stable_sort(cand.begin(), cand.end(), [&](int a, int b){
        auto ita = G.heuristics.find(a);
        auto itb = G.heuristics.find(b);
        const int da = (ita==G.heuristics.end()? INT_MAX : ita->second[curr_loc]);
        const int db = (itb==G.heuristics.end()? INT_MAX : itb->second[curr_loc]);
        if (da != db) return da < db;
        return a < b;
    });

    const int take = std::min(agent_capacity, (int)cand.size());
    for (int i = 0; i < take; ++i)
        goal_locations[k].emplace_back(cand[i], 0);

    // Initialize batch counters when we assign a new batch
    if ((int)batch_goals_done.size() != num_of_drives) {
        batch_goals_done.assign(num_of_drives, 0);
        batch_goals_target.assign(num_of_drives, 0);
    }
    batch_goals_done[k]   = 0;
    batch_goals_target[k] = (int)goal_locations[k].size();

    if (screen >= 1) {
        std::cout << "[cap] t=" << timestep << " agent " << k
                  << " batch=" << goal_locations[k].size() << std::endl;
    }
}

// ------------------------- initialize -------------------------

void KivaSystem::initialize()
{
    initialize_solvers();

    starts.resize(num_of_drives);
    goal_locations.resize(num_of_drives);
    paths.resize(num_of_drives);
    finished_tasks.resize(num_of_drives);
    held_endpoints.clear();

    // initialize batch counters (always sized; may be unused if capacity_mode=false)
    batch_goals_done.assign(num_of_drives, 0);
    batch_goals_target.assign(num_of_drives, 0);
    count_batch_sum = capacity_mode; // enable batch-sum counting only in capacity mode

    bool succ = load_records(); // continue simulating from records
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
}

void KivaSystem::initialize_start_locations()
{
    // start at agent_home_locations with optional rotation
    for (int k = 0; k < num_of_drives; k++)
    {
        int orientation = -1;
        if (consider_rotation) orientation = rand() % 4;

        starts[k] = State(G.agent_home_locations[k], 0, orientation);
        paths[k].emplace_back(starts[k]);
        finished_tasks[k].emplace_back(G.agent_home_locations[k], 0);
    }
}

void KivaSystem::initialize_goal_locations()
{
    if (hold_endpoints || useDummyPaths) return;

    if (!capacity_mode) {
        // single-goal baseline
        for (int k = 0; k < num_of_drives; k++) {
            int goal = G.endpoints[rand() % (int)G.endpoints.size()];
            goal_locations[k].emplace_back(goal, 0);
        }
        return;
    }

    // capacity mode: build batch from starts (and init counters)
    for (int k = 0; k < num_of_drives; k++) {
        const int curr_loc = starts[k].location;
        build_capacity_batch_for_agent(k, curr_loc);
    }
}

// ------------------------- goal refresh each cycle -------------------------

void KivaSystem::update_goal_locations()
{
    // always clear; we’ll explicitly push who should replan below
    new_agents.clear();

    // 1) Hold endpoints mode (legacy)
    if (hold_endpoints)
    {
        std::unordered_map<int, int> held_locations; // <location, agent id>
        for (int k = 0; k < num_of_drives; k++)
        {
            int curr = paths[k].empty() ? starts[k].location
                                        : paths[k][std::min(timestep, (int)paths[k].size()-1)].location;

            if (goal_locations[k].empty())
            {
                int next = G.endpoints[rand() % (int)G.endpoints.size()];
                while (next == curr || held_endpoints.find(next) != held_endpoints.end())
                    next = G.endpoints[rand() % (int)G.endpoints.size()];
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
                    cout << "Agent " << removed_agent << " has to wait for agent "
                         << agent << " because of location " << loc << endl;
                    held_locations[loc] = agent;
                    agent = removed_agent;
                    loc = paths[agent][timestep].location;
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
                cout << "Agent " << agent << " has to wait for agent "
                     << held_locations[goal_locations[k].back().first]
                     << " because of location " << goal_locations[k].back().first << endl;
                auto it = held_locations.find(loc);
                while (it != held_locations.end())
                {
                    int removed_agent = it->second;
                    if (goal_locations[removed_agent].back().first != loc)
                        cout << "BUG" << endl;
                    new_agents.remove(removed_agent);
                    cout << "Agent " << removed_agent << " has to wait for agent "
                         << agent << " because of location " << loc << endl;
                    held_locations[loc] = agent;
                    agent = removed_agent;
                    loc = paths[agent][timestep].location;
                    it = held_locations.find(loc);
                }
                held_locations[loc] = agent;
            }
        }
        return;
    }

    // 2) Dummy paths mode (legacy)
    if (useDummyPaths)
    {
        for (int k = 0; k < num_of_drives; k++)
        {
            int curr = paths[k].empty() ? starts[k].location
                                        : paths[k][std::min(timestep, (int)paths[k].size()-1)].location;
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
        return;
    }

    // 3) Capacity mode — rebuild batch for each agent and force replan
    if (capacity_mode)
    {
        // Make sure counters are sized and counting is enabled
        if ((int)batch_goals_done.size() != num_of_drives) {
            batch_goals_done.assign(num_of_drives, 0);
            batch_goals_target.assign(num_of_drives, 0);
        }
        count_batch_sum = true;

        for (int k = 0; k < num_of_drives; k++)
        {
            int curr_loc = paths[k].empty() ? starts[k].location
                                            : paths[k][std::min(timestep, (int)paths[k].size()-1)].location;

            build_capacity_batch_for_agent(k, curr_loc);

            new_agents.emplace_back(k); // force replan
        }
        return;
    }

    // 4) Original rolling-goal logic (single-goal), replan everyone
    for (int k = 0; k < num_of_drives; k++)
    {
        int curr = paths[k].empty() ? starts[k].location
                                    : paths[k][std::min(timestep, (int)paths[k].size()-1)].location;

        pair<int, int> goal;
        if (goal_locations[k].empty())
            goal = make_pair(curr, 0);
        else
            goal = goal_locations[k].back();

        // If agent may finish within next window, chain another endpoint
        double min_timesteps = G.get_Manhattan_distance(goal.first, curr);
        while (min_timesteps <= simulation_window)
        {
            pair<int, int> next;
            if (G.types[goal.first] == "Endpoint")
            {
                do {
                    next = make_pair(G.endpoints[rand() % (int)G.endpoints.size()], 0);
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

        new_agents.emplace_back(k); // replan this agent
    }
}

// ------------------------- simulate -------------------------

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

        // Safety net: replan all agents every cycle
        new_agents.clear();
        for (int k = 0; k < num_of_drives; k++) new_agents.emplace_back(k);

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
