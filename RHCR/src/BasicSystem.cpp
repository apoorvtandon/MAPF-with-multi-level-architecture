#include "BasicSystem.h"

#include <boost/tokenizer.hpp>
#include <boost/unordered_map.hpp>

#include <algorithm>
#include <fstream>
#include <iostream>

using std::cout;
using std::endl;
using std::list;
using std::tuple;
using std::vector;
using boost::tokenizer;
using boost::char_separator;

// ---------- internal helper (templated) ----------
// Works with both std::unordered_map<int,double> and boost::unordered_map<int,double>
namespace {

template <typename UMap>
void update_travel_times_impl(const std::vector<Path>& paths,
                              int timestep,
                              int travel_time_window,
                              UMap& travel_times)
{
    if (travel_time_window <= 0)
        return;

    travel_times.clear();
    UMap count; // use same map type for counts

    const int t_min = std::max(0, timestep - travel_time_window);
    if (t_min >= timestep)
        return;

    for (const auto& path : paths)
    {
        int t = timestep;
        while (t >= t_min)
        {
            const int loc = path[t].location;
            const int dir = path[t].orientation;

            int wait = 0;
            while (t > wait &&
                   path[t - 1 - wait].location    == loc &&
                   path[t - 1 - wait].orientation == dir)
            {
                wait++;
            }

            auto it = travel_times.find(loc);
            if (it == travel_times.end())
            {
                travel_times[loc] = wait;
                count[loc] = 1;
            }
            else
            {
                it->second += wait;
                count[loc] += 1;
            }

            t = t - 1 - wait;
        }
    }

    for (const auto& kv : count)
    {
        if (kv.second > 1)
            travel_times[kv.first] /= kv.second;
    }
}

} // namespace

// ---------- ctor / dtor ----------

BasicSystem::BasicSystem(const BasicGraph& G, MAPFSolver& solver)
    : G(G), solver(solver), num_of_tasks(0) {}

BasicSystem::~BasicSystem() {}

// ---------- load (agents+goals) ----------

bool BasicSystem::load_locations()
{
    std::string fname = G.map_name + "_rotation=" + std::to_string(consider_rotation) +
                        "_" + std::to_string(num_of_drives) + ".agents";
    std::ifstream myfile(fname.c_str());
    if (!myfile.is_open())
        return false;

    std::string line;
    getline(myfile, line);
    char_separator<char> sep(",");

    if (atoi(line.c_str()) != num_of_drives)
    {
        cout << "The agent file does not match the settings." << endl;
        exit(-1);
    }
    for (int k = 0; k < num_of_drives; k++)
    {
        getline(myfile, line);
        tokenizer<char_separator<char>> tok(line, sep);
        auto beg = tok.begin();
        // starts
        int start_loc = atoi((*beg).c_str());
        beg++;
        int start_orient = atoi((*beg).c_str());
        beg++;
        starts[k] = State(start_loc, 0, start_orient);
        paths[k].emplace_back(starts[k]);
        finished_tasks[k].push_back(std::make_pair(start_loc, 0));
        // goals
        int goal = atoi((*beg).c_str());
        goal_locations[k].emplace_back(goal, 0);
    }
    myfile.close();
    return true;
}

// ---------- small updates ----------

void BasicSystem::update_start_locations()
{
    for (int k = 0; k < num_of_drives; k++)
    {
        starts[k] = State(paths[k][timestep].location, 0, paths[k][timestep].orientation);
    }
}

void BasicSystem::update_paths(const std::vector<Path*>& MAPF_paths, int max_timestep)
{
    for (int k = 0; k < num_of_drives; k++)
    {
        int length = std::min(max_timestep, (int)MAPF_paths[k]->size());
        paths[k].resize(timestep + length);
        for (int t = 0; t < length; t++)
        {
            if (MAPF_paths[k]->at(t).location < 0)
                paths[k][timestep + t] = State(-starts[k].location - 1, timestep + t, starts[k].orientation);
            else
            {
                paths[k][timestep + t] = MAPF_paths[k]->at(t);
                paths[k][timestep + t].timestep = timestep + t;
            }
        }
    }
}

void BasicSystem::update_paths(const std::vector<Path>& MAPF_paths, int max_timestep)
{
    for (int k = 0; k < num_of_drives; k++)
    {
        int length = std::min(max_timestep, (int)MAPF_paths[k].size());
        paths[k].resize(timestep + length);
        for (int t = 0; t < length; t++)
        {
            paths[k][timestep + t] = MAPF_paths[k][t];
            paths[k][timestep + t].timestep = timestep + t;
        }
    }
}

void BasicSystem::update_initial_paths(vector<Path>& initial_paths) const
{
    initial_paths.clear();
    initial_paths.resize(num_of_drives);
    for (int k = 0; k < num_of_drives; k++)
    {
        // check whether the path traverse every goal location
        int i = (int)goal_locations[k].size() - 1;
        int j = (int)paths[k].size() - 1;
        while (i >= 0 && j >= 0)
        {
            while (j >= 0 && paths[k][j].location != goal_locations[k][i].first &&
                   paths[k][j].timestep >= goal_locations[k][i].second)
                j--;
            i--;
        }
        if (j < 0)
            continue;

        if ((int)paths[k].size() <= timestep + planning_window)
            continue;

        initial_paths[k].resize(paths[k].size() - timestep);
        for (int t = 0; t < (int)initial_paths[k].size(); t++)
        {
            initial_paths[k][t] = paths[k][timestep + t];
            initial_paths[k][t].timestep = t;
        }
    }
}

void BasicSystem::update_initial_constraints(std::list< std::tuple<int, int, int> >& initial_constraints) const
{
    initial_constraints.clear();
    for (int k = 0; k < num_of_drives; k++)
    {
        int prev_location = -1;
        for (int t = timestep; t > std::max(0, timestep - k_robust); t--)
        {
            int curr_location = paths[k][t].location;
            if (curr_location < 0)
                continue;
            else if (curr_location != prev_location)
            {
                initial_constraints.emplace_back(k, curr_location, t + k_robust + 1 - timestep);
                prev_location = curr_location;
            }
        }
    }
}

// ---------- checks ----------

bool BasicSystem::check_collisions(const vector<Path>& input_paths) const
{
    for (int a1 = 0; a1 < (int)input_paths.size(); a1++)
    {
        for (int a2 = a1 + 1; a2 < (int)input_paths.size(); a2++)
        {
            // TODO: add k-robust
            size_t min_path_length = input_paths[a1].size() < input_paths[a2].size() ?
                                     input_paths[a1].size() : input_paths[a2].size();
            for (size_t ts = 0; ts < min_path_length; ts++)
            {
                int loc1 = input_paths[a1].at(ts).location;
                int loc2 = input_paths[a2].at(ts).location;
                if (loc1 == loc2)
                    return true;
                else if (ts < min_path_length - 1
                      && loc1 == input_paths[a2].at(ts + 1).location
                      && loc2 == input_paths[a1].at(ts + 1).location)
                    return true;
            }
            if ((hold_endpoints || useDummyPaths) && input_paths[a1].size() != input_paths[a2].size())
            {
                int a1_ = input_paths[a1].size() < input_paths[a2].size() ? a1 : a2;
                int a2_ = input_paths[a1].size() < input_paths[a2].size() ? a2 : a1;
                int loc1 = input_paths[a1_].back().location;
                for (size_t ts = min_path_length; ts < input_paths[a2_].size(); ts++)
                {
                    int loc2 = input_paths[a2_].at(ts).location;
                    if (loc1 == loc2)
                        return true;
                }
            }
        }
    }
    return false;
}

bool BasicSystem::congested() const
{
    if (simulation_window <= 1)
        return false;
    int wait_agents = 0;
    for (const auto& path : paths)
    {
        int t = 0;
        while (t < simulation_window &&
               path[timestep].location == path[timestep + t].location &&
               path[timestep].orientation == path[timestep + t].orientation)
            t++;
        if (t == simulation_window)
            wait_agents++;
    }
    return wait_agents > num_of_drives / 2;  // more than half of drives didn't make progress
}

// ---------- simulate one planning cycle move ----------

list<tuple<int, int, int>> BasicSystem::move()
{
    int start_timestep = timestep;
    int end_timestep = timestep + simulation_window;

    list<tuple<int, int, int>> finished; // <agent_id, location, timestep>

    // Ensure plans are long enough (agents wait if needed)
    for (int t = start_timestep; t <= end_timestep; t++)
    {
        for (int k = 0; k < num_of_drives; k++) {
            while ((int)paths[k].size() <= t)
            {
                State final_state = paths[k].back();
                paths[k].emplace_back(final_state.location, final_state.timestep + 1, final_state.orientation);
            }
        }
    }

    for (int t = start_timestep; t <= end_timestep; t++)
    {
        for (int k = 0; k < num_of_drives; k++)
        {
            State curr = paths[k][t];

            // complete goals if reached
            if ((!hold_endpoints || (int)paths[k].size() == t + 1) &&
                !goal_locations[k].empty() &&
                curr.location == goal_locations[k].front().first &&
                curr.timestep >= goal_locations[k].front().second)
            {
                goal_locations[k].erase(goal_locations[k].begin());
                finished.emplace_back(k, curr.location, t);
                num_of_tasks++; // legacy: count every goal reached
            }

            // validity checks
            if (t > 0)
            {
                State prev = paths[k][t - 1];

                if (curr.location == prev.location)
                {
                    if (G.get_rotate_degree(prev.orientation, curr.orientation) == 2)
                    {
                        cout << "Drive " << k << " rotates 180 degrees from " << prev << " to " << curr << endl;
                        save_results();
                        exit(-1);
                    }
                }
                else if (consider_rotation)
                {
                    if (prev.orientation != curr.orientation)
                    {
                        cout << "Drive " << k << " rotates while moving from " << prev << " to " << curr << endl;
                        save_results();
                        exit(-1);
                    }
                    else if (!G.valid_move(prev.location, prev.orientation) ||
                             prev.location + G.move[prev.orientation] != curr.location)
                    {
                        cout << "Drive " << k << " jump from " << prev << " to " << curr << endl;
                        save_results();
                        exit(-1);
                    }
                }
                else
                {
                    int dir = G.get_direction(prev.location, curr.location);
                    if (dir < 0 || !G.valid_move(prev.location, dir))
                    {
                        cout << "Drive " << k << " jump from " << prev << " to " << curr << endl;
                        save_results();
                        exit(-1);
                    }
                }
            }

            // conflict checks (vertex; edge-swaps handled above)
            if (G.types[curr.location] != "Magic")
            {
                for (int j = k + 1; j < num_of_drives; j++)
                {
                    for (int i = std::max(0, t - k_robust); i <= std::min(t + k_robust, end_timestep); i++)
                    {
                        if ((int)paths[j].size() <= i)
                            break;
                        if (paths[j][i].location == curr.location)
                        {
                            cout << "Drive " << k << " at " << curr << " has a conflict with drive " << j
                                 << " at " << paths[j][i] << endl;
                            save_results(); // write termination reason if needed
                            exit(-1);
                        }
                    }
                }
            }
        }
    }
    return finished;
}

// ---------- priorities helper ----------

void BasicSystem::add_partial_priorities(const vector<Path>& initial_paths, PriorityGraph& initial_priorities) const
{
    list<int> low_priorities;
    list<int> high_priorities;
    for (int k = 0; k < num_of_drives; k++)
    {
        if (initial_paths[k].empty())
            low_priorities.push_back(k);
        else
            high_priorities.push_back(k);
    }

    for (auto low : low_priorities)
    {
        for (auto high : high_priorities)
            initial_priorities.add(low, high);
    }
}

// ---------- saving ----------

void BasicSystem::save_results()
{
    if (screen)
        std::cout << "*** Saving " << seed << " ***" << std::endl;
    clock_t t = std::clock();
    std::ofstream output;

    // settings
    output.open(outfile + "/config.txt", std::ios::out);
    output << "map: " << G.map_name << std::endl
           << "#drives: " << num_of_drives << std::endl
           << "seed: " << seed << std::endl
           << "solver: " << solver.get_name() << std::endl
           << "time_limit: " << time_limit << std::endl
           << "simulation_window: " << simulation_window << std::endl
           << "planning_window: " << planning_window << std::endl
           << "simulation_time: " << simulation_time << std::endl
           << "robust: " << k_robust << std::endl
           << "rotate: " << consider_rotation << std::endl
           << "use_dummy_paths: " << useDummyPaths << std::endl
           << "hold_endpoints: " << hold_endpoints << std::endl;
    output.close();

    // tasks
    output.open(outfile + "\\tasks.txt", std::ios::out);
    output << num_of_drives << std::endl;
    for (int k = 0; k < num_of_drives; k++)
    {
        int prev = finished_tasks[k].front().first;
        for (auto task : finished_tasks[k])
        {
            output << task.first << "," << task.second << ",";
            if (task.second != 0)
                output << G.heuristics.at(task.first)[prev];
            output << ";";
            prev = task.first;
        }
        for (auto goal : goal_locations[k]) // unfinished tasks
        {
            output << goal.first << ",-1,;";
        }
        output << std::endl;
    }
    output.close();

    // paths
    output.open(outfile + "\\paths.txt", std::ios::out);
    output << num_of_drives << std::endl;
    for (int k = 0; k < num_of_drives; k++)
    {
        for (auto p : paths[k])
        {
            if (p.timestep <= timestep)
                output << p << ";";
        }
        output << std::endl;
    }
    output.close();
    saving_time = (std::clock() - t) / CLOCKS_PER_SEC;
    if (screen)
        std::cout << "Done! (" << saving_time << " s)" << std::endl;
}

// ---------- public std:: flavor (delegates to helper) ----------

void BasicSystem::update_travel_times(std::unordered_map<int, double>& travel_times)
{
    update_travel_times_impl(paths, timestep, travel_time_window, travel_times);
}

// ---------- solve cycle ----------

void BasicSystem::solve()
{
    LRA_called = false;
    LRAStar lra(G, solver.path_planner);
    lra.simulation_window = simulation_window;
    lra.k_robust = k_robust;
    solver.clear();

    if (solver.get_name() == "LRA")
    {
        // directly update into solver.travel_times (Boost map) using helper
        update_travel_times_impl(paths, timestep, travel_time_window, solver.travel_times);

        bool sol = solver.run(starts, goal_locations, time_limit);
        (void)sol; // unused variable guard if you don't print it here
        update_paths(solver.solution);
    }
    else if (solver.get_name() == "WHCA")
    {
        update_initial_constraints(solver.initial_constraints);

        bool sol = solver.run(starts, goal_locations, time_limit);
        if (sol)
        {
            update_paths(solver.solution);
        }
        else
        {
            lra.resolve_conflicts(solver.solution);
            update_paths(lra.solution);
        }
    }
    else // PBS or ECBS
    {
        update_initial_constraints(solver.initial_constraints);

        // solve
        if (hold_endpoints || useDummyPaths)
        {
            vector<State> new_starts;
            vector<vector<std::pair<int, int>>> new_goal_locations;
            for (int i : new_agents)
            {
                new_starts.emplace_back(starts[i]);
                new_goal_locations.emplace_back(goal_locations[i]);
            }
            vector<Path> planned_paths(num_of_drives);
            solver.initial_rt.clear();
            auto p = new_agents.begin();
            for (int i = 0; i < num_of_drives; i++)
            {
                planned_paths[i].resize(paths[i].size() - timestep);
                for (int t = 0; t < (int)planned_paths[i].size(); t++)
                {
                    planned_paths[i][t] = paths[i][timestep + t];
                    planned_paths[i][t].timestep = t;
                }
                if (p == new_agents.end() || *p != i)
                {
                    solver.initial_rt.insertPath2CT(planned_paths[i]);
                }
                else
                    ++p;
            }
            if (!new_agents.empty())
            {
                bool sol;
                if (timestep == 0)
                    sol = solver.run(new_starts, new_goal_locations, 10 * time_limit);
                else
                    sol = solver.run(new_starts, new_goal_locations, time_limit);

                if (sol)
                {
                    auto pt = solver.solution.begin();
                    for (int i : new_agents)
                    {
                        planned_paths[i] = *pt;
                        ++pt;
                    }
                    if (check_collisions(planned_paths))
                    {
                        cout << "COLLISIONS!" << endl;
                        exit(-1);
                    }
                }
                else
                {
                    bool ok = solve_by_WHCA(planned_paths, new_starts, new_goal_locations);
                    (void)ok; // assert ok if you prefer
                }
            }
            update_paths(planned_paths);
        }
        else
        {
            bool sol = solver.run(starts, goal_locations, time_limit);
            if (sol)
            {
                if (log)
                    solver.save_constraints_in_goal_node(outfile + "/goal_nodes/" + std::to_string(timestep) + ".gv");
                update_paths(solver.solution);
            }
            else
            {
                lra.resolve_conflicts(solver.solution);
                update_paths(lra.solution);
            }
        }
        if (log)
            solver.save_search_tree(outfile + "/search_trees/" + std::to_string(timestep) + ".gv");
    }

    solver.save_results(outfile + "/solver.csv",
        std::to_string(timestep) + "," + std::to_string(num_of_drives) + "," + std::to_string(seed));
}

// ---------- WHCA fallback ----------

bool BasicSystem::solve_by_WHCA(vector<Path>& planned_paths,
                                const vector<State>& new_starts,
                                const vector< vector<std::pair<int, int>> >& new_goal_locations)
{
    WHCAStar whca(G, solver.path_planner);
    whca.k_robust = k_robust;
    whca.window = INT_MAX;
    whca.hold_endpoints = hold_endpoints || useDummyPaths;
    whca.screen = screen;
    whca.initial_rt.hold_endpoints = true;
    whca.initial_rt.map_size = G.size();
    whca.initial_rt.k_robust = k_robust;
    whca.initial_rt.window = INT_MAX;
    whca.initial_rt.copy(solver.initial_rt);
    whca.initial_solution.resize(new_starts.size());
    if (whca.hold_endpoints)
    {
        if (timestep == 0)
        {
            for (int i = 0; i < (int)new_starts.size(); i++)
                whca.initial_solution[i].emplace_back(starts[i]); // hold initial location
        }
        else
        {
            whca.initial_solution.clear();
            for (auto agent : new_agents)
                whca.initial_solution.emplace_back(planned_paths[agent]); // hold old paths
        }
    }
    bool sol = false;
    if (timestep == 0)
        sol = whca.run(new_starts, new_goal_locations, 10 * time_limit);
    else
        sol = whca.run(new_starts, new_goal_locations, time_limit);

    whca.save_results(outfile + "/solver.csv",
        std::to_string(timestep) + "," + std::to_string(num_of_drives) + "," + std::to_string(seed));

    if (sol)
    {
        auto pt = whca.solution.begin();
        for (int i : new_agents)
        {
            planned_paths[i] = *pt;
            ++pt;
        }
    }
    whca.clear();
    return sol;
}

// ---------- solver init ----------

void BasicSystem::initialize_solvers()
{
    solver.k_robust = k_robust;
    solver.window = planning_window;
    solver.hold_endpoints = hold_endpoints || useDummyPaths;
    solver.screen = screen;

    solver.initial_rt.hold_endpoints = true;
    solver.initial_rt.map_size = G.size();
    solver.initial_rt.k_robust = k_robust;
    solver.initial_rt.window = INT_MAX;
}

// ---------- resume from files ----------

bool BasicSystem::load_records()
{
    char_separator<char> sep1(";");
    char_separator<char> sep2(",");
    std::string line;

    // load paths
    std::ifstream myfile(outfile + "/paths.txt");
    if (!myfile.is_open())
        return false;

    timestep = INT_MAX;
    getline(myfile, line);
    if (atoi(line.c_str()) != num_of_drives)
    {
        cout << "The path file does not match the settings." << endl;
        exit(-1);
    }
    for (int k = 0; k < num_of_drives; k++)
    {
        getline(myfile, line);
        tokenizer<char_separator<char>> tok1(line, sep1);
        for (auto task : tok1)
        {
            tokenizer<char_separator<char>> tok2(task, sep2);
            auto beg = tok2.begin();
            int loc = atoi((*beg).c_str()); beg++;
            int orientation = atoi((*beg).c_str()); beg++;
            int time = atoi((*beg).c_str());
            paths[k].emplace_back(loc, time, orientation);
        }
        timestep = std::min(timestep, paths[k].back().timestep);
    }
    myfile.close();

    // pick the timestep
    timestep = int((timestep - 1) / simulation_window) * simulation_window;

    // load tasks
    myfile.open(outfile + "/tasks.txt");
    if (!myfile.is_open())
        return false;

    getline(myfile, line);
    if (atoi(line.c_str()) != num_of_drives)
    {
        cout << "The task file does not match the settings." << endl;
        exit(-1);
    }
    for (int k = 0; k < num_of_drives; k++)
    {
        getline(myfile, line);
        tokenizer<char_separator<char>> tok1(line, sep1);
        for (auto task : tok1)
        {
            tokenizer<char_separator<char>> tok2(task, sep2);
            auto beg = tok2.begin();
            int loc = atoi((*beg).c_str()); beg++;
            int time = atoi((*beg).c_str());
            if (time >= 0 && time <= timestep)
            {
                finished_tasks[k].emplace_back(loc, time);
                timestep = std::max(timestep, time);
            }
            else
            {
                goal_locations[k].emplace_back(loc, 0);
            }
        }
    }
    myfile.close();
    return true;
}
