#pragma once
#include "BasicGraph.h"
#include "States.h"
#include "PriorityGraph.h"
#include "PBS.h"
#include "WHCAStar.h"
#include "ECBS.h"
#include "LRAStar.h"

#include <list>
#include <tuple>
#include <vector>
#include <unordered_map>
#include <string>
#include <climits>

class BasicSystem
{
public:
    // params for MAPF algorithms
    MAPFSolver& solver;
    bool hold_endpoints = false;
    bool useDummyPaths = false;
    int time_limit = 0;
    int travel_time_window = 0;
    int screen = 0;
    bool log = false;
    int num_of_drives = 0;
    int seed = 0;
    int simulation_window = 1;
    int planning_window = INT_MAX / 2;
    int simulation_time = 0;

    // params for drive model
    bool consider_rotation = false;
    int k_robust = 0;

    BasicSystem(const BasicGraph& G, MAPFSolver& solver);
    ~BasicSystem();

    // I/O
    std::string outfile;
    void save_results();
    double saving_time = 0; // seconds
    int num_of_tasks;       // number of finished tasks

    std::list<int> new_agents; // used for replanning a subgroup of agents

    // used for MAPF instance
    std::vector<State> starts;
    std::vector< std::vector<std::pair<int, int>> > goal_locations;
    int timestep = 0;

    // record movements of drives
    std::vector<Path> paths;
    std::vector<std::list<std::pair<int, int>>> finished_tasks; // location + finish time

    bool congested() const;
    bool check_collisions(const std::vector<Path>& input_paths) const;

    // update
    void update_start_locations();

    // default args here (header) so 1-arg calls compile everywhere
    void update_paths(const std::vector<Path*>& MAPF_paths, int max_timestep = INT_MAX);
    void update_paths(const std::vector<Path>&  MAPF_paths, int max_timestep = INT_MAX);

    // public API only in std:: flavor — implementation can handle boost internally
    void update_travel_times(std::unordered_map<int, double>& travel_times);

    void update_initial_paths(std::vector<Path>& initial_paths) const;
    void update_initial_constraints(std::list< std::tuple<int, int, int> >& initial_constraints) const;

    void add_partial_priorities(const std::vector<Path>& initial_paths, PriorityGraph& initial_priorities) const;
    std::list<std::tuple<int, int, int>> move(); // return finished tasks
    void solve();
    void initialize_solvers();
    bool load_records();
    bool load_locations();

protected:
    bool solve_by_WHCA(std::vector<Path>& planned_paths,
                       const std::vector<State>& new_starts,
                       const std::vector< std::vector<std::pair<int, int>> >& new_goal_locations);
    bool LRA_called = false;

private:
    const BasicGraph& G;
};
