#pragma once
#include "BasicSystem.h"
#include "KivaGraph.h"
#include "MAPFSolver.h"
#include <vector>
#include <utility>
#include <unordered_set>

class KivaSystem : public BasicSystem {
public:
    KivaSystem(const KivaGrid& G, MAPFSolver& solver);
    ~KivaSystem();
	bool count_batch_sum = false;                 // when true: count once per micro-batch
    std::vector<int> batch_goals_done;           // how many goals finished in current batch (per agent)
    std::vector<int> batch_goals_target;    
    // Capacity knobs (driver sets these)
    bool capacity_mode = false;      // enable capacity-aware batching
    int  agent_capacity = 2;         // max goals per agent per plan
    int  agent_endpoint_min_sep = 2; // optional bounded endpoint separation (set in RT)

    // lifecycle
    void initialize()  ;
    void simulate(int simulation_time)  ;

protected:
    const KivaGrid& G;

    // start/goal initialization + per-cycle refresh
    void initialize_start_locations();
    void initialize_goal_locations();
    void update_goal_locations();

    // capacity batching helper (build ≤ agent_capacity endpoints near curr_loc)
    void build_capacity_batch_for_agent(int k, int curr_loc);

    // utils
    int manhattan(int a, int b) const;

    // Only used when hold_endpoints == true
    std::unordered_set<int> held_endpoints;
};
