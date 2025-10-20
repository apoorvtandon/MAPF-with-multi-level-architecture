#pragma once

#include "BasicSystem.h"
#include "KivaGraph.h"

#include <unordered_set>
#include <vector>
#include <deque>

class KivaSystem : public BasicSystem
{
public:
    KivaSystem(const KivaGrid& G, MAPFSolver& solver);
    ~KivaSystem();

    void simulate(int simulation_time);

    // ====== Capacity & testing controls ======
    void setCapacityMode(bool on)                 { capacity_mode = on; }
    void setAgentCapacity(int c)                  { default_agent_capacity = (c > 0 ? c : 1); per_agent_capacity.clear(); }
    void setAgentCapacities(const std::vector<int>& caps) { per_agent_capacity = caps; } // per-agent override
    void setGivenGoals(const std::vector<std::vector<int>>& gg) { given_goals = gg; }
    void setRandomizeSequences(bool on)           { randomize_sequences = on; }
    void setRngSeed(unsigned s)                   { rng_seed = s; }
    void setSafetyMode(bool on)                   { safety_mode = on; }   // DVS ordering only (visit ordering)
    void setAutoRefill(bool on)                   { auto_refill = on; }   // keeps rest[] stocked
    void setAvoidDuplicateGoals(bool on)          { avoid_dup_goals = on; } // avoid active duplicates
    void setCapacityDebug(bool on)                { capacity_debug = on; }  // print bundle/rest & replans

    // ====== Stitching controls ======
    void setStitchMode(bool on)                   { stitch_mode = on; }
    void setStitchAgent(int k)                    { stitch_target = k; }   // -1 = all, default 0 = only agent 0

    // Choose the engine used for stitching:
    // false = greedy stepper (fallback), true = SIPP-based stitching
    void setStitchUseSIPP(bool on)                { stitch_use_sipp = on; }

private:
    // ===== lifecycle =====
    void initialize();
    void initialize_start_locations();
    void initialize_goal_locations();
    void update_goal_locations();

    // ===== baseline helper =====
    void ensure_goal_exists(int k, int curr);

    // ===== capacity scaffolding =====
    using Goal = std::pair<int,int>; // (endpoint, release_time)
    std::vector<std::vector<int>> given_goals; // optional per-agent endpoints
    std::vector<std::deque<Goal>> bundle;      // active goals (size <= capacity)
    std::vector<std::deque<Goal>> rest;        // backlog

    bool capacity_mode         = true; // OFF by default (legacy)
    int  default_agent_capacity= 3;     // default capacity when no per-agent override
    std::vector<int> per_agent_capacity;
    bool randomize_sequences   = true;
    unsigned rng_seed          = 1;
    bool safety_mode           = true;
    bool auto_refill           = true;
    bool avoid_dup_goals       = true;
    bool capacity_debug        = false;

    // helpers for capacity behavior
    int  cap_of(int k) const;
    void bundle_configure(int num_agents, int capacity, bool randomize, unsigned seed);
    void bundle_initialize_from_given(const std::vector<std::vector<int>>& gg);
    bool bundle_on_goal_reached(int k);
    bool bundle_maybe_top_up(int k);
    void bundle_mirror_to_engine();
    void bundle_assert_capacity_ok(int k) const;

    // Reorder the active bundle by DVS (distance-only). Gated by safety_mode.
    void reorder_bundle_by_dvs(int k);

    // Auto-refill helpers
    int  generate_endpoint_for(int k, int avoid_v) const;
    void maybe_autorefill_rest(int k);

    // Global-claim helpers (avoid active duplicates)
    std::unordered_set<int> collect_claimed_active_endpoints(int except_agent = -1) const;

    // Debug printing
    void debug_print_capacity_state() const;

    // ===== stitching =====
    void plan_stitched_all_applicable_agents(); // based on stitch_target
    void plan_stitched_for_agent(int k);        // choose engine (SIPP or fallback)
    void suppress_replan_for(int k);            // remove k from new_agents so solver doesn’t overwrite
    bool stitch_mode     = true;
    int  stitch_target   = 0;    // default: only agent 0; set -1 to stitch all
    bool stitch_use_sipp = true; // NEW: use SIPP to stitch (default ON)

private:
    // ===== original data we rely on =====
    const KivaGrid& G;
    std::unordered_set<int> held_endpoints;
};
