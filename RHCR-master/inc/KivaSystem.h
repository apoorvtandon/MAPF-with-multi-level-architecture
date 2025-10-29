#pragma once

#include "BasicSystem.h"
#include "KivaGraph.h"
#include "ReservationTable.h"

#include <unordered_set>
#include <vector>
#include <deque>
#include <string>

class KivaSystem : public BasicSystem
{
public:
    KivaSystem(const KivaGrid& G, MAPFSolver& solver);
    ~KivaSystem();

    void simulate(int simulation_time);

    // ====== Capacity & testing controls ======
    void setCapacityMode(bool on)                 { capacity_mode = on; }
    void setAgentCapacity(int c)                  { default_agent_capacity = (c > 0 ? c : 1); per_agent_capacity.clear(); }
    void setAgentCapacities(const std::vector<int>& caps) { per_agent_capacity = caps; }
    void setGivenGoals(const std::vector<std::vector<int>>& gg) { given_goals = gg; }
    void setRandomizeSequences(bool on)           { randomize_sequences = on; }
    void setRngSeed(unsigned s)                   { rng_seed = s; }
    void setSafetyMode(bool on)                   { safety_mode = on; }
    void setAutoRefill(bool on)                   { auto_refill = on; }
    void setAvoidDuplicateGoals(bool on)          { avoid_dup_goals = on; }
    void setCapacityDebug(bool on)                { capacity_debug = on; }

    // ====== Stitching controls ======
    void setStitchMode(bool on)                   { stitch_mode = on; }
    void setStitchAgent(int k)                    { stitch_target = k; }   // -1 = all
    void setStitchUseSIPP(bool on)                { stitch_use_sipp = on; }
    enum class StitchOrder { ByIndex=0, ShortestRemaining=1, ClosestNextGoal=2 };
    void setStitchBatchOrder(StitchOrder ord)     { stitch_batch_order = ord; }
    void setStitchCropToWindow(bool on)           { stitch_crop_horizon = on; }

    // ====== Restitch policy & depth ======
    void setRestitchOnChange(bool on)             { restitch_on_change = on; }
    void setStitchDepth(int d)                    { stitch_depth = (d > 0 ? d : 1); }

    // ====== Metrics controls ======
    void setMetricsCSV(const std::string& path)   { metrics_csv_path = path; metrics_csv_enabled = !path.empty(); }
    void setMetricsVerbose(bool on)               { metrics_verbose = on; } // print per-tick stitch summary

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
    std::vector<std::vector<int>> given_goals;
    std::vector<std::deque<Goal>> bundle; // active, capped by capacity
    std::vector<std::deque<Goal>> rest;   // backlog

    bool capacity_mode         = true;
    int  default_agent_capacity= 3;
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

    void reorder_bundle_by_dvs(int k);
    bool dss_debug_print = true; // set false to silence

    int  generate_endpoint_for(int k, int avoid_v) const;
    void maybe_autorefill_rest(int k);

    std::unordered_set<int> collect_claimed_active_endpoints(int except_agent = -1) const;

    void debug_print_capacity_state() const;

    // ===== stitching =====
    void plan_stitched_all_applicable_agents();     // kept for completeness
    void plan_stitched_for_agent(int k);
    void suppress_replan_for(int k);
    bool stitch_mode     = true;
    int  stitch_target   = -1;    // -1 = all
    bool stitch_use_sipp = true;

    // Batch stitching with RT injection
    void plan_stitched_batch();
    std::vector<int> compute_batch_order() const;

    // RT helpers
    void build_rt_from_teammates(int current_agent, ReservationTable& rt) const;
    void build_rt_from_teammates_with_crop(int current_agent, int horizon_t, ReservationTable& rt) const;

    // SIPP wait-retry helper (jam mitigation)
    bool try_sipp_with_initial_waits(int k, int start_v, int start_t,
                                     const std::vector<int>& goals,
                                     std::vector<State>& out_suffix);

    // Dirty tracking & limits
    std::vector<bool> bundle_dirty;
    bool restitch_on_change = true;
    int  stitch_depth       = 1;

    // batch options
    StitchOrder stitch_batch_order = StitchOrder::ByIndex;
    bool stitch_crop_horizon = true;

    // ===== Metrics =====
    long long m_stitch_attempts_total   = 0;
    long long m_stitch_agents_ticks     = 0;
    long long m_sipp_success_total      = 0;
    long long m_sipp_fallback_total     = 0;
    long long m_sipp_fail_total         = 0;
    long long m_skipped_clean_total     = 0;
    long long m_restitches_total        = 0;

    int m_tick_stitched_agents          = 0;
    int m_tick_sipp_success             = 0;
    int m_tick_sipp_fallback            = 0;
    int m_tick_skipped_clean            = 0;

    bool metrics_csv_enabled = false;
    bool metrics_verbose     = true;
    std::string metrics_csv_path;

    void metrics_begin_tick();
    void metrics_after_stitch(bool used_sipp, bool sipp_ok, bool fell_back, bool skipped_clean);
    void metrics_end_tick_and_maybe_log();
    void metrics_print_summary() const;
    // Per-agent cooldown ticks remaining
static const int REPLAN_COOLDOWN_TICKS = 3;
std::vector<int> replan_cooldown; // size = num_of_drives


private:
    // ===== original data we rely on =====
    const KivaGrid& G;
    std::unordered_set<int> held_endpoints;
};
