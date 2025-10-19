#pragma once
#include "SingleAgentSolver.h"
#include "ReservationTable.h"
#include "States.h"
#include "BasicGraph.h"
#include <boost/heap/fibonacci_heap.hpp>
#include <unordered_set>
#include <vector>
#include <limits>
#include <cstdint>
#include <string>

// Capacity-aware SIPP: serves a tiny batch (<= capacity) of goals in one single-agent run.
// - Keeps SIPP-style safe intervals.
// - Tracks served goals via a bitmask in the node state.
// - Uses admissible multi-goal heuristic (min-to-any + MST over remaining).
class CapSIPP : public SingleAgentSolver {
public:
    int capacity = 3;          // max goals to serve in a single run
    int endpoint_min_sep = 0;  // optional bounded hold (handled via RT intervals if you add it there)

    CapSIPP() = default;
    std::string getName() const override { return "CAPSIPP"; }
    Path run(const BasicGraph& G,
             const State& start,
             const std::vector<std::pair<int,int>>& goal_locations,
             ReservationTable& rt) override;

private:
    struct Node {
        // Comparator must be declared before heap handle types.
        struct NodeCompare {
            bool operator()(const Node* a, const Node* b) const {
                const double fa = a->g_val + a->h_val;
                const double fb = b->g_val + b->h_val;
                if (fa != fb) return fa > fb;                  // min-heap by f
                if (a->conflicts != b->conflicts) return a->conflicts > b->conflicts;
                return a->state.timestep > b->state.timestep;  // tie-breaker
            }
        };

        State state;                // location, timestep, orientation
        uint32_t mask = 0;          // which batch goals are already served (bitmask)
        double g_val = 0, h_val = 0;
        int conflicts = 0;
        Node* parent = nullptr;

        Interval interval = Interval(0, INTERVAL_MAX, 0);

        // Handles for boost fib-heap (use fully-qualified type)
        boost::heap::fibonacci_heap<Node*, boost::heap::compare<NodeCompare>>::handle_type open_handle;
        boost::heap::fibonacci_heap<Node*, boost::heap::compare<NodeCompare>>::handle_type focal_handle;
        bool in_openlist = false;

        struct Hasher {
            size_t operator()(const Node* n) const {
                size_t h = 1469598103934665603ull;
                auto mix = [&](uint64_t v){ h ^= v; h *= 1099511628211ull; };
                mix((uint64_t)n->state.location);
                mix((uint64_t)(n->state.orientation + 5)); // keep -1 safe
                mix((uint64_t)n->state.timestep);
                mix((uint64_t)n->mask);
                return h;
            }
        };
        struct Eq {
            bool operator()(const Node* a, const Node* b) const {
                return a->state.location   == b->state.location &&
                       a->state.orientation == b->state.orientation &&
                       a->state.timestep    == b->state.timestep &&
                       a->mask              == b->mask;
            }
        };

        inline double getFVal() const { return g_val + h_val; }
    };

    // OPEN and FOCAL heaps (same pattern as SIPP/StateTimeAStar)
    typedef boost::heap::fibonacci_heap<Node*, boost::heap::compare<Node::NodeCompare>> FibHeap;
    FibHeap open_list;
    FibHeap focal_list;
    std::unordered_set<Node*, Node::Hasher, Node::Eq> allNodes;

    double min_f_val = 0;
    double focal_bound = 0; // = suboptimal_bound * min_f_val

    // narrowed goal batch for this run (<= capacity)
    std::vector<std::pair<int,int>> batch; // (location, release_time)

    // helpers
    void   reset();
    void   release();
    Path   buildPath(const Node* goal) const;

    void   make_batch(const BasicGraph& G, int from, const std::vector<std::pair<int,int>>& goals);

    // heuristic = min(from -> any remaining) + MST over remaining
    double h_lower_bound(const BasicGraph& G, int from, uint32_t mask) const;
    double mst_lb(const BasicGraph& G, const std::vector<int>& rem) const;

    void   push_or_update(Node* succ);

    // flip mask if standing on any released goal now
    bool   try_serve_goals(Node*& curr);

    // if hold_endpoints is enabled, a conservative earliest holding time hint (optional)
    int    earliest_holding_time(const ReservationTable& rt) const;
};
