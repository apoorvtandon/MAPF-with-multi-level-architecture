#include "CapSIPP.h"
#include <algorithm>
#include <cassert>
#include <climits>

using std::vector;
using std::pair;
using std::min;
using std::max;

// ---------------- public API ----------------

Path CapSIPP::run(const BasicGraph& G,
                  const State& start,
                  const vector<pair<int,int>>& goal_locations,
                  ReservationTable& rt)
{
    num_expanded = 0;
    num_generated = 0;
    num_of_conf = 0;
    path_cost = 0;
    runtime = 0;
    clock_t t0 = std::clock();

    if (goal_locations.empty())
        return Path();

    // Pick a tiny batch (<= capacity) nearest to the current location.
    make_batch(G, start.location, goal_locations);

    // Quick reachability check from start to at least one batch goal
    bool any_reachable = false;
    for (auto& g : batch) {
        auto it = G.heuristics.find(g.first);
        if (it != G.heuristics.end() && it->second[start.location] < INT_MAX) {
            any_reachable = true; break;
        }
    }
    if (!any_reachable)
        return Path();

    if (rt.isConstrained(start.location, start.location, 0))
        return Path();

    reset();

    // Root node
    Node* root = new Node();
    root->state = start;
    root->mask = 0;
    root->g_val = 0.0;
    root->h_val = h_lower_bound(G, start.location, 0);
    root->conflicts = 0;
    root->interval = rt.getFirstSafeInterval(start.location);
    root->in_openlist = true;

    num_generated++;
    min_f_val = root->getFVal();
    focal_bound = min_f_val * suboptimal_bound;

    root->open_handle  = open_list.push(root);
    root->focal_handle = focal_list.push(root);
    allNodes.insert(root);

    const uint32_t FULL = (batch.size() >= 32) ? 0xFFFFFFFFu : ((1u << (unsigned)batch.size()) - 1u);
    (void)FULL; // silence if not used by compilation mode

    // Main loop
    while (!focal_list.empty())
    {
        Node* curr = focal_list.top(); focal_list.pop();
        open_list.erase(curr->open_handle);
        curr->in_openlist = false;
        num_expanded++;

        // serve goals if standing on any released goal
        if (try_serve_goals(curr)) {
            curr->h_val = h_lower_bound(G, curr->state.location, curr->mask);
            curr->in_openlist = true;
            curr->open_handle  = open_list.push(curr);
            if (curr->getFVal() <= focal_bound)
                curr->focal_handle = focal_list.push(curr);

            // all done?
            if (batch.size() <= 32) {
                const uint32_t FULLM = ((1u << (unsigned)batch.size()) - 1u);
                if (curr->mask == FULLM) {
                    Path p = buildPath(curr);
                    release();
                    runtime = (double)(std::clock() - t0) / CLOCKS_PER_SEC;
                    path_cost = curr->getFVal();
                    num_of_conf = curr->conflicts;
                    return p;
                }
            } else {
                // defensive: if someone sets capacity>32 (not recommended), fall back to checking served count
                bool all_served = true;
                for (int i = 0; i < (int)batch.size(); ++i)
                    if (!(curr->mask & (1u<<i))) { all_served = false; break; }
                if (all_served) {
                    Path p = buildPath(curr);
                    release();
                    runtime = (double)(std::clock() - t0) / CLOCKS_PER_SEC;
                    path_cost = curr->getFVal();
                    num_of_conf = curr->conflicts;
                    return p;
                }
            }
            // continue to update focal/OPEN
            goto maintain_focal;
        }

        // expand motion (SIPP-style)
        for (const auto& ns : G.get_neighbors(curr->state))
        {
            if (rt.isConstrained(curr->state.location, ns.location, ns.timestep))
                continue;

            const double w = G.get_weight(curr->state.location, ns.location);
            if (w >= WEIGHT_MAX - 1) continue;

            // enumerate feasible destination intervals
            for (auto interval : rt.getSafeIntervals(curr->state.location, ns.location, ns.timestep,
                                                     std::get<1>(curr->interval) + 1))
            {
                Node* succ = new Node();
                succ->parent = curr;
                succ->state = ns;
                succ->mask = curr->mask;
                succ->interval = interval;
                succ->g_val = curr->g_val + w;
                succ->h_val = h_lower_bound(G, ns.location, succ->mask);
                succ->conflicts = curr->conflicts;
                if (rt.isConflicting(curr->state.location, ns.location, ns.timestep))
                    succ->conflicts++;

                push_or_update(succ);
            }
        }

maintain_focal:
        if (open_list.empty()) break;

        Node* head = open_list.top();
        if (head->getFVal() > min_f_val) {
            const double new_min = head->getFVal();
            const double new_foc = new_min * suboptimal_bound;
            for (auto* n : open_list) {
                if (n->getFVal() > focal_bound && n->getFVal() <= new_foc) {
                    n->focal_handle = focal_list.push(n);
                }
            }
            min_f_val = new_min;
            focal_bound = new_foc;
        }
    }

    // no path found
    release();
    runtime = (double)(std::clock() - t0) / CLOCKS_PER_SEC;
    return Path();
}

// ---------------- helpers ----------------

void CapSIPP::reset() {
    open_list.clear();
    focal_list.clear();
    allNodes.clear();
    min_f_val = 0;
    focal_bound = 0;
}
void CapSIPP::release() {
    for (auto* n : allNodes) delete n;
    allNodes.clear();
    open_list.clear();
    focal_list.clear();
}

Path CapSIPP::buildPath(const Node* goal) const {
    Path path(goal->state.timestep + 1);
    const Node* curr = goal;
    for (int t = goal->state.timestep; t >= 0; --t) {
        if (curr->state.timestep > t) {
            curr = curr->parent;
            assert(curr && curr->state.timestep <= t);
        }
        path[t] = curr->state;
    }
    return path;
}

void CapSIPP::make_batch(const BasicGraph& G, int from, const vector<pair<int,int>>& goals)
{
    batch.clear();
    if ((int)goals.size() <= capacity) {
        batch = goals;
        return;
    }
    vector<int> idx(goals.size());
    for (int i = 0; i < (int)goals.size(); ++i) idx[i] = i;

    std::stable_sort(idx.begin(), idx.end(), [&](int a, int b){
        const auto& ga = goals[a]; const auto& gb = goals[b];
        auto ita = G.heuristics.find(ga.first);
        auto itb = G.heuristics.find(gb.first);
        const double da = (ita==G.heuristics.end()? (double)INT_MAX : ita->second[from]);
        const double db = (itb==G.heuristics.end()? (double)INT_MAX : itb->second[from]);
        if (da != db) return da < db;
        return ga.second < gb.second;
    });

    for (int i = 0; i < capacity; ++i) batch.push_back(goals[idx[i]]);
}

double CapSIPP::mst_lb(const BasicGraph& G, const vector<int>& rem) const
{
    if (rem.size() <= 1) return 0.0;
    const int n = (int)rem.size();

    vector<double> min_edge(n, std::numeric_limits<double>::infinity());
    vector<char> used(n, 0);
    min_edge[0] = 0.0;
    double total = 0.0;

    auto dist = [&](int i, int j)->double{
        auto it = G.heuristics.find(rem[j]);
        if (it == G.heuristics.end()) return (double)INT_MAX;
        return it->second[rem[i]];
    };

    for (int iters = 0; iters < n; ++iters) {
        int v = -1;
        for (int i = 0; i < n; ++i) if (!used[i] && (v==-1 || min_edge[i] < min_edge[v])) v = i;
        used[v] = 1; total += min_edge[v];
        for (int u = 0; u < n; ++u) if (!used[u]) {
            const double w = dist(v,u);
            if (w < min_edge[u]) min_edge[u] = w;
        }
    }
    return total;
}

double CapSIPP::h_lower_bound(const BasicGraph& G, int from, uint32_t mask) const
{
    vector<int> rem;
    rem.reserve(batch.size());
    for (int i = 0; i < (int)batch.size(); ++i)
        if (!(mask & (1u << i)))
            rem.push_back(batch[i].first);

    if (rem.empty()) return 0.0;

    double d0 = (double)INT_MAX;
    for (int g : rem) {
        auto it = G.heuristics.find(g);
        if (it == G.heuristics.end()) continue;
        d0 = std::min(d0, it->second[from]);
    }
    if (d0 >= INT_MAX) return d0; // disconnected

    double tree = mst_lb(G, rem);
    if (tree >= INT_MAX) return (double)INT_MAX;

    return d0 + tree;
}

void CapSIPP::push_or_update(Node* succ)
{
    auto it = allNodes.find(succ);
    if (it == allNodes.end()) {
        succ->in_openlist = true;
        succ->open_handle  = open_list.push(succ);
        if (succ->getFVal() <= focal_bound)
            succ->focal_handle = focal_list.push(succ);
        allNodes.insert(succ);
        num_generated++;
        return;
    }

    Node* ex = *it;
    const double newf = succ->getFVal();
    const double oldf = ex->getFVal();

    if (oldf > newf || (oldf == newf && ex->conflicts > succ->conflicts)) {
        ex->g_val = succ->g_val;
        ex->h_val = succ->h_val;
        ex->parent = succ->parent;
        ex->interval = succ->interval;
        ex->conflicts = succ->conflicts;

        if (ex->in_openlist) {
            open_list.increase(ex->open_handle); // f improved
            if (ex->getFVal() <= focal_bound)
                ex->focal_handle = focal_list.push(ex);
        } else {
            ex->in_openlist = true;
            ex->open_handle = open_list.push(ex);
            if (ex->getFVal() <= focal_bound)
                ex->focal_handle = focal_list.push(ex);
        }
    }
    delete succ;
}

bool CapSIPP::try_serve_goals(Node*& curr)
{
    uint32_t newmask = curr->mask;
    for (int i = 0; i < (int)batch.size(); ++i) {
        if (newmask & (1u<<i)) continue;
        const auto& gi = batch[i];
        if (curr->state.location == gi.first && curr->state.timestep >= gi.second) {
            newmask |= (1u<<i);
        }
    }
    if (newmask != curr->mask) {
        curr->mask = newmask;
        return true;
    }
    return false;
}

int CapSIPP::earliest_holding_time(const ReservationTable& rt) const
{
    if (!hold_endpoints || batch.empty()) return 0;
    const int last = batch.back().first;
    return rt.getHoldingTimeFromCT(last);
}
