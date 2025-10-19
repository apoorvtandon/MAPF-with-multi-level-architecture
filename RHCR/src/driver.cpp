#include "KivaSystem.h"
#include "SortingSystem.h"
#include "OnlineSystem.h"
#include "BeeSystem.h"

#include "WHCAStar.h"
#include "ECBS.h"
#include "LRAStar.h"
#include "PBS.h"
#include "ID.h"

#include "SIPP.h"
#include "StateTimeAStar.h"
#include "CapSIPP.h"

#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>

#include <climits>
#include <ctime>
#include <cassert>
#include <iostream>
#include <fstream>

using std::cout;
using std::endl;
using std::string;

static void set_parameters(BasicSystem& system, const boost::program_options::variables_map& vm)
{
    system.outfile            = vm["output"].as<std::string>();
    system.screen             = vm["screen"].as<int>();
    system.log                = vm["log"].as<bool>();
    system.num_of_drives      = vm["agentNum"].as<int>();
    system.time_limit         = vm["cutoffTime"].as<int>();
    system.simulation_window  = vm["simulation_window"].as<int>();
    system.planning_window    = vm["planning_window"].as<int>();
    system.travel_time_window = vm["travel_time_window"].as<int>();
    system.consider_rotation  = vm["rotation"].as<bool>();
    system.k_robust           = vm["robust"].as<int>();
    system.hold_endpoints     = vm["hold_endpoints"].as<bool>();
    system.useDummyPaths      = vm["dummy_paths"].as<bool>();
    if (vm.count("seed"))
        system.seed = vm["seed"].as<int>();
    else
        system.seed = (int)time(0);
    srand(system.seed);
}

static MAPFSolver* set_solver(const BasicGraph& G, const boost::program_options::variables_map& vm)
{
    // -------- single-agent selection --------
    string sa_name = vm["single_agent_solver"].as<string>();
    SingleAgentSolver* path_planner = nullptr;

    if (sa_name == "ASTAR") {
        path_planner = new StateTimeAStar();
    } else if (sa_name == "SIPP") {
        path_planner = new SIPP();
    } else if (sa_name == "CAPSIPP") {
        auto* capsipp = new CapSIPP();
        capsipp->capacity         = vm["agent_capacity"].as<int>();
        capsipp->endpoint_min_sep = vm["agent_endpoint_min_sep"].as<int>();
        path_planner = capsipp;
    } else {
        cout << "Single-agent solver " << sa_name << " does not exist!" << endl;
        exit(-1);
    }

    // -------- multi-agent selection --------
    string ma_name = vm["solver"].as<string>();
    MAPFSolver* mapf_solver = nullptr;

    if (ma_name == "ECBS") {
        auto* ecbs = new ECBS(G, *path_planner);
        ecbs->potential_function  = vm["potential_function"].as<string>();
        ecbs->potential_threshold = vm["potential_threshold"].as<double>();
        ecbs->suboptimal_bound    = vm["suboptimal_bound"].as<double>();
        mapf_solver = ecbs;
    } else if (ma_name == "PBS") {
        auto* pbs = new PBS(G, *path_planner);
        pbs->lazyPriority = vm["lazyP"].as<bool>();
        auto prioritize_start = vm["prioritize_start"].as<bool>();
        if (vm["hold_endpoints"].as<bool>() || vm["dummy_paths"].as<bool>())
            prioritize_start = false;
        pbs->prioritize_start = prioritize_start;
        pbs->setRT(vm["CAT"].as<bool>(), prioritize_start);

        // Pass reservation-table hardening knobs into the initial RT
        pbs->initial_rt.endpoint_min_sep = vm["agent_endpoint_min_sep"].as<int>();
        pbs->initial_rt.tail_padding     = vm["tail_padding"].as<int>();  // <-- requires int tail_padding in ReservationTable.h
        pbs->initial_rt.k_robust         = vm["robust"].as<int>();

        mapf_solver = pbs;
    } else if (ma_name == "WHCA") {
        mapf_solver = new WHCAStar(G, *path_planner);
    } else if (ma_name == "LRA") {
        mapf_solver = new LRAStar(G, *path_planner);
    } else {
        cout << "Solver " << ma_name << " does not exist!" << endl;
        exit(-1);
    }

    if (vm["id"].as<bool>()) {
        return new ID(G, *path_planner, *mapf_solver);
    } else {
        return mapf_solver;
    }
}

int main(int argc, char** argv)
{
    namespace po = boost::program_options;

    // -------- CLI options --------
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help", "produce help message")
        ("scenario", po::value<std::string>()->required(), "scenario (SORTING, KIVA, ONLINE, BEE)")
        ("map,m",    po::value<std::string>()->required(), "input map file")
        ("task",     po::value<std::string>()->default_value(""), "input task file")
        ("output,o", po::value<std::string>()->default_value("../exp/test"), "output folder name")
        ("agentNum,k", po::value<int>()->required(), "number of drives")
        ("cutoffTime,t", po::value<int>()->default_value(60), "cutoff time (seconds)")
        ("seed,d",   po::value<int>(), "random seed")
        ("screen,s", po::value<int>()->default_value(1), "screen option (0: none; 1: results; 2:all)")

        ("solver", po::value<string>()->default_value("PBS"), "solver (LRA, PBS, WHCA, ECBS)")
        ("id",     po::value<bool>()->default_value(false), "independence detection")
        ("single_agent_solver", po::value<string>()->default_value("SIPP"),
            "single-agent solver (ASTAR, SIPP, CAPSIPP)")
        ("lazyP",  po::value<bool>()->default_value(false), "use lazy priority")

        ("simulation_time",   po::value<int>()->default_value(5000), "run simulation")
        ("simulation_window", po::value<int>()->default_value(5), "call the planner every simulation_window timesteps")
        ("travel_time_window",po::value<int>()->default_value(0), "consider traffic jams within the given window")
        ("planning_window",   po::value<int>()->default_value(INT_MAX / 2),
            "planner outputs plans with first planning_window timesteps collision-free")

        ("potential_function",  po::value<string>()->default_value("NONE"), "potential function (NONE, SOC, IC)")
        ("potential_threshold", po::value<double>()->default_value(0), "potential threshold")
        ("rotation", po::value<bool>()->default_value(false), "consider rotation")
        ("robust",   po::value<int>()->default_value(0), "k-robust (for PBS)")
        ("CAT",      po::value<bool>()->default_value(false), "use conflict-avoidance table")
        ("hold_endpoints", po::value<bool>()->default_value(false), "Hold endpoints (Ma et al., AAMAS 2017)")
        ("dummy_paths",    po::value<bool>()->default_value(false), "Dummy paths (Liu et al., AAMAS 2019)")
        ("prioritize_start", po::value<bool>()->default_value(true), "Prioritize waiting at start locations")
        ("suboptimal_bound", po::value<double>()->default_value(1), "Suboptimal bound for ECBS")
        ("log", po::value<bool>()->default_value(false), "save search trees / priority trees")

        // ---- Capacity + endpoint controls
        ("capacity_mode",          po::value<bool>()->default_value(false), "enable capacity-aware batching")
        ("agent_capacity",         po::value<int>()->default_value(2), "max goals per agent per plan")
        ("agent_endpoint_min_sep", po::value<int>()->default_value(2), "bounded endpoint separation (timesteps)")
        ("tail_padding",           po::value<int>()->default_value(0), "ghost hold at path end (timesteps)")
    ;

    clock_t start_time = clock();
    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);

    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 1;
    }
    po::notify(vm);

    // sanity for hold/dummy modes
    if (vm["hold_endpoints"].as<bool>() || vm["dummy_paths"].as<bool>()) {
        if (vm["hold_endpoints"].as<bool>() && vm["dummy_paths"].as<bool>()) {
            std::cerr << "Hold endpoints and dummy paths cannot be used simultaneously" << endl;
            return -1;
        }
        if (vm["simulation_window"].as<int>() != 1) {
            std::cerr << "Hold endpoints and dummy paths require simulation_window = 1" << endl;
            return -1;
        }
        if (vm["planning_window"].as<int>() < INT_MAX / 2) {
            std::cerr << "Hold endpoints and dummy paths cannot work with planning windows" << endl;
            return -1;
        }
    }

    // ensure output dirs
    boost::filesystem::path dir(vm["output"].as<std::string>() + "/");
    boost::filesystem::create_directories(dir);
    if (vm["log"].as<bool>()) {
        boost::filesystem::path dir1(vm["output"].as<std::string>() + "/goal_nodes/");
        boost::filesystem::path dir2(vm["output"].as<std::string>() + "/search_trees/");
        boost::filesystem::create_directories(dir1);
        boost::filesystem::create_directories(dir2);
    }

    // -------- scenarios --------
    string scenario = vm["scenario"].as<string>();
    if (scenario == "KIVA") {
        KivaGrid G;
        if (!G.load_map(vm["map"].as<std::string>()))
            return -1;

        MAPFSolver* solver = set_solver(G, vm);
        KivaSystem system(G, *solver);
        set_parameters(system, vm);

        // capacity knobs into system
        system.capacity_mode           = vm["capacity_mode"].as<bool>();
        system.agent_capacity          = vm["agent_capacity"].as<int>();
        system.agent_endpoint_min_sep  = vm["agent_endpoint_min_sep"].as<int>();

        G.preprocessing(system.consider_rotation);
        system.simulate(vm["simulation_time"].as<int>());

        delete solver;
        return 0;
    }
    else if (scenario == "SORTING") {
        SortingGrid G;
        if (!G.load_map(vm["map"].as<std::string>()))
            return -1;
        MAPFSolver* solver = set_solver(G, vm);
        SortingSystem system(G, *solver);
        assert(!system.hold_endpoints);
        assert(!system.useDummyPaths);
        set_parameters(system, vm);
        G.preprocessing(system.consider_rotation);
        system.simulate(vm["simulation_time"].as<int>());
        delete solver;
        return 0;
    }
    else if (scenario == "ONLINE") {
        OnlineGrid G;
        if (!G.load_map(vm["map"].as<std::string>()))
            return -1;
        MAPFSolver* solver = set_solver(G, vm);
        OnlineSystem system(G, *solver);
        assert(!system.hold_endpoints);
        assert(!system.useDummyPaths);
        set_parameters(system, vm);
        G.preprocessing(system.consider_rotation);
        system.simulate(vm["simulation_time"].as<int>());
        delete solver;
        return 0;
    }
    else if (scenario == "BEE") {
        BeeGraph G;
        if (!G.load_map(vm["map"].as<std::string>()))
            return -1;
        MAPFSolver* solver = set_solver(G, vm);
        BeeSystem system(G, *solver);
        assert(!system.hold_endpoints);
        assert(!system.useDummyPaths);
        set_parameters(system, vm);
        G.preprocessing(vm["task"].as<std::string>(), system.consider_rotation);
        system.load_task_assignments(vm["task"].as<std::string>());
        system.simulate();
        double runtime = (double)(clock() - start_time) / CLOCKS_PER_SEC;
        cout << "Overall runtime:           " << runtime << " seconds." << endl;
        cout << "Makespan:       " << system.get_makespan() << " timesteps." << endl;
        cout << "Flowtime:       " << system.get_flowtime() << " timesteps." << endl;
        cout << "Flowtime lowerbound: " << system.get_flowtime_lowerbound() << " timesteps." << endl;
        auto flower_ids = system.get_missed_flower_ids();
        cout << "Missed tasks:";
        for (auto id : flower_ids) cout << " " << id;
        cout << endl;
        cout << "Objective: " << system.get_objective() << endl;
        std::ofstream output(vm["output"].as<std::string>() + "/MAPF_results.txt");
        output << "Overall runtime: " << runtime << " seconds." << endl;
        output << "Makespan: " << system.get_makespan() << " timesteps." << endl;
        output << "Flowtime: " << system.get_flowtime() << " timesteps." << endl;
        output << "Flowtime lowerbound: " << system.get_flowtime_lowerbound() << " timesteps." << endl;
        output << "Missed tasks:";
        for (auto id : flower_ids) output << " " << id;
        output << endl;
        output << "Objective: " << system.get_objective() << endl;
        output.close();
        delete solver;
        return 0;
    }
    else {
        cout << "Scenario " << scenario << " does not exist!" << endl;
        return -1;
    }
}
