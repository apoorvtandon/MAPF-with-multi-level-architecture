#include "MultiGoalSIPPAdapter.h"
#include <iostream>
#include <vector>

// We include these to ensure the translation unit knows the real types.
#include "BasicGraph.h"
#include "ReservationTable.h"
#include "States.h"
#include "StateTimeAStar.h"

// Minimal smoke test: we won't build a full map here.
// We only check that the adapter symbols link and can be instantiated.
// For a real path, you'll run through KivaSystem with a real graph later.

int main() {
    using namespace mgmapf;

    // Create dummy objects (not fully functional without a real graph).
    // In RHCR, BasicGraph is usually constructed elsewhere; here we only need references.
    // So we won't run plan_bundle here (would need a real graph). We just ensure compilation.

    MultiGoalSIPPAdapterRunner runner;

    std::cout << "MultiGoalSIPPAdapterRunner compiled & linked OK.\n";
    std::cout << "Next: integrate into KivaSystem and run on real kiva.map.\n";
    return 0;
}
