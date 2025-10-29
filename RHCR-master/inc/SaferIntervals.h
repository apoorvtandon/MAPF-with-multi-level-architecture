#pragma once

struct SaferCfg {
    bool  enabled   = true;   // --safer_intervals
    double theta    = 0.6;     // --safer_theta
    double alpha    = 1.0;     // weight: CAT density
    double beta     = 1.0;     // weight: short-horizon flow
    double gamma    = 0.25;    // weight: inverse slack
    int    k_robust = 1;       // reuse your k-robust if you have one
};
