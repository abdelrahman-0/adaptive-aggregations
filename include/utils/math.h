#pragma once

#include <cmath>

namespace math {

inline double find_max_quadratic_root(double a, double b, double c)
{
    // discriminant
    double discriminant = b * b - 4 * a * c;
    if (discriminant < 0) {
        return -1.0;
    }
    double sqrt_factor = std::sqrt(discriminant);
    double root1       = (-b - sqrt_factor) / (2 * a);
    double root2       = (-b + sqrt_factor) / (2 * a);
    return std::max(root1, root2);
}

} // namespace math
