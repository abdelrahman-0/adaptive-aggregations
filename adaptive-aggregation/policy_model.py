from math import sqrt
from math import ceil

def solve_quadratic_fn(a, b, c):
    discriminant = b * b - 4 * a * c
    if discriminant < 0:
        return 1
    sqrt_discriminant = sqrt(discriminant)
    root1 = (-b + sqrt_discriminant) / (2 * a)
    root2 = (-b - sqrt_discriminant) / (2 * a)
    return max(1, root1, root2)


def build_time_estimator(ngrps, factor):
    return ngrps / factor


def estimate_nworkers(elapsed_time, remainder_factor, elapsed_data, groups):
    glob_ht_build_time = build_time_estimator(groups, 1e7)
    t = elapsed_data * remainder_factor / BANDWIDTH
    nworkers = solve_quadratic_fn(a=SLA - elapsed_time, b=-t - glob_ht_build_time, c=t)
    return nworkers


SLA = 1.5
BANDWIDTH = 100e9 / 8

if __name__ == '__main__':
    # SLA in seconds
    elapsed_time = 0.0
    factor = 9
    data = 1e9
    ngrps = 1000

    for data_GB in range(1, 11):
        print("data seen:", data_GB, "GB", end=', ')
        print("nworkers:", estimate_nworkers(elapsed_time, factor, data * data_GB, ngrps))
        print("=====")
