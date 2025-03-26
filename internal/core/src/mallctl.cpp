#include "jemalloc.h"
#include <iostream>

int
main() {
    size_t allocated = 0;
    size_t len = sizeof(allocated);
    mallctl("stats.allocated", &allocated, &len, NULL, 0);
    std::cout << "allocated: " << allocated << std::endl;

    size_t active = 0;
    len = sizeof(active);
    mallctl("stats.active", &active, &len, NULL, 0);
    std::cout << "active: " << active << std::endl;

    size_t mapped = 0;
    len = sizeof(mapped);
    mallctl("stats.mapped", &mapped, &len, NULL, 0);
    std::cout << "mapped: " << mapped << std::endl;

    size_t resident = 0;
    len = sizeof(resident);
    mallctl("stats.resident", &resident, &len, NULL, 0);
    std::cout << "resident: " << resident << std::endl;

    size_t retained = 0;
    len = sizeof(retained);
    mallctl("stats.retained", &retained, &len, NULL, 0);
    std::cout << "retained: " << retained << std::endl;

    size_t metadata = 0;
    len = sizeof(metadata);
    mallctl("stats.metadata", &metadata, &len, NULL, 0);
    std::cout << "metadata: " << metadata << std::endl;
}
