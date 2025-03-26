#include "mallctl_c.h"

#include <iostream>
#include <jemalloc/jemalloc.h>

void
print_jemalloc_stats() {
    size_t allocated;
    size_t len = sizeof(allocated);
    mallctl("stats.allocated", &allocated, &len, NULL, 0);
    std::cout << "jemalloc stats allocated: " << allocated << std::endl;

    size_t active;
    len = sizeof(active);
    mallctl("stats.active", &active, &len, NULL, 0);
    std::cout << "jemalloc stats active: " << active << std::endl;

    size_t mapped;
    len = sizeof(mapped);
    mallctl("stats.mapped", &mapped, &len, NULL, 0);
    std::cout << "jemalloc stats mapped: " << mapped << std::endl;

    size_t resident;
    len = sizeof(resident);
    mallctl("stats.resident", &resident, &len, NULL, 0);
    std::cout << "jemalloc stats resident: " << resident << std::endl;

    size_t retained;
    len = sizeof(retained);
    mallctl("stats.retained", &retained, &len, NULL, 0);
    std::cout << "jemalloc stats retained: " << retained << std::endl;

    size_t metadata;
    len = sizeof(metadata);
    mallctl("stats.metadata", &metadata, &len, NULL, 0);
    std::cout << "jemalloc stats metadata: " << metadata << std::endl;
}
