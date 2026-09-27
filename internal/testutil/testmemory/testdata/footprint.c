#include <libproc.h>
#include <sys/resource.h>
#include <unistd.h>
#include <stddef.h>
#include <stdio.h>

_Static_assert(sizeof(struct rusage_info_v0) == 96, "V0 size");
_Static_assert(_Alignof(struct rusage_info_v0) == 8, "V0 alignment");
_Static_assert(offsetof(struct rusage_info_v0, ri_phys_footprint) == 72, "footprint offset");
int main(void) {
    // The test creates this helper. Only observe that owning parent, never
    // accept an arbitrary PID from an argument or enumerate user processes.
    struct rusage_info_v0 usage = {0};
    pid_t owner = getppid();
    if (proc_pid_rusage(owner, RUSAGE_INFO_V0, (rusage_info_t *)&usage) != 0) return 1;
    printf("%d %llu\n", owner, usage.ri_phys_footprint);
    return usage.ri_phys_footprint == 0;
}
