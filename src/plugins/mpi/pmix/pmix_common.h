#ifndef PMIX_COMMON_H
#define PMIX_COMMON_H

#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

// Common includes for all source files
// Define SLURM translator header first to override
// all translated functions
#include "src/common/slurm_xlator.h"

// Other useful includes
#include "slurm/slurm_errno.h"
#include "src/common/mpi.h"
#include "src/slurmd/slurmstepd/slurmstepd_job.h"
#include "src/common/xmalloc.h"
#include "src/common/xassert.h"
#include "src/common/eio.h"

#endif // PMIX_COMMON_H
