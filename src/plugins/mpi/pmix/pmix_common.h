#ifndef PMIX_COMMON_H
#define PMIX_COMMON_H

#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <stdlib.h>
#include <unistd.h>

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
#include "src/common/fd.h"
#include "src/common/net.h"

#define PMIX_STEPD_ADDR_FMT "/home/artpol/slurm_tmp/sock.pmix_stepd.%u.%u"
#define PMIX_SRUN_ADDR_FMT "/home/artpol/slurm_tmp/sock.pmix_srun.%u.%u"
#define PMIX_CLI_ADDR_FMT "/home/artpol/slurm_tmp/sock.pmix_cli.%u.%u"

#define MAX_USOCK_PATH                                      \
  ( (size_t) &(((struct sockaddr_un *)0 + 1)->sun_family) - \
    (size_t)&(((struct sockaddr_un *)0)->sun_path) )

#define SERVER_URI_ENV "PMIX_SERVER_URI"
#define JOBID_ENV "PMIX_ID"

#define PMIX_SRUN_HOST_ENV "SLURM_SRUN_COMM_HOST"
#define PMIX_SRUN_PORT_ENV "SLURM_PMIX_SRUN_PORT"
#define PMIX_STEP_NODES_ENV "SLURM_STEP_NODELIST"

#endif // PMIX_COMMON_H
