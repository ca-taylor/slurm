#ifndef MPI_PMIX_H
#define MPI_PMIX_H

#include "pmix_common.h"

#include "pmix_debug.h"
#include "pmix_msg.h"
#include "pmix_info.h"
#include "pmix_agent.h"

#define PMIX_STEPD_ADDR_FMT "/home/artpol/slurm_tmp/sock.pmix_stepd.%u.%u"
#define PMIX_SRUN_ADDR_FMT "/home/artpol/slurm_tmp/sock.pmix_srun.%u.%u"
#define PMIX_CLI_ADDR_FMT "/home/artpol/slurm_tmp/sock.pmix_cli.%u.%u"

#define MAX_USOCK_PATH                                      \
  ( (size_t) &(((struct sockaddr_un *)0 + 1)->sun_family) - \
    (size_t)&(((struct sockaddr_un *)0)->sun_path) )

#define SERVER_URI_ENV "PMIX_SERVER_URI"
#define JOBID_ENV "PMIX_ID"

#define PMIX_SRUN_PORT_ENV "SLURM_PMIX_SRUN_PORT"
#define PMIX_STEP_NODES_ENV "SLURM_PMIX_SRUN_PORT"

#endif // MPI_PMIX_H
