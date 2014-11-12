/*****************************************************************************\
 **  pmix_common.h - PMIx common declarations and includes
 *****************************************************************************
 *  Copyright (C) 2014 Institude of Semiconductor Physics Siberian Branch of
 *                     Russian Academy of Science
 *  Written by Artem Polyakov <artpol84@gmail.com>.
 *  All rights reserved.
 *
 *  This file is part of SLURM, a resource management program.
 *  For details, see <http://slurm.schedmd.com/>.
 *  Please also read the included file: DISCLAIMER.
 *
 *  SLURM is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and
 *  distribute linked combinations including the two. You must obey the GNU
 *  General Public License in all respects for all of the code used other than
 *  OpenSSL. If you modify file(s) with this exception, you may extend this
 *  exception to your version of the file(s), but you are not obligated to do
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in
 *  the program, then also delete it here.
 *
 *  SLURM is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with SLURM; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
\*****************************************************************************/

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
#define PMIX_STEPD_LOCAL_ADDR_FMT "/home/artpol/slurm_tmp/sock.pmix_stepd_local.%u.%u"
#define PMIX_SRUN_ADDR_FMT "/home/artpol/slurm_tmp/sock.pmix_srun.%u.%u"
#define PMIX_CLI_ADDR_FMT "/home/artpol/slurm_tmp/sock.pmix_cli.%u.%u"

#define MAX_USOCK_PATH                                      \
  ( (size_t) &(((struct sockaddr_un *)0 + 1)->sun_family) - \
    (size_t)&(((struct sockaddr_un *)0)->sun_path) )

#define SERVER_URI_ENV "PMIX_SERVER_URI"
#define JOBID_ENV "PMIX_ID"

// Job master process contact info
#define PMIX_SRUN_HOST_ENV "SLURM_SRUN_COMM_HOST"
#define PMIX_SRUN_PORT_ENV "SLURM_PMIX_SRUN_PORT"
// Job/step resource description
#define PMIX_STEP_NODES_ENV "SLURM_STEP_NODELIST"
#define PMIX_JOB_NODES_ENV "SLURM_JOB_NODELIST"
#define PMIX_CPUS_PER_NODE_ENV "SLURM_JOB_CPUS_PER_NODE"
#define PMIX_CPUS_PER_TASK "SLURM_CPUS_PER_TASK"

// For future spawn implementation
#define PMIX_SPAWN_BASE_STEP "SLURM_PMIX_SPAWN_BASE_STEP"
#define PMIX_SPAWN_BASE_NODE "SLURM_PMIX_SPAWN_BASE_NODE"

#endif // PMIX_COMMON_H
