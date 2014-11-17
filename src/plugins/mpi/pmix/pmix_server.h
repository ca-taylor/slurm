/*****************************************************************************\
 **  pmix_server.h - PMIx server side functionality
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

#ifndef PMIX_SERVER_H
#define PMIX_SERVER_H

#include "pmix_common.h"

typedef enum { PMIX_FENCE, PMIX_FENCE_RESP, PMIX_DIRECT, PMIX_DIRECT_RESP  } pmix_srv_cmd_t;

int pmix_stepd_init(const stepd_step_rec_t *job, char ***env);
int pmix_srun_init(const mpi_plugin_client_info_t *job, char ***env);
void pmix_server_new_conn(int fd);

void *pmix_server_alloc_msg(uint32_t size, void **payload);
void pmix_server_msg_setcmd(void *msg, pmix_srv_cmd_t cmd);
void pmix_server_msg_finalize(void *msg);
uint32_t pmix_server_msg_size(void *msg);
void *pmix_server_msg_start(void *msg);
void pmix_server_dmdx_request(uint32_t gid);

#endif // SERVER_H
