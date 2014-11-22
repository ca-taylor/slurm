/*****************************************************************************\
 **  pmix_state.h - PMIx agent state related code
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

#ifndef PMIX_STATE_H
#define PMIX_STATE_H

#include "pmix_common.h"
#include "pmix_debug.h"
#include "pmix_io.h"

typedef enum { PMIX_CLI_UNCONNECTED, PMIX_CLI_ACK, PMIX_CLI_OPERATE, PMIX_CLI_COLL, PMIX_CLI_COLL_NB } pmix_cli_state_t;

typedef struct {
	pmix_cli_state_t state;
	uint32_t task_id;
	int fd;
	pmix_io_engine_t eng;
} client_state_t;

typedef enum { PMIX_COLL_SYNC, PMIX_COLL_GATHER, PMIX_COLL_FORWARD } pmix_coll_state_t;

typedef struct {
	pmix_coll_state_t state;
	uint32_t local_joined;
	uint8_t *local_contrib;
	uint32_t nodes_joined;
	uint8_t *nodes_contrib;
} collective_state_t;

typedef struct {
#ifndef NDEBUG
#       define PMIX_STATE_MAGIC 0xdeadbeef
	int  magic;
#endif
	uint32_t cli_size;
	client_state_t *cli_state;
	collective_state_t coll;
	eio_handle_t *cli_handle, *srv_handle;
} pmix_state_t;

extern pmix_state_t pmix_state;

void pmix_state_init();

inline static void pmix_state_sanity_check()
{
	xassert( pmix_state.magic == PMIX_STATE_MAGIC );
}

/*
 * Client state
 */

inline static void pmix_state_cli_sanity_check(uint32_t localid)
{
	pmix_state_sanity_check();
	xassert( localid < pmix_state.cli_size);
	xassert( pmix_state.cli_state[localid].fd >= 0 );
}

inline static pmix_io_engine_t *pmix_state_cli_io(int localid)
{
	pmix_state_cli_sanity_check(localid);
	return &pmix_state.cli_state[localid].eng;
}

inline static int pmix_state_cli_fd(int localid)
{
	pmix_state_cli_sanity_check(localid);
	return pmix_state.cli_state[localid].fd;
}


inline static pmix_cli_state_t pmix_state_cli(uint32_t localid)
{
	pmix_state_cli_sanity_check(localid);
	client_state_t *cli = &pmix_state.cli_state[localid];
	return cli->state;
}

inline static int pmix_state_cli_connecting(int localid, int fd)
{
	pmix_state_sanity_check();
	if( !( localid < pmix_state.cli_size ) ){
		return SLURM_ERROR;
	}
	client_state_t *cli = &pmix_state.cli_state[localid];
	if( cli->state != PMIX_CLI_UNCONNECTED ){
		// We already have this task connected. Shouldn't happen.
		// FIXME: should we ignore new or old connection?
		// Ignore new by now, discuss with Ralph.
		return SLURM_ERROR;
	}
	// TODO: will need to implement additional step - ACK
	cli->state = PMIX_CLI_ACK;
	cli->task_id = localid;
	cli->fd = fd;
	return SLURM_SUCCESS;
}

inline static int pmix_state_cli_connected(int taskid)
{
	pmix_state_sanity_check();
	if( !( taskid < pmix_state.cli_size ) ){
		return SLURM_ERROR;
	}
	client_state_t *cli = &pmix_state.cli_state[taskid];
	// TODO: will need to implement additional step - ACK
	cli->state = PMIX_CLI_OPERATE;
	return SLURM_SUCCESS;
}

inline static void pmix_state_cli_finalize(uint32_t localid)
{
	pmix_state_cli_sanity_check(localid);
	client_state_t *cli = &pmix_state.cli_state[localid];
	cli->fd = -1;
	cli->state = PMIX_CLI_UNCONNECTED;
	if( !pmix_io_finalized( &cli->eng ) ){
		pmix_io_finalize(&cli->eng, 0);
	}
}

/*
 * Collective state
 */

/*
 * Database-related information
 */

bool pmix_state_node_contrib_ok(uint32_t gen, int idx);
bool pmix_state_task_contrib_ok(int idx, bool blocking);
bool pmix_state_coll_local_ok();
bool pmix_state_coll_forwad();
bool pmix_state_coll_sync();
bool pmix_state_node_contrib_cancel(int idx);
bool pmix_state_task_contrib_cancel(int idx);

inline static void pmix_state_task_coll_finish(uint32_t localid)
{
	pmix_state_cli_sanity_check(localid);
	client_state_t *cli = &pmix_state.cli_state[localid];
	xassert( cli->state == PMIX_CLI_COLL || cli->state == PMIX_CLI_COLL_NB);
	cli->state = PMIX_CLI_OPERATE;
}

/*
 * Direct modex
 */
void pmix_state_defer_local_req(uint32_t localid, uint32_t taskid);
bool pmix_state_local_reqs_to_posted(uint32_t taskid);
List pmix_state_local_reqs_to(uint32_t taskid);
List pmix_state_local_reqs_from(uint32_t localid);
void pmix_state_defer_remote_req(uint32_t localid, uint32_t nodeid);
List pmix_state_remote_reqs_to(uint32_t localid);
int pmix_state_remote_reqs_to_cnt(uint32_t localid);

#endif // STATE_H
