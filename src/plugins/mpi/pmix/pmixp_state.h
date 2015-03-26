/*****************************************************************************\
 **  pmix_state.h - PMIx agent state related code
 *****************************************************************************
 *  Copyright (C) 2014-2015 Artem Polyakov. All rights reserved.
 *  Copyright (C) 2015      Mellanox Technologies. All rights reserved.
 *  Written by Artem Polyakov <artpol84@gmail.com>.
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

#ifndef PMIXP_STATE_H
#define PMIXP_STATE_H

#include "pmixp_common.h"
#include "pmixp_debug.h"
#include "pmixp_io.h"
#include "pmixp_coll.h"

/*
 * Client state structure
 */

typedef enum { PMIXP_CLI_UNCONNECTED, PMIXP_CLI_OPERATE } pmix_cli_state_name_t;

typedef struct {
#ifndef NDEBUG
#       define PMIXP_CLIENT_STATE_MAGIC 0xFEEDFACE
	int  magic;
#endif
	pmix_cli_state_name_t state;
	uint32_t localid;
	// TODO: To support clones on one rank need variable-size
	// array of I/O engines here in future
	pmixp_io_engine_t eng;
} pmixp_cli_state_t;

/*
 * Collective state structure
 */


/*
 * PMIx plugin state structure
 */

typedef struct {
#ifndef NDEBUG
#       define PMIX_STATE_MAGIC 0xFEEDCAFE
	int  magic;
#endif
	uint32_t cli_size;
	pmixp_cli_state_t *cli_state;
	List coll;
	eio_handle_t *cli_handle, *srv_handle;
} pmixp_state_t;

extern pmixp_state_t _pmixp_state;

/*
 * General PMIx plugin state manipulation functions
 */

int pmixp_state_init();
void pmixp_state_finalize();

inline static void pmixp_state_sanity_check()
{
	xassert( _pmixp_state.magic == PMIX_STATE_MAGIC );
}

inline static uint32_t pmixp_state_cli_count()
{
	pmixp_state_sanity_check();
	return _pmixp_state.cli_size;
}

/*
 * Client state manipulation functions
 */

inline static void pmixp_state_cli_sanity_check(uint32_t localid)
{
	pmixp_state_sanity_check();
	xassert( localid < _pmixp_state.cli_size);
	xassert( _pmixp_state.cli_state[localid].magic == PMIXP_CLIENT_STATE_MAGIC );
}

inline static int pmixp_state_cli_connected(int localid)
{
	pmixp_state_cli_sanity_check(localid);
	pmixp_cli_state_t *cli = &_pmixp_state.cli_state[localid];
	xassert( cli->state == PMIXP_CLI_UNCONNECTED );
	cli->state = PMIXP_CLI_OPERATE;
	return SLURM_SUCCESS;
}

inline static int pmixp_state_cli_disconnected(int localid)
{
	pmixp_state_cli_sanity_check(localid);
	pmixp_cli_state_t *cli = &_pmixp_state.cli_state[localid];
	xassert( cli->state == PMIXP_CLI_OPERATE );
	cli->state = PMIXP_CLI_UNCONNECTED;
	return SLURM_SUCCESS;
}

inline static pmixp_cli_state_t *
pmixp_state_cli(uint32_t localid)
{
	pmixp_state_cli_sanity_check(localid);
	pmixp_cli_state_t *cli = &_pmixp_state.cli_state[localid];
	return cli;
}

inline static pmixp_io_engine_t *
pmixp_state_cli_io(int localid)
{
	pmixp_state_cli_sanity_check(localid);
	pmixp_cli_state_t *cli = pmixp_state_cli(localid);
	xassert( cli->state == PMIXP_CLI_OPERATE );
	return &cli->eng;
}

inline static int pmixp_state_cli_fd(int localid)
{
	pmixp_state_cli_sanity_check(localid);
	pmixp_cli_state_t *cli = pmixp_state_cli(localid);
	xassert( cli->state == PMIXP_CLI_OPERATE );
	return cli->eng.sd;
}


/*
 * Collective state
 */

pmixp_coll_t *
pmixp_state_coll_find(pmixp_coll_type_t type, const pmix_range_t *ranges,
		      size_t nranges);
pmixp_coll_t *
pmixp_state_coll_new(pmixp_coll_type_t type, const pmix_range_t *ranges,
		     size_t nranges);

#endif // PMIXP_STATE_H
