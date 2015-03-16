/*****************************************************************************\
 **  pmix_state.c - PMIx agent state related code
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

#include "pmixp_common.h"
#include "pmixp_debug.h"
#include "pmixp_info.h"
#include "pmixp_state.h"
#include "pmixp_nspaces.h"
#include "pmixp_coll.h"

pmixp_state_t _pmixp_state;

int pmixp_state_init()
{
	size_t size, i;
	pmix_range_t range;
	pmixp_coll_t *coll = NULL;

#ifndef NDEBUG
	_pmixp_state.magic = PMIX_STATE_MAGIC;
#endif
	_pmixp_state.cli_size = pmixp_info_tasks_loc();
	size = _pmixp_state.cli_size * sizeof(pmixp_cli_state_t);
	_pmixp_state.cli_state = xmalloc( size );
	for( i = 0; i < _pmixp_state.cli_size; i++ ){
#ifndef NDEBUG
		_pmixp_state.cli_state[i].magic = PMIXP_CLIENT_STATE_MAGIC;
#endif
		_pmixp_state.cli_state[i].state = PMIXP_CLI_UNCONNECTED;
		_pmixp_state.cli_state[i].localid = i;
	}

	_pmixp_state.coll = list_create(pmixp_xfree_buffer);

	// Add default collectives
	range.nranks = 0;
	range.ranks = NULL;
	strcpy(range.nspace, pmixp_info_namespace());
	coll = pmixp_state_coll_new(PMIXP_COLL_TYPE_FENCE, &range, 1);
	if( NULL == coll ){
		PMIXP_ERROR("Cannot add default FENCE collective");
		return SLURM_ERROR;
	}
	return SLURM_SUCCESS;
}

void pmixp_state_finalize()
{
	size_t size, i;
#ifndef NDEBUG
	_pmixp_state.magic = 0;
#endif
	_pmixp_state.cli_size = pmixp_info_tasks_loc();
	size = _pmixp_state.cli_size * sizeof(pmixp_cli_state_t);
	_pmixp_state.cli_state = xmalloc( size );
	for( i = 0; i < _pmixp_state.cli_size; i++ ){
		_pmixp_state.cli_state[i].state = PMIXP_CLI_UNCONNECTED;
	}
	list_destroy(_pmixp_state.coll);
}

pmixp_coll_t *
pmixp_state_coll_new(pmixp_coll_type_t type, const pmix_range_t *ranges,
		     size_t nranges)
{
	pmixp_coll_t *coll = pmixp_coll_new(ranges, nranges, type);
	xassert( coll );
	list_append(_pmixp_state.coll, coll);
	return coll;
}

static bool
_compare_ranges(const pmix_range_t *r1, const pmix_range_t *r2,
	       size_t nranges)
{
	int i, j;
	for( i=0; i < nranges; i++){
		if( 0 != strcmp(r1[i].nspace, r2[i].nspace) ){
			return false;
		}
		if( r1[i].nranks != r2[i].nranks ){
			return false;
		}
		for( j=0; j < r1[i].nranks; j++ ){
			if( r1[i].ranks[j] != r2[i].ranks[j] ){
				return false;
			}
		}
	}
	return true;
}

pmixp_coll_t *
pmixp_state_coll_find(pmixp_coll_type_t type, const pmix_range_t *ranges,
		      size_t nranges)
{
	pmixp_coll_t *coll = NULL;
	ListIterator it;

	it = list_iterator_create(_pmixp_state.coll);
	while( NULL != ( coll = list_next(it))){
		if( coll->nranges != nranges ){
			continue;
		}
		if( coll->type != type ){
			continue;
		}
		if( 0 == coll->nranges ){
			return coll;
		} if( _compare_ranges(coll->ranges, ranges, nranges) ){
			return coll;
		}
	}
	list_iterator_destroy(it);
	return NULL;
}




