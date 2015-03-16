/*****************************************************************************\
 **  pmix_coll.h - PMIx collective primitives
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

#ifndef PMIXP_COLL_H
#define PMIXP_COLL_H
#include "pmixp_common.h"

typedef enum { PMIXP_COLL_SYNC, PMIXP_COLL_FAN_IN, PMIXP_COLL_FAN_OUT } pmixp_coll_state_t;
typedef enum { PMIXP_COLL_TYPE_FENCE, PMIXP_COLL_TYPE_CONNECT, PMIXP_COLL_TYPE_DISCONNECT } pmixp_coll_type_t;
typedef enum {PMIX_PARENT_NONE, PMIX_PARENT_ROOT, PMIX_PARENT_SRUN, PMIX_PARENT_STEPD } pmixp_coll_parent_t;


typedef struct {
#ifndef NDEBUG
#       define PMIXP_COLL_STATE_MAGIC 0xCA11CAFE
	int  magic;
#endif
	/* general information */
	pmixp_coll_state_t state;
	pmixp_coll_type_t type;
	/* PMIx collective id */
	pmix_range_t *ranges;
	size_t nranges;
	int my_nspace;
	uint32_t nodeid;
	/* tree structure */
	char *parent_host;
	hostlist_t all_children;
	uint32_t children_cnt;
	/* contributions accounting */
	bool local_contrib;
	uint32_t contrib_cnt;

	/* Check who contributes */
	int *ch_nodeids;
#ifndef NDEBUG
	bool *ch_contribs;
#endif
	/* collective data */
	char *data;
	/* max and actual size used for the
	 * collective data */
	size_t data_sz, data_pay;

	/* libpmix callback data*/
	pmix_modex_cbfunc_t cbfunc;
	void *cbdata;
} pmixp_coll_t;

inline static void pmixp_coll_sanity_check(pmixp_coll_t *coll)
{
	xassert( coll->magic == PMIXP_COLL_STATE_MAGIC );
}

int pmixp_coll_init(char ***env, uint32_t hdrsize);
pmixp_coll_t *pmixp_coll_new(const pmix_range_t *ranges, size_t nranges,
			     pmixp_coll_type_t type);
inline static void pmixp_coll_set_callback(pmixp_coll_t *coll,
			       pmix_modex_cbfunc_t cbfunc, void *cbdata){
	pmixp_coll_sanity_check(coll);
	coll->cbfunc = cbfunc;
	coll->cbdata = cbdata;
}

void pmixp_coll_fan_out_data(pmixp_coll_t *coll, void *data,
				uint32_t size);
int pmixp_coll_contrib_loc(pmixp_coll_t *coll);
int pmixp_coll_contrib_node(pmixp_coll_t *coll, char *nodename,
			    void *contrib, size_t size);
bool pmixp_coll_progress(pmixp_coll_t *coll, char *fwd_node,
			 void **data, uint64_t size);
int pmixp_coll_unpack_ranges(void *data, size_t size, pmixp_coll_type_t *type,
			     pmix_range_t **ranges, size_t *nranges);

#endif // PMIXP_COLL_H
