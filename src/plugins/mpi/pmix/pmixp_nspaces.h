/*****************************************************************************\
 **  pmix_db.h - PMIx KVS database
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

#ifndef PMIXP_NSPACES_H
#define PMIXP_NSPACES_H

#include "pmixp_common.h"
#include "pmixp_info.h"
#include "pmixp_debug.h"
#include "pmixp_state.h"

typedef struct {
	void *blob;
	int blob_sz;
} pmixp_blob_t;

typedef struct {
	pmix_modex_data_t data;
	pmix_scope_t scope;
} pmixp_modex_t;


typedef struct {
#ifndef NDEBUG
#       define PMIXP_NSPACE_MAGIC 0xCAFED00D
	int  magic;
#endif
	char name[PMIX_MAX_NSLEN];
	uint32_t nnodes;      /* number of nodes in this namespace              */
	int node_id;          /* relative position of this node in this step	*/
	uint32_t ntasks;      /* total number of tasks in this namespace        */
	uint32_t *task_cnts;  /* Number of tasks on each node in this namespace */
	char *task_map_packed;  /* string represents packed task mapping information */
	uint32_t *task_map;	/* i'th task is located on task_map[i] node     */
	hostlist_t hl;

	/* Current database */
	pmixp_blob_t *local_blobs;
	pmixp_blob_t *remote_blobs;
	pmixp_blob_t *global_blobs;
	// FIXME:
	// 1. do we want to account generations of DB?
	// 2. how will we merge pieces of database?
	// Next generation of database
	/*
	void **blobs_new;
	int *blob_sizes_new;
	uint32_t cur_gen, next_gen; // Data generation
	*/
} pmixp_namespace_t;

typedef struct {
#ifndef NDEBUG
#       define PMIXP_NSPACE_DB_MAGIC 0xCAFEBABE
	int  magic;
#endif
	List nspaces;
} pmixp_db_t;


extern pmixp_db_t _pmixp_nspaces;

/* namespaces list operations */
int pmixp_nspaces_init();
pmixp_namespace_t *pmixp_nspaces_find(const char *name);
int pmixp_nspaces_add(char *name, uint32_t nnodes, int node_id,
			 uint32_t ntasks, uint32_t *task_cnts,
			 char *task_map_packed, hostlist_t hl);

/* operations on the specific namespace */
inline static hostlist_t pmixp_nspace_hostlist(pmixp_namespace_t *nsptr)
{
	hostlist_t hl = hostlist_copy(nsptr->hl);
	return hl;
}
hostlist_t pmixp_nspace_rankhosts(pmixp_namespace_t *nsptr,
				  int *ranks, size_t nranks);
int pmixp_nspace_add_blob(const char *nspace, pmix_scope_t scope, int taskid, void *blob, int size);
int pmixp_nspace_blob(const char *nspace, pmix_scope_t scope, List l);
int pmixp_nspace_rank_blob(const char *nspace, pmix_scope_t scope,
			   int rank, List l);


// TODO: Check the usefulness of this and remove in future.

/*
static inline uint32_t pmix_db_generation(){
	return pmix_db.cur_gen;
}

static inline uint32_t pmix_db_generation_next(){
	return pmix_db.next_gen;
}

static inline uint32_t pmix_db_consistent(){
	return (pmix_db.cur_gen == pmix_db.next_gen);
}

static inline void pmix_db_start_update()
{
	if( pmix_db_consistent() ){
		pmix_db.next_gen++;
		PMIXP_DEBUG("DB: current = %d, next = %d",
				   pmix_db.cur_gen, pmix_db.next_gen);
	}
}


static inline void pmix_db_commit()
{
	int i;
	xassert(pmix_db.magic == PMIXP_NSPACE_MAGIC);

	// Make new database to be current.
	for(i=0; i<pmix_info_tasks(); i++){
		// Drop old blob
		if( pmix_db.blobs[i] != NULL ){
			xfree(pmix_db.blobs[i]);
			pmix_db.blobs[i] = NULL;
			pmix_db.blob_sizes[i] = 0;
		}
		// Save new one
		pmix_db.blobs[i] = pmix_db.blobs_new[i];
		pmix_db.blob_sizes[i] = pmix_db.blob_sizes_new[i];
		// Clear references in new db
		pmix_db.blobs_new[i] = NULL;
		pmix_db.blob_sizes_new[i] = 0;
	}
	// Move entire database forward
	pmix_db.cur_gen = pmix_db.next_gen;
	PMIXP_DEBUG("DB: current = %d, next = %d",
			   pmix_db.cur_gen, pmix_db.next_gen);

}
*/


/*
 * With direct modex we have two cases:
 * 1. if (hdr->gen == cur_gen) we submit into the current DB.
 * 2. if (hdr->gen <> cur_gen) we discard the data
 */
/*
static inline void pmix_db_dmdx_add_blob(uint32_t gen, int taskid, void *blob, int size)
{
	xassert(_pmixp_nspaces.magic == PMIXP_NSPACE_MAGIC);
	if( _pmixp_nspaces.cur_gen == gen ){
		_pmixp_nspaces.blobs[taskid] = blob;
		_pmixp_nspaces.blob_sizes[taskid] = size;
	}
}

static inline int pmix_db_get_blob(int taskid, void **blob)
{
	xassert(_pmixp_nspaces.magic == PMIXP_NSPACE_MAGIC);
	*blob = _pmixp_nspaces.blobs[taskid];
	return _pmixp_nspaces.blob_sizes[taskid];
}
*/
#endif // PMIXP_NSPACES_H
