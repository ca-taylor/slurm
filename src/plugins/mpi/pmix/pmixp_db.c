/*****************************************************************************\
 **  pmix_db.c - PMIx KVS database
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
#include "pmixp_db.h"

pmixp_db_t _pmixp_db;

static void _xfree_nspace(void *n)
{
	pmixp_namespace_t *nsptr = n;
	xfree(nsptr->ntasks);
	xfree(nsptr->task_map);
	xfree(nsptr->task_map_packed);
	xfree(nsptr->local_blobs);
	xfree(nsptr->remote_blobs);
	xfree(nsptr->global_blobs);
	xfree(nsptr);
}

int pmixp_nspaces_init()
{
	char *mynspace, *task_map;
	uint32_t nnodes, ntasks, *task_cnts;
	int nodeid, rc;
	hostlist_t hl;

#ifndef NDEBUG
	_pmixp_db.magic = PMIXP_DB_MAGIC;
#endif
	_pmixp_db.nspaces = list_create(_xfree_nspace);
	mynspace = pmixp_info_namespace();
	nnodes = pmixp_info_nodes();
	nodeid = pmixp_info_nodeid();
	ntasks = pmixp_info_tasks();
	task_cnts = pmixp_info_tasks_cnts();
	task_map = pmixp_info_task_map();
	hl = pmixp_info_step_hostlist();
	// Initialize the namespace
	rc = pmixp_nspaces_add(mynspace, nnodes, nodeid, ntasks,
				   task_cnts, task_map, hostlist_copy(hl));
	return rc;
}

int pmixp_nspaces_add(char *name, uint32_t nnodes, int node_id,
			 uint32_t ntasks, uint32_t *task_cnts,
			 char *task_map_packed, hostlist_t hl)
{
	size_t size;
	pmixp_namespace_t *nsptr = xmalloc(sizeof(pmixp_namespace_t));
	int i;

	/* fill up informational part */
	strcpy(nsptr->name, name);
	nsptr->nnodes = nnodes;
	nsptr->node_id = node_id;
	nsptr->ntasks = ntasks;
	nsptr->task_cnts = xmalloc( sizeof(uint32_t) * nnodes );
	// Cannot use memcpy here because of different types
	for(i=0; i<nnodes; i++){
		nsptr->task_cnts[i] = task_cnts[i];
	}
	nsptr->task_map_packed = xstrdup(task_map_packed);
	nsptr->task_map = unpack_process_mapping_flat(task_map_packed, nnodes,
						      ntasks, NULL);
	if(nsptr->task_map == NULL ){
		xfree(nsptr->task_cnts);
		return SLURM_ERROR;
	}
	nsptr->hl = hl;

	/* prepare data storage */
	size = sizeof(pmixp_blob_t) * ntasks;
	nsptr->local_blobs = xmalloc(size);
	memset(nsptr->local_blobs, 0, size);
	nsptr->remote_blobs = xmalloc(size);
	memset(nsptr->remote_blobs, 0, size);
	nsptr->global_blobs = xmalloc(size);
	memset(nsptr->global_blobs, 0, size);
	list_append(_pmixp_db.nspaces, nsptr);
	return SLURM_SUCCESS;
}

pmixp_namespace_t *
pmixp_nspaces_find(const char *name)
{
	ListIterator it = list_iterator_create(_pmixp_db.nspaces);
	pmixp_namespace_t *nsptr = NULL;
	while( NULL != (nsptr = list_next(it))){
		if( 0 == strcmp(nsptr->name, name) ){
			goto exit;
		}
	}
	// Didn't found one!
	nsptr = NULL;
exit:
	return nsptr;
}

hostlist_t pmixp_nspace_rankhosts(pmixp_namespace_t *nsptr,
				  int *ranks, size_t nranks)
{
	hostlist_t hl = hostlist_create("");
	int i;
	for(i=0; i<nranks; i++){
		int rank = ranks[i];
		int node = nsptr->task_map[rank];
		char *node_s = hostlist_nth(nsptr->hl, node);
		hostlist_push(hl, node_s);
		free(node_s);
	}
	hostlist_uniq(hl);
	return hl;
}

int pmixp_nspace_add_blob(const char *nspace, pmix_scope_t scope, int taskid, void *blob, int size)
{
	pmixp_namespace_t *nsptr;
	pmixp_blob_t *blob_ptr;
	xassert(_pmixp_db.magic == PMIXP_NSPACE_MAGIC);
	ListIterator it = list_iterator_create(_pmixp_db.nspaces);
	while( NULL != (nsptr = list_next(it)) ){
		if( 0 == strcmp(nsptr->name, nspace) ){
			break;
		}
	}

	if( NULL == nsptr ){
		PMIXP_DEBUG("Trying to add blob to unknown namespace %s", nspace);
		return SLURM_ERROR;
	}

	xassert(nsptr->ntasks > taskid);

	switch( scope ){
	case PMIX_LOCAL:
		blob_ptr = &nsptr->local_blobs[taskid];
		break;
	case PMIX_REMOTE:
		blob_ptr = &nsptr->remote_blobs[taskid];
		break;
	case PMIX_GLOBAL:
		blob_ptr = &nsptr->global_blobs[taskid];
		break;
	default:
		PMIXP_ERROR_STD("Cannot add blob for %d scope",scope);
		return SLURM_ERROR;
	}

	if( 0 < blob_ptr->blob_sz ){
		// Should we merge db's here?
		// by now just warn
		PMIXP_ERROR_STD("Double blob submission for nspace=%s, taskid = %d",
			   nspace, taskid);
		xfree(blob_ptr->blob);
		blob_ptr->blob = NULL;
		blob_ptr->blob_sz = 0;
	}
	blob_ptr->blob_sz = size;
	blob_ptr->blob = xmalloc(size);
	memcpy(blob_ptr->blob, blob, size);
	return SLURM_SUCCESS;
}


static void _add_modex_data(const char *nspace, pmix_scope_t scope, pmixp_blob_t *ptr,
			    int rank, List modex_data)
{
	if( 0 < ptr->blob_sz ){
		pmixp_modex_t *m = xmalloc( sizeof(*m) );
		m->data.blob = ptr->blob;
		m->data.size = ptr->blob_sz;
		m->data.rank = rank;
		strcpy(m->data.nspace, nspace);
		m->scope = scope;
		list_append(modex_data, m);
	}
}

static void _nspace_modex_data(const char *nspace, pmix_scope_t scope,
			       pmixp_blob_t *ptr, int cnt,  List modex_data)
{
	int i;
	for(i=0; i<cnt; i++){
		_add_modex_data(nspace, scope, ptr, i, modex_data);
	}
}

int pmixp_nspace_blob(const char *nspace, pmix_scope_t scope, List l)
{
	pmixp_namespace_t *nsptr;

	xassert(_pmixp_db.magic == PMIXP_NSPACE_MAGIC);
	ListIterator it = list_iterator_create(_pmixp_db.nspaces);
	while( NULL != (nsptr = list_next(it)) ){
		if( 0 == strcmp(nsptr->name, nspace) ){
			break;
		}
	}

	if( NULL == nsptr ){
		return SLURM_ERROR;
	}

	_nspace_modex_data(nspace, scope, nsptr->local_blobs,
			 nsptr->ntasks, l);
	return SLURM_SUCCESS;
}

int pmixp_nspace_rank_blob(const char *nspace, pmix_scope_t scope,
			   int rank, List l)
{
	pmixp_namespace_t *nsptr;

	xassert(_pmixp_db.magic == PMIXP_NSPACE_MAGIC);
	ListIterator it = list_iterator_create(_pmixp_db.nspaces);
	while( NULL != (nsptr = list_next(it)) ){
		if( 0 == strcmp(nsptr->name, nspace) ){
			break;
		}
	}

	if( NULL == nsptr ){
		return SLURM_ERROR;
	}

	xassert( rank < nsptr->ntasks );

	_add_modex_data(nspace, scope, &nsptr->local_blobs[rank],
			rank, l);

	return SLURM_SUCCESS;
}
