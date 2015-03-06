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
	pmixp_namespace_t *nspace = n;
	xfree(nspace->local_blobs);
	xfree(nspace->remote_blobs);
	xfree(nspace->global_blobs);
	xfree(nspace);
}

void pmixp_db_init(char *nspace)
{
	uint32_t tasks = pmix_info_tasks();
	size_t size;
#ifndef NDEBUG
	_pmixp_db.magic = PMIXP_DB_MAGIC;
#endif
	_pmixp_db.nspaces = slurm_list_create(_xfree_nspace);

	pmixp_namespace_t *nsptr = xmalloc(sizeof(pmixp_namespace_t));
#ifndef NDEBUG
	nsptr->magic = PMIXP_NSPACE_MAGIC;
#endif
	strcpy(nsptr->name, nspace);
	size = sizeof(pmixp_blob_t) * tasks;
	nsptr->tasks = tasks;
	// TODO: do we need local blobs here?
	// it can be solely be handled by libpmix.
	nsptr->local_blobs = xmalloc(size);
	memset(nsptr->local_blobs, 0, size);
	nsptr->remote_blobs = xmalloc(size);
	memset(nsptr->remote_blobs, 0, size);
	nsptr->global_blobs = xmalloc(size);
	memset(nsptr->global_blobs, 0, size);
	slurm_list_append(_pmixp_db.nspaces, nsptr);

/*
	uint32_t bsize = sizeof(int*) * tasks;
	uint32_t ssize = sizeof(int) * tasks;

	pmix_db.cur_gen = pmix_db.next_gen = 0;

	// Setup "current" Databse
	pmix_db.blobs = xmalloc( bsize );
	memset(pmix_db.blobs, 0, bsize);
	pmix_db.blob_sizes = xmalloc( ssize );
	memset(pmix_db.blob_sizes, 0, ssize);

	// Setup new database
	pmix_db.blobs_new = xmalloc( bsize );
	memset(pmix_db.blobs_new, 0, bsize);
	pmix_db.blob_sizes_new = xmalloc( ssize );
	memset(pmix_db.blob_sizes_new, 0, ssize);
*/
}

int pmixp_db_add_blob(const char *nspace, pmix_scope_t scope, int taskid, void *blob, int size)
{
	pmixp_namespace_t *nsptr;
	pmixp_blob_t *blob_ptr;
	xassert(_pmixp_db.magic == PMIXP_NSPACE_MAGIC);
	ListIterator it = slurm_list_iterator_create(_pmixp_db.nspaces);
	while( NULL != (nsptr = slurm_list_next(it)) ){
		if( 0 == strcmp(nsptr->name, nspace) ){
			break;
		}
	}

	if( NULL == nsptr ){
		PMIXP_DEBUG("Trying to add blob to unknown namespace %s", nspace);
		return SLURM_ERROR;
	}

	xassert(nsptr->tasks > taskid);

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
		PMIXP_ERROR("Cannot add blob for %d scope",scope);
		return SLURM_ERROR;
	}

	if( 0 < blob_ptr->blob_sz ){
		// Should we merge db's here?
		// by now just warn
		PMIXP_ERROR("Double blob submission for nspace=%s, taskid = %d",
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


static void _add_modex_data(const char *nspace, pmixp_blob_t *ptr,
			    int rank, List modex_data)
{
	if( 0 < ptr->blob_sz ){
		pmix_modex_data_t *m = xmalloc( sizeof(*m) );
		m->blob = ptr->blob;
		m->size = ptr->blob_sz;
		m->rank = rank;
		strcpy(m->nspace, nspace);
		slurm_list_append(modex_data, m);
	}
}

static void _nspace_modex_data(const char *nspace, pmixp_blob_t *ptr,
			     int cnt, List modex_data)
{
	int i;
	for(i=0; i<cnt; i++){
		_add_modex_data(nspace, ptr, i, modex_data);
	}
}

int pmixp_db_blob(const char *nspace, List modex_data)
{
	pmixp_namespace_t *nsptr;

	xassert(_pmixp_db.magic == PMIXP_NSPACE_MAGIC);
	ListIterator it = slurm_list_iterator_create(_pmixp_db.nspaces);
	while( NULL != (nsptr = slurm_list_next(it)) ){
		if( 0 == strcmp(nsptr->name, nspace) ){
			break;
		}
	}

	if( NULL == nsptr ){
		return SLURM_ERROR;
	}

	_nspace_modex_data(nspace, nsptr->global_blobs,
			 nsptr->tasks, modex_data);
	_nspace_modex_data(nspace, nsptr->local_blobs,
			 nsptr->tasks, modex_data);
	_nspace_modex_data(nspace, nsptr->remote_blobs,
			 nsptr->tasks, modex_data);
	return SLURM_SUCCESS;
}

int pmixp_db_blob_r(const char *nspace, int rank, List modex_data)
{
	pmixp_namespace_t *nsptr;

	xassert(_pmixp_db.magic == PMIXP_NSPACE_MAGIC);
	ListIterator it = slurm_list_iterator_create(_pmixp_db.nspaces);
	while( NULL != (nsptr = slurm_list_next(it)) ){
		if( 0 == strcmp(nsptr->name, nspace) ){
			break;
		}
	}

	if( NULL == nsptr ){
		return SLURM_ERROR;
	}

	xassert( rank < nsptr->tasks );

	_add_modex_data(nspace, &nsptr->global_blobs[rank], rank, modex_data);
	_add_modex_data(nspace, &nsptr->local_blobs[rank], rank, modex_data);
	_add_modex_data(nspace, &nsptr->remote_blobs[rank], rank, modex_data);

	return SLURM_SUCCESS;
}
