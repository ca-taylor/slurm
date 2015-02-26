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

#ifndef PMIX_DB_H
#define PMIX_DB_H

#include "pmixp_common.h"
#include "pmixp_info.h"
#include "pmixp_debug.h"
#include "pmixp_state.h"

typedef struct {
#ifndef NDEBUG
#       define PMIX_MSGSTATE_MAGIC 0xdeadbeef
	int  magic;
#endif
	uint32_t cur_gen, next_gen; // Data generation
	// Current database
	void **blobs;
	int *blob_sizes;
	// Next generation of database
	void **blobs_new;
	int *blob_sizes_new;
} pmix_db_t;

extern pmix_db_t pmix_db;

static inline void pmix_db_init()
{
	uint32_t tasks = pmix_info_tasks();
	uint32_t bsize = sizeof(int*) * tasks;
	uint32_t ssize = sizeof(int) * tasks;

#ifndef NDEBUG
	pmix_db.magic = PMIX_MSGSTATE_MAGIC;
#endif
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
}

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
		PMIX_DEBUG("DB: current = %d, next = %d",
				   pmix_db.cur_gen, pmix_db.next_gen);
	}
}


static inline void pmix_db_commit()
{
	int i;
	xassert(pmix_db.magic == PMIX_MSGSTATE_MAGIC);

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
	PMIX_DEBUG("DB: current = %d, next = %d",
			   pmix_db.cur_gen, pmix_db.next_gen);

}

static inline void pmix_db_add_blob(int taskid, void *blob, int size)
{
	xassert(pmix_db.magic == PMIX_MSGSTATE_MAGIC);
	xassert( !pmix_db_consistent() );

	// check that we update data incrementally with step = 1
	if( NULL != pmix_db.blobs_new[taskid] ){
		// This theoretically shouldn't happen.
		// WARN and accept the new blob and remove the old one
		PMIX_ERROR_NO(0,"NEW blob for task %d was rewritten!", taskid);
		xfree(pmix_db.blobs_new[taskid]);
		pmix_db.blobs_new[taskid] = NULL;
	}
	pmix_db.blobs_new[taskid] = blob;
	pmix_db.blob_sizes_new[taskid] = size;
}

/*
 * With direct modex we have two cases:
 * 1. if (hdr->gen == cur_gen) we submit into the current DB.
 * 2. if (hdr->gen <> cur_gen) we discard the data
 */
static inline void pmix_db_dmdx_add_blob(uint32_t gen, int taskid, void *blob, int size)
{
	xassert(pmix_db.magic == PMIX_MSGSTATE_MAGIC);
	if( pmix_db.cur_gen == gen ){
		pmix_db.blobs[taskid] = blob;
		pmix_db.blob_sizes[taskid] = size;
	}
}

static inline int pmix_db_get_blob(int taskid, void **blob)
{
	xassert(pmix_db.magic == PMIX_MSGSTATE_MAGIC);
	*blob = pmix_db.blobs[taskid];
	return pmix_db.blob_sizes[taskid];
}

#endif // PMIX_DB_H
