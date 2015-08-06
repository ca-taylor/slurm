/*****************************************************************************\
 **  pmix_coll.c - PMIx collective primitives
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

#include "pmixp_common.h"
#include "src/slurmd/common/reverse_tree_math.h"
#include "src/common/slurm_protocol_api.h"
#include "pmixp_coll.h"
#include "pmixp_nspaces.h"
#include "pmixp_server.h"

static int _server_hdr_size = 0;

static void _progress_fan_in(pmixp_coll_t *coll);
static int _is_child_no(pmixp_coll_t *coll, int nodeid);

int pmixp_coll_fw_init(char ***env, uint32_t hdrsize)
{
	_server_hdr_size = hdrsize;
	return SLURM_SUCCESS;
}

static int
_hostset_from_ranges(const pmix_proc_t *procs, size_t nprocs,
		     hostlist_t *hl_out)
{
	int i;
	hostlist_t hl = hostlist_create("");
	pmixp_namespace_t *nsptr = NULL;
	for(i=0; i<nprocs; i++){
		char *node = NULL;
		hostlist_t tmp;
		nsptr = pmixp_nspaces_find(procs[i].nspace);
		if( NULL == nsptr ){
			goto err_exit;
		}
		if( procs[i].rank == PMIX_RANK_WILDCARD ){
			tmp = hostlist_copy(nsptr->hl);
		} else {
			tmp = pmixp_nspace_rankhosts(nsptr, &procs[i].rank, 1);
		}
		while( NULL != (node = hostlist_pop(tmp)) ){
			hostlist_push(hl, node);
			free(node);
		}
		hostlist_destroy(tmp);
	}
	hostlist_uniq(hl);
	*hl_out = hl;
	return SLURM_SUCCESS;
err_exit:
	hostlist_destroy(hl);
	return SLURM_ERROR;
}

static void _adjust_data_size(pmixp_coll_t *coll, size_t size)
{
	if( NULL == coll->data ){
		coll->data = xmalloc( size );
		coll->data_sz = size;
		coll->data_pay = 0;
		return;
	}

	// reallocate only if there is a need
	if( coll->data_pay + size > coll->data_sz ){
		// not enough space for the new contribution
		size_t size_new = coll->data_pay + size;
		coll->data_sz = size_new;
		coll->data = xrealloc_nz(coll->data, coll->data_sz);
	}
}

static int _pack_ranges(pmixp_coll_t *coll)
{
	pmix_proc_t *procs = coll->procs;
	size_t nprocs = coll->nprocs;
	Buf buf;
	uint32_t size = _server_hdr_size + 2*sizeof(uint32_t);
	int i;

	buf = create_buf(xmalloc(size), size);
	// Skip header. It will be filled right before the sending
	set_buf_offset(buf, _server_hdr_size);

	// 1. store the type of collective
	size = coll->type;
	pack32(size,buf);

	// 2. Put the number of ranges
	pack32(nprocs, buf);
	for(i=0; i< (int)nprocs; i++){
		// Count cumulative size
		size = 0;
		size += strlen(procs->nspace) + sizeof(uint32_t);
		size += sizeof(uint32_t);
		grow_buf(buf, size);
		// Pack namespace
		packmem(procs->nspace, strlen(procs->nspace), buf);
		pack32(procs->rank, buf);
	}

	coll->data = buf->head;
	coll->data_sz = size_buf(buf);
	coll->data_pay = get_buf_offset(buf);

	/* free buffer protecting data */
	buf->head = NULL;
	free_buf(buf);

	return SLURM_SUCCESS;
}

int pmixp_coll_unpack_ranges(void *data, size_t size,
			     pmixp_coll_type_t *type,
			     pmix_proc_t **r, size_t *nr)
{
	pmix_proc_t *procs = NULL;
	uint32_t nprocs = 0;
	Buf buf;
	uint32_t tmp;
	int i, rc;

	size = (size < MAX_PACK_MEM_LEN) ? size : MAX_PACK_MEM_LEN;

	buf = create_buf(data, size);

	// 1. extract the type of collective
	if( SLURM_SUCCESS != ( rc = unpack32(&tmp, buf))){
		PMIXP_ERROR("Cannot unpack collective type");
		return rc;
	}
	*type = tmp;

	// 2. get the number of ranges
	if( SLURM_SUCCESS != ( rc = unpack32(&nprocs, buf))){
		PMIXP_ERROR("Cannot unpack collective type");
		return rc;
	}
	*nr = nprocs;

	procs = xmalloc(sizeof(pmix_proc_t) * (nprocs));
	*r = procs;

	for(i=0; i< (int)nprocs; i++){
		// 3. get namespace/rank of particular process
		rc = unpackmem(procs[i].nspace, &tmp, buf);
		if( SLURM_SUCCESS != rc ){
			PMIXP_ERROR("Cannot unpack namespace for process #%d", i);
			return rc;
		}
		procs[i].nspace[tmp] = '\0';

		unsigned int tmp;
		rc = unpack32(&tmp, buf);
		procs[i].rank = tmp;
		if( SLURM_SUCCESS != rc ){
			PMIXP_ERROR("Cannot unpack ranks for process #%d, nsp=%s",
				    i, procs[i].nspace);
			return rc;
		}
	}
	rc = get_buf_offset(buf);
	buf->head = NULL;
	free_buf(buf);
	return rc;
}

int pmixp_coll_belong_chk(pmixp_coll_type_t type, const pmix_proc_t *procs,
			  size_t nprocs)
{
	int i;
	pmixp_namespace_t *nsptr = pmixp_nspaces_local();
	// Find my namespace in the range
	for(i=0; i < nprocs; i++){
		if( 0 != strcmp(procs[i].nspace, nsptr->name) ){
			continue;
		}
		if( (procs[i].rank == PMIX_RANK_WILDCARD) )
			return 0;
		if( 0 <= pmixp_info_taskid2localid(procs[i].rank) ){
			return 0;
		}
	}
	// we don't participate in this collective!
	PMIXP_ERROR("Have collective that doesn't include this job's namespace");
	return -1;
}


/*
 * Based on ideas provided by Hongjia Cao <hjcao@nudt.edu.cn> in PMI2 plugin
 */
int pmixp_coll_init(pmixp_coll_t *coll, const pmix_proc_t *procs, size_t nprocs,
		    pmixp_coll_type_t type)
{
	hostlist_t hl;
	uint32_t nodeid = 0, nodes = 0;
	int parent_id, depth, max_depth, tmp;
	int width, my_nspace = -1;
	char *p;

#ifndef NDEBUG
	coll->magic = PMIXP_COLL_STATE_MAGIC;
#endif
	coll->type = type;
	coll->state = PMIXP_COLL_SYNC;
	coll->procs = xmalloc( sizeof(*procs) * nprocs);
	memcpy(coll->procs, procs, sizeof(*procs) * nprocs);
	coll->nprocs = nprocs;
	coll->my_nspace = my_nspace;

	if( SLURM_SUCCESS != _hostset_from_ranges(procs, nprocs, &hl) ){
		// TODO: provide ranges output routine
		PMIXP_ERROR("Bad ranges information");
		goto err_exit;
	}

	width = slurm_get_tree_width();
	nodes = hostlist_count(hl);
	nodeid = hostlist_find(hl,pmixp_info_hostname());
	reverse_tree_info(nodeid, nodes, width, &parent_id, &tmp,
			  &depth, &max_depth);
	coll->children_cnt = tmp;
	coll->nodeid = nodeid;

	// We interested in amount of direct childs
	coll->seq = 0;
	coll->contrib_cntr = 0;
	coll->contrib_local = false;
	coll->ch_nodeids = xmalloc( sizeof(int) * width );
	coll->ch_contribs = xmalloc( sizeof(int) * width );
	coll->children_cnt = reverse_tree_direct_children(nodeid, nodes, width,
							  depth, coll->ch_nodeids);

	if( parent_id == -1 ){
		coll->parent_host = NULL;
	} else if( parent_id >= 0 ){
		p = hostlist_nth(hl, parent_id);
		coll->parent_host = xstrdup(p);
		free(p);
	}
	coll->all_children = hl;

	/* Collective data */
	coll->data = NULL;
	coll->data_sz = 0;
	coll->data_pay = 0;
	if( SLURM_SUCCESS != _pack_ranges(coll) ){
		PMIXP_ERROR("Cannot pack ranges to coll message header!");
		goto err_exit;
	}

	/* Callback information */
	coll->cbdata = NULL;
	coll->cbfunc = NULL;

	/* init fine grained lock */
	pthread_mutexattr_t attr;
	pthread_mutexattr_init(&attr);
	pthread_mutex_init(&coll->lock, &attr);
	pthread_mutexattr_destroy(&attr);

	return SLURM_SUCCESS;
err_exit:
	return SLURM_ERROR;
}

int pmixp_coll_contrib_local(pmixp_coll_t *coll, char *data, size_t size)
{
	/* sanity check */
	pmixp_coll_sanity_check(coll);

	/* lock the structure */
	pthread_mutex_lock(&coll->lock);

	/* change the collective state if need */
	if( PMIXP_COLL_SYNC == coll->state ){
		coll->state = PMIXP_COLL_FAN_IN;
	}
	xassert( PMIXP_COLL_FAN_IN == coll->state);

	_adjust_data_size(coll, size);
	memcpy(coll->data + coll->data_pay, data, size);
	coll->data_pay += size;
	coll->contrib_local = true;

	/* unlock the structure */
	pthread_mutex_unlock(&coll->lock);

	/* check if the collective is ready to progress */
	_progress_fan_in(coll);

	return SLURM_SUCCESS;
}

int pmixp_coll_contrib_node(pmixp_coll_t *coll, char *nodename,
			    void *contrib, size_t size)
{
	int idx;
	int nodeid;

	pmixp_coll_sanity_check(coll);

	/* lock the structure */
	pthread_mutex_lock(&coll->lock);

	/* fix the collective status if need */
	if( PMIXP_COLL_SYNC == coll->state ){
		coll->state = PMIXP_COLL_FAN_IN;
	}
	xassert( PMIXP_COLL_FAN_IN == coll->state);

	/* Because of possible timeouts/delays in transmission we
     * can receive a contribution second time. Avoid duplications
     * by checking our records. */
	nodeid = hostlist_find(coll->all_children, nodename);
	idx = _is_child_no(coll, nodeid);
	xassert( 0 <= idx );
	if( 0 < coll->ch_contribs[idx]){
		/* shouldn't be grater than 1! */
		xassert(1 == coll->ch_contribs[idx]);
		/* this is duplication, skip. */
		return SLURM_SUCCESS;
	}

	_adjust_data_size(coll, size);
	memcpy(coll->data + coll->data_pay, contrib, size);
	coll->data_pay += size;

	/* increase number of individual contributions */
	coll->ch_contribs[idx]++;

	/* increase number of total contributions */
	coll->contrib_cntr++;

	/* unlock the structure */
	pthread_mutex_unlock(&coll->lock);

	/* make a progress */
	_progress_fan_in(coll);

	return SLURM_SUCCESS;
}

static int _is_child_no(pmixp_coll_t *coll, int nodeid)
{
	// Check for initialization
	pmixp_coll_sanity_check(coll);
	int i;
	for(i=0;i< coll->children_cnt; i++){
		if( nodeid == coll->ch_nodeids[i] ){
			return i;
		}
	}
	return -1;
}

void _progress_fan_in(pmixp_coll_t *coll)
{
	pmixp_srv_cmd_t type;
	const char *addr = pmixp_info_srv_addr();
	char *hostlist = NULL;
	int rc;

	pmixp_coll_sanity_check(coll);

	/* lock the */
	pthread_mutex_lock(&coll->lock);

	if( PMIXP_COLL_FAN_IN != coll->state ){
		/* In case of race condition between libpmix and
	 * slurm threads progress_fan_in can be called
	 * after we moved to the next step. */
		goto exit;
	}

	if( !coll->contrib_local || coll->contrib_cntr != coll->children_cnt ){
		/* Not yet ready to go to the next step */
		goto exit;
	}

	/* The root of the collective will have parent_host == NULL */
	if( NULL != coll->parent_host ){
		hostlist = xstrdup(coll->parent_host);
		type = PMIXP_MSG_FAN_IN;
	} else {
		hostlist = hostlist_ranged_string_xmalloc(coll->all_children);
		type = PMIXP_MSG_FAN_OUT;
	}

	rc = pmixp_server_send(hostlist, type, coll->seq, addr,
			       coll->data, coll->data_pay);
	xfree(hostlist);

	if( SLURM_SUCCESS != rc ){
		PMIXP_ERROR("Cannot send database (size = %lu), to hostlist:\n%s",
			    (uint64_t)coll->data_pay, hostlist);
		// TODO: abort the whole application here?
		// Currently it will just hang!
		return;
	}

	coll->state = PMIXP_COLL_FAN_OUT;
	xfree(coll->data);
	coll->data = NULL;
	coll->data_pay = coll->data_sz = 0;

exit:
	/* lock the */
	pthread_mutex_unlock(&coll->lock);
}

void pmixp_coll_fan_out_data(pmixp_coll_t *coll, void *data,
			     uint32_t size)
{
	pmixp_coll_sanity_check(coll);

	xassert( PMIXP_COLL_FAN_OUT == coll->state );

	/* lock the structure */
	pthread_mutex_lock(&coll->lock);

	// update the database
	if( NULL != coll->cbfunc ){
		coll->cbfunc(PMIX_SUCCESS, data, size, coll->cbdata);
	}

	// Prepare for the next collective operation
	coll->state = PMIXP_COLL_SYNC;
	memset(coll->ch_contribs, 0, sizeof(int) * coll->children_cnt);
	coll->seq++; /* move to the next collective */
	coll->contrib_cntr = 0;

	/* lock the structure */
	pthread_mutex_lock(&coll->lock);
}
