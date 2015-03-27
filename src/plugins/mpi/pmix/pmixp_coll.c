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
static void _progress_fan_out(pmixp_coll_t *coll);
static int _is_child_no(pmixp_coll_t *coll, int nodeid);


int pmixp_coll_init(char ***env, uint32_t hdrsize)
{
	_server_hdr_size = hdrsize;
	return SLURM_SUCCESS;
}

static int
_hostset_from_ranges(const pmix_range_t *ranges, size_t nranges, hostlist_t *hl_out)
{
	int i;
	hostlist_t hl = hostlist_create("");
	pmixp_namespace_t *nsptr = NULL;
	for(i=0; i<nranges; i++){
		char *node = NULL;
		hostlist_t tmp;
		nsptr = pmixp_nspaces_find(ranges[i].nspace);
		if( NULL == nsptr ){
			goto err_exit;
		}
		if( 0 == ranges[i].nranks){
			/* use all nodes in the namespace */
			tmp = pmixp_nspace_hostlist(nsptr);
		} else {
			tmp = pmixp_nspace_rankhosts(nsptr, ranges[i].ranks,
						     ranges[i].nranks);
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
	pmix_range_t *ranges = coll->ranges;
	size_t nranges = coll->nranges;
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
	pack32(nranges, buf);
	for(i=0; i< (int)nranges; i++){
		uint32_t *ranks_tmp = NULL;
		int j;
		// Count cumulative size
		size = 0;
		size += strlen(ranges->nspace) + sizeof(uint32_t);
		size += ranges->nranks * sizeof(int) + sizeof(uint32_t);
		grow_buf(buf, size);
		// Pack namespace
		packmem(ranges->nspace, strlen(ranges->nspace), buf);
		// Pack ranks (with tmp array for type conversion)
		// TODO: can this be done better?
		ranks_tmp = xmalloc(ranges->nranks * sizeof(uint32_t));
		for(j=0; j<ranges->nranks; j++){
			ranks_tmp[j] = (uint32_t)ranges->ranks[j];
		}
		pack32_array(ranks_tmp, ranges->nranks, buf);
		xfree(ranks_tmp);
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
			       pmix_range_t **r, size_t *nr)
{
	pmix_range_t *ranges = NULL;
	uint32_t nranges = 0;
	Buf buf;
	uint32_t tmp;
	int i, rc;

	size = (size < MAX_PACK_MEM_LEN) ? size : MAX_PACK_MEM_LEN;

	buf = create_buf(data, size);

	// 1. store the type of collective
	if( SLURM_SUCCESS != ( rc = unpack32(&tmp, buf))){
		PMIXP_ERROR("Cannot unpack collective type");
		return rc;
	}
	*type = tmp;

	// 2. Put the number of ranges
	if( SLURM_SUCCESS != ( rc = unpack32(&nranges, buf))){
		PMIXP_ERROR("Cannot unpack collective type");
		return rc;
	}
	*nr = nranges;

	ranges = xmalloc(sizeof(pmix_range_t) * (nranges));
	*r = ranges;

	for(i=0; i< (int)nranges; i++){
		uint32_t *ranks_tmp = NULL;
		uint32_t nranks_tmp;
		int j;
		// 2. Put the number of ranges
		rc = unpackmem(ranges[i].nspace, &tmp, buf);
		if( SLURM_SUCCESS != rc ){
			PMIXP_ERROR("Cannot unpack namespace for range #%d", i);
			return rc;
		}
		ranges[i].nspace[tmp] = '\0';

		rc = unpack32_array(&ranks_tmp, &nranks_tmp, buf);
		if( SLURM_SUCCESS != rc ){
			PMIXP_ERROR("Cannot unpack ranks for range #%d, nsp=%s",
				    i, ranges[i].nspace);
			return rc;
		}
		ranges[i].nranks = nranks_tmp;
		ranges[i].ranks = NULL;
		if( 0 < nranks_tmp ){
			ranges[i].ranks = xmalloc_nz(sizeof(int)*nranks_tmp);
			for(j=0; j<nranks_tmp; j++){
				ranges[i].ranks[j] = ranks_tmp[j];
			}
		}
		xfree(ranks_tmp);
	}
	rc = get_buf_offset(buf);
	buf->head = NULL;
	free_buf(buf);
	return rc;
}

/*
 * Based on ideas provided by Hongjia Cao <hjcao@nudt.edu.cn> in PMI2 plugin
 */
pmixp_coll_t *pmixp_coll_new(const pmix_range_t *ranges, size_t nranges,
			     pmixp_coll_type_t type)
{
	pmixp_coll_t *coll;
	hostlist_t hl;
	uint32_t nodeid = 0, nodes = 0;
	int parent_id, depth, max_depth, tmp;
	int width, i;
	int my_nspace = -1;
	char *p;

	// Find my namespace in the range
	for(i=0; i< nranges; i++){
		if( 0 == strcmp(ranges[i].nspace, pmixp_info_namespace()) ){
			my_nspace = i;
			break;
		}
	}
	if( 0 > my_nspace ){
		// we don't participate in this collective!
		PMIXP_ERROR("Have collective that doesn't include this job's namespace");
		return NULL;
	}

	coll = xmalloc( sizeof(*coll) );
#ifndef NDEBUG
	coll->magic = PMIXP_COLL_STATE_MAGIC;
#endif
	coll->type = type;
	coll->state = PMIXP_COLL_SYNC;
	coll->ranges = xmalloc( sizeof(*ranges) * nranges);
	memcpy(coll->ranges, ranges, sizeof(*ranges) * nranges);
	coll->nranges = nranges;
	coll->my_nspace = my_nspace;

	if( SLURM_SUCCESS != _hostset_from_ranges(ranges, nranges, &hl) ){
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
	coll->rcvframe_no = 0;
	coll->local_ok = false;
	coll->contrib_cntr = 0;
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
	return coll;
err_exit:
	if( NULL != coll->ranges ){
		xfree(coll->ranges);
	}
	xfree(coll);
	return NULL;
}

static size_t _modex_list_size(List l)
{
	ListIterator it = list_iterator_create(l);
	pmix_modex_data_t *data;
	size_t ret = 0;

	while( NULL != (data = list_next(it) ) ){
		// we need to save:
		// - rank (uint32_t)
		// - scope (uint32_t)
		// - size of the blob (uint32_t)
		// - blob data (data->size)
		ret += data->size + 3*sizeof(int);
	}
	list_iterator_destroy(it);
	return ret;
}

static int _collect_range(pmix_range_t *range, pmix_scope_t scope, List l)
{
	int rc;
	pmixp_namespace_t *nsptr;
	nsptr = pmixp_nspaces_find(range->nspace);
	if( NULL == nsptr ){
		PMIXP_ERROR("Cannot find namespace %s", range->nspace);
		return SLURM_ERROR;
	}
	if( 0 == range->nranks ){
		rc = pmixp_nspace_blob(range->nspace, scope, l);
		if( SLURM_SUCCESS != rc ){
			PMIXP_ERROR("Cannot get blob from nsp=%s, scope=%d",
				    range->nspace, scope);
			return SLURM_ERROR;
		}
	} else {
		int j;
		for(j=0; j<range->nranks; j++){
			int rank = range->ranks[j];
			rc = pmixp_nspace_rank_blob(range->nspace, scope,
						    rank, l);
			if( SLURM_SUCCESS != rc ){
				PMIXP_ERROR("Cannot get blob from nsp=%s, scope=%d, rank=%d",
					    range->nspace, scope, rank);
				return SLURM_ERROR;
			}
		}
	}
	return SLURM_SUCCESS;
}

/* extract only local ranks for modex submission */
static void
_fill_local_ranks(pmix_range_t *glob, pmix_range_t *loc)
{
	int i;
	strcpy(loc->nspace, glob->nspace);
	loc->ranks = xmalloc(sizeof(int) * pmixp_info_tasks_loc());
	loc->nranks = 0;
	if( 0 < glob->nranks ){
		/* selected ranks */
		for(i=0; i < glob->nranks; i++ ){
			int j;
			for(j=0; j<pmixp_info_tasks_loc();j++){
				if( glob->ranks[i] == pmixp_info_taskid(j) ){
					loc->ranks[loc->nranks++] = glob->ranks[i];
				}
			}
		}
	} else {
		int j;
		/* all ranks */
		for(j=0; j < pmixp_info_tasks_loc(); j++ ){
			loc->ranks[loc->nranks++]  = pmixp_info_taskid(j);
		}
	}
}

int pmixp_coll_contrib_loc(pmixp_coll_t *coll, int collect_data)
{
	List modex_list;
	ListIterator it;
	pmixp_modex_t *pmdx;
	size_t size, modex_size;
	Buf buf;
	int rc;
	pmix_range_t range, *mynsp = NULL;

	/* sanity check */
	pmixp_coll_sanity_check(coll);
	if( PMIXP_COLL_SYNC == coll->state ){
		coll->state = PMIXP_COLL_FAN_IN;
	}
	xassert( PMIXP_COLL_FAN_IN == coll->state);

	/* Check that mynamespace is set correctly */
	xassert(coll->my_nspace < coll->nranges );
	mynsp = &coll->ranges[coll->my_nspace];
	xassert( 0 == strcmp(mynsp->nspace, pmixp_info_namespace()) );

	if( !collect_data ){
		/* If we don't collect the data - we just
		 * put 0 as contribution count */
		size = sizeof(uint32_t);
		_adjust_data_size(coll, size);
		buf = create_buf(coll->data + coll->data_pay, coll->data_sz - coll->data_pay);
		pack32(0, buf);
		return SLURM_SUCCESS;
	}
	/* user wants to collect the data */
	modex_list = list_create(pmixp_xfree_buffer);

	_fill_local_ranks(mynsp, &range);

	if( SLURM_SUCCESS != (rc = _collect_range(&range,PMIX_GLOBAL, modex_list) ) ){
		goto err_exit;
	}
	if( SLURM_SUCCESS != (rc = _collect_range(&range,PMIX_REMOTE, modex_list) ) ){
		goto err_exit;
	}

	// Reserve the space for namespace and it's size
	size = strlen(range.nspace) + sizeof(uint32_t);
	// Reserve the space for modex count and size
	size += 2*sizeof(uint32_t);
	// Account the datasize
	modex_size = _modex_list_size(modex_list);
	size += modex_size;

	_adjust_data_size(coll, size);

	buf = create_buf(coll->data + coll->data_pay, coll->data_sz - coll->data_pay);

	pack32(list_count(modex_list), buf);
	packmem(range.nspace,strlen(range.nspace),buf);
	pack32(modex_size, buf);

	it = list_iterator_create(modex_list);
	while( NULL != ( pmdx = list_next(it))){
		uint32_t tmp;
		// Make sure that we correctly allocate the buffer
		xassert( remaining_buf(buf) >= (pmdx->data.size + 3*sizeof(int)) );
		tmp = pmdx->data.rank;
		pack32(tmp, buf);
		tmp = pmdx->scope;
		pack32(tmp, buf);
		packmem((char*)pmdx->data.blob, (uint32_t)pmdx->data.size, buf);
	}

	list_iterator_destroy(it);
	coll->data_pay += get_buf_offset(buf);
	// Protect data and free the buffer
	buf->head = NULL;
	free_buf(buf);

	coll->local_ok = true;
	_progress_fan_in(coll);
	return SLURM_SUCCESS;
err_exit:
	list_destroy(modex_list);
	return SLURM_ERROR;
}

int pmixp_coll_contrib_node(pmixp_coll_t *coll, char *nodename,
			    void *contrib, size_t size)
{
	int idx;
	int nodeid;

	pmixp_coll_sanity_check(coll);
	if( PMIXP_COLL_SYNC == coll->state ){
		coll->state = PMIXP_COLL_FAN_IN;
	}
	xassert( PMIXP_COLL_FAN_IN == coll->state);

	/* Because of possible timeouts/delays in transmission we theoretically
	 * can receive a contribution second time. Avoid applying it by checking
	 * our records. */
	nodeid = hostlist_find(coll->all_children, nodename);
	idx = _is_child_no(coll, nodeid);
	xassert(0 <= idx );
	if( 0 < coll->ch_contribs[idx]){
		/* shouldn't be grater than 1! */
		xassert(1 == coll->ch_contribs[idx]);
		/* this is duplication, skip. */
		return SLURM_SUCCESS;
	}

	/* Copy data ONLY if we collect data, otherwise we
	 * will have only one uint32_t cell with 0 in it */
	if( sizeof(uint32_t) < size ){
		_adjust_data_size(coll, size);
		memcpy(coll->data + coll->data_pay, contrib, size);
		coll->data_pay += size;
	}

	/* increase number of individual contributions */
	coll->ch_contribs[idx]++;

	/* increase number of total contributions */
	coll->contrib_cntr++;

	/* make a progress */
	_progress_fan_in(coll);

	return SLURM_SUCCESS;
}

#define _move_and_free(data, size, buf)		\
{						\
	uint32_t offset = get_buf_offset(buf);	\
	data += offset;			\
	size -= offset;				\
	buf->head = NULL;			\
	free_buf(buf);				\
}

static int _push_into_db(void *data, size_t size)
{
	Buf buf;
	int rc;

	if( sizeof(uint32_t) >= size ){
		return SLURM_SUCCESS;
	}

	while( 0 < size ){
		pmixp_namespace_t *nsptr;
		char nspace[PMIX_MAX_NSLEN];
		uint32_t cnt, tmp, size_estim;
		int i;

		/* There might be up to PMIX_MAX_NSLEN symbols in
		 * namespace, sizeof(int) size of namespace
		 * and 2 parameters */
		size_estim = PMIX_MAX_NSLEN + 3*sizeof(uint32_t);
		size_estim = size_estim < size ? size_estim : size;
		buf = create_buf(data, size_estim);

		if( SLURM_SUCCESS != (rc = unpack32(&cnt,buf)) ){
			return rc;
		}

		if( SLURM_SUCCESS != ( rc = unpackmem(nspace,&tmp,buf) ) ){
			return rc;
		}
		nspace[tmp] = '\0';


		if( SLURM_SUCCESS != (rc = unpack32(&size_estim,buf)) ){
			return rc;
		}
		_move_and_free(data, size, buf);

		if( NULL == (nsptr = pmixp_nspaces_find(nspace) ) ){
			PMIXP_ERROR("Unknown namespace %s", nspace);
			return SLURM_ERROR;
		}

		buf = create_buf(data, size_estim);
		for(i=0; i<cnt; i++){
			uint32_t bsize = 0, scope;
			uint32_t rank = 0;
			char *blob = NULL;
			if( SLURM_SUCCESS != (rc = unpack32(&rank,buf)) ){
				return rc;
			}
			if( SLURM_SUCCESS != (rc = unpack32(&scope,buf)) ){
				return rc;
			}
			if( SLURM_SUCCESS != (rc = unpackmem_ptr(&blob, &bsize, buf)) ){
				return rc;
			}
			pmixp_nspace_add_blob(nspace, scope, (int)rank, blob, bsize);
		}
		_move_and_free(data, size, buf);
	}
	return SLURM_SUCCESS;
}

static int _reply_to_libpmix(pmixp_coll_t *coll)
{
	int i;
	List modex_list = list_create(pmixp_xfree_buffer);
	ListIterator it;
	pmix_modex_data_t *mdx = NULL;
	pmixp_modex_t *pmdx;
	size_t size;
	int rc;

	for( i=0; i < coll->nranges; i++)
	{
		pmix_range_t *range = &coll->ranges[i];

		if( SLURM_SUCCESS != (rc = _collect_range(range,PMIX_LOCAL, modex_list) ) ){
			goto err_exit;
		}
		if( SLURM_SUCCESS != (rc = _collect_range(range,PMIX_GLOBAL, modex_list) ) ){
			goto err_exit;
		}
		if( SLURM_SUCCESS != (rc = _collect_range(range,PMIX_REMOTE, modex_list) ) ){
			goto err_exit;
		}
	}
	size = list_count(modex_list);
	mdx = malloc(sizeof(pmix_modex_data_t) * size);
	it = list_iterator_create(modex_list);
	for(i=0; i<size; i++){
		pmdx = list_next(it);
		mdx[i] = pmdx->data;
	}
	list_iterator_destroy(it);
	list_destroy(modex_list);

	if( NULL != coll->cbfunc ){
		coll->cbfunc(PMIX_SUCCESS, mdx, size, coll->cbdata);
	}
	return SLURM_SUCCESS;
err_exit:
	list_destroy(modex_list);
	coll->cbfunc(PMIX_ERROR, NULL, 0, coll->cbdata);
	return SLURM_ERROR;
}

#ifndef NDEBUG
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
#endif

void _progress_fan_in(pmixp_coll_t *coll)
{
	pmixp_srv_cmd_t type;
	const char *addr = pmixp_info_srv_addr();
	char *hostlist = NULL;
	int rc;

	pmixp_coll_sanity_check(coll);
	xassert( PMIXP_COLL_FAN_IN == coll->state );

	if( !coll->local_ok || (coll->contrib_cntr != coll->children_cnt) ){
		// Nothing to do.
		return;
	}

	// The root of the collective will have parent_host == NULL
	// TODO: in future root might need to exchange with other root's
	// (in case of multiple namespace collective)
	if( NULL != coll->parent_host ){
		hostlist = xstrdup(coll->parent_host);
		type = PMIXP_MSG_FAN_IN;
	} else {
		hostlist = hostlist_ranged_string_xmalloc(coll->all_children);
		type = PMIXP_MSG_FAN_OUT;
	}

	rc = pmixp_server_send_coll(hostlist, type, coll->seq, addr,
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
}

void _progress_fan_out(pmixp_coll_t *coll)
{
	pmixp_coll_sanity_check(coll);
	xassert( PMIXP_COLL_FAN_OUT == coll->state );

	// update the database
	_push_into_db(coll->data, coll->data_pay);
	_reply_to_libpmix(coll);

	// Prepare for the next collective operation
	coll->state = PMIXP_COLL_SYNC;
	memset(coll->ch_contribs, 0, sizeof(int) * coll->children_cnt);
	coll->seq++; /* move to the next collective */
	coll->contrib_cntr = 0;
	coll->local_ok = false;
	// WARRNING: At fan-out stage we are working with data
	// in message directly. It will be freed elsewhere!
	// ! NO xfree(coll->data);
	coll->data = NULL;
	coll->data_pay = coll->data_sz = 0;

}

void pmixp_coll_fan_out_data(pmixp_coll_t *coll, void *data,
				uint32_t size)
{
	coll->data = data;
	coll->data_sz = coll->data_pay = (size_t)size;
	_progress_fan_out(coll);
}
