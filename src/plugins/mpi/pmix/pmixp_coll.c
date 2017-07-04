/*****************************************************************************\
 **  pmix_coll.c - PMIx collective primitives
 *****************************************************************************
 *  Copyright (C) 2014-2015 Artem Polyakov. All rights reserved.
 *  Copyright (C) 2015-2017 Mellanox Technologies. All rights reserved.
 *  Written by Artem Polyakov <artpol84@gmail.com, artemp@mellanox.com>.
 *
 *  This file is part of SLURM, a resource management program.
 *  For details, see <https://slurm.schedmd.com/>.
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

static void _progress_coll(pmixp_coll_t *coll);
static void _reset_coll(pmixp_coll_t *coll);

static int _hostset_from_ranges(const pmix_proc_t *procs, size_t nprocs,
				hostlist_t *hl_out)
{
	int i;
	hostlist_t hl = hostlist_create("");
	pmixp_namespace_t *nsptr = NULL;
	for (i = 0; i < nprocs; i++) {
		char *node = NULL;
		hostlist_t tmp;
		nsptr = pmixp_nspaces_find(procs[i].nspace);
		if (NULL == nsptr) {
			goto err_exit;
		}
		if (procs[i].rank == PMIX_RANK_WILDCARD) {
			tmp = hostlist_copy(nsptr->hl);
		} else {
			tmp = pmixp_nspace_rankhosts(nsptr, &procs[i].rank, 1);
		}
		while (NULL != (node = hostlist_pop(tmp))) {
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

static int _pack_ranges(pmixp_coll_t *coll, Buf buf)
{
	pmix_proc_t *procs = coll->procs;
	size_t nprocs = coll->nprocs;
	uint32_t size;
	int i;

	/* 1. store the type of collective */
	size = coll->type;
	pack32(size, buf);

	/* 2. Put the number of ranges */
	pack32(nprocs, buf);
	for (i = 0; i < (int)nprocs; i++) {
		/* Pack namespace */
		packmem(procs->nspace, strlen(procs->nspace) + 1, buf);
		pack32(procs->rank, buf);
	}

	return SLURM_SUCCESS;
}

int pmixp_coll_unpack_ranges(Buf buf, pmixp_coll_type_t *type,
			     pmix_proc_t **r, size_t *nr)
{
	pmix_proc_t *procs = NULL;
	uint32_t nprocs = 0;
	uint32_t tmp;
	int i, rc;

	/* 1. extract the type of collective */
	if (SLURM_SUCCESS != (rc = unpack32(&tmp, buf))) {
		PMIXP_ERROR("Cannot unpack collective type");
		return rc;
	}
	*type = tmp;

	/* 2. get the number of ranges */
	if (SLURM_SUCCESS != (rc = unpack32(&nprocs, buf))) {
		PMIXP_ERROR("Cannot unpack collective type");
		return rc;
	}
	*nr = nprocs;

	procs = xmalloc(sizeof(pmix_proc_t) * nprocs);
	*r = procs;

	for (i = 0; i < (int)nprocs; i++) {
		/* 3. get namespace/rank of particular process */
		rc = unpackmem(procs[i].nspace, &tmp, buf);
		if (SLURM_SUCCESS != rc) {
			PMIXP_ERROR("Cannot unpack namespace for process #%d",
				    i);
			return rc;
		}
		procs[i].nspace[tmp] = '\0';

		unsigned int tmp;
		rc = unpack32(&tmp, buf);
		procs[i].rank = tmp;
		if (SLURM_SUCCESS != rc) {
			PMIXP_ERROR("Cannot unpack ranks for process #%d,"
				    " nsp=%s",
				    i, procs[i].nspace);
			return rc;
		}
	}
	return SLURM_SUCCESS;
}

int pmixp_coll_belong_chk(pmixp_coll_type_t type,
			  const pmix_proc_t *procs, size_t nprocs)
{
	int i;
	pmixp_namespace_t *nsptr = pmixp_nspaces_local();
	/* Find my namespace in the range */
	for (i = 0; i < nprocs; i++) {
		if (0 != xstrcmp(procs[i].nspace, nsptr->name)) {
			continue;
		}
		if ((procs[i].rank == PMIX_RANK_WILDCARD))
			return 0;
		if (0 <= pmixp_info_taskid2localid(procs[i].rank)) {
			return 0;
		}
	}
	/* we don't participate in this collective! */
	PMIXP_ERROR("Have collective that doesn't include this job's "
		    "namespace");
	return -1;
}

static void _reset_coll_ufwd(pmixp_coll_t *coll)
{
	/* upward status */
	coll->contrib_children = 0;
	coll->contrib_local = false;
	memset(coll->contrib_child, 0,
	       sizeof(coll->contrib_child[0]) * coll->children_cnt);
	coll->serv_offs = pmixp_server_buf_reset(coll->ufwd_buf);
	if (SLURM_SUCCESS != _pack_ranges(coll, coll->ufwd_buf)) {
		PMIXP_ERROR("Cannot pack ranges to message header!");
	}
	coll->ufwd_offset = get_buf_offset(coll->ufwd_buf);
	coll->ufwd_status = PMIXP_COLL_SND_NONE;
}

static void _reset_coll_dfwd(pmixp_coll_t *coll)
{
	/* downwards status */
	(void)pmixp_server_buf_reset(coll->dfwd_buf);
	if (SLURM_SUCCESS != _pack_ranges(coll, coll->dfwd_buf)) {
		PMIXP_ERROR("Cannot pack ranges to message header!");
	}
	coll->dfwd_complete_cnt = 0;
	coll->dfwd_status = PMIXP_COLL_SND_NONE;
	coll->contrib_parent = false;
	/* Save the toal service offset */
	coll->dfwd_offset = get_buf_offset(coll->dfwd_buf);
}

static void _reset_coll(pmixp_coll_t *coll)
{
	switch (coll->state) {
	case PMIXP_COLL_SYNC:
		/* already reset */
		xassert(!coll->contrib_local && !coll->contrib_children &&
			!coll->contrib_parent);
		break;
	case PMIXP_COLL_COLLECT:
	case PMIXP_COLL_UPFWD:
		coll->seq++;
		coll->state = PMIXP_COLL_SYNC;
		_reset_coll_ufwd(coll);
		_reset_coll_dfwd(coll);
		coll->cbdata = NULL;
		coll->cbfunc = NULL;
		break;
	case PMIXP_COLL_DOWNFWD:
		coll->seq++;
		_reset_coll_dfwd(coll);
		if (coll->contrib_local || coll->contrib_children) {
			/* next collective was already started */
			coll->state = PMIXP_COLL_COLLECT;
		} else {
			coll->state = PMIXP_COLL_SYNC;
		}

		if (!coll->contrib_local) {
			/* drop the callback info if we haven't started
			 * next collective locally
			 */
			coll->cbdata = NULL;
			coll->cbfunc = NULL;
		}
		break;
	default:
		PMIXP_ERROR("Bad collective state = %d", (int)coll->state);
		abort();
	}
}


/*
 * Based on ideas provided by Hongjia Cao <hjcao@nudt.edu.cn> in PMI2 plugin
 */
int pmixp_coll_init(pmixp_coll_t *coll, const pmix_proc_t *procs,
		    size_t nprocs, pmixp_coll_type_t type)
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
	coll->procs = xmalloc(sizeof(*procs) * nprocs);
	memcpy(coll->procs, procs, sizeof(*procs) * nprocs);
	coll->nprocs = nprocs;
	coll->my_nspace = my_nspace;

	if (SLURM_SUCCESS != _hostset_from_ranges(procs, nprocs, &hl)) {
		/* TODO: provide ranges output routine */
		PMIXP_ERROR("Bad ranges information");
		goto err_exit;
	}
#ifdef PMIXP_COLL_DEBUG
	/* if we debug collectives - store a copy of a full
	 * hostlist to resolve participant id to the hostname */
	coll->all_nodes = hostlist_copy(hl);
#endif

	width = slurm_get_tree_width();
	nodes = hostlist_count(hl);
	nodeid = hostlist_find(hl, pmixp_info_hostname());
	reverse_tree_info(nodeid, nodes, width, &parent_id, &tmp, &depth,
			  &max_depth);

	coll->children_cnt = tmp;
	coll->nodeid = nodeid;

	/* We interested in amount of direct childs */
	coll->seq = 0;
	coll->contrib_children = 0;
	coll->contrib_local = false;
	coll->children_ids = xmalloc(sizeof(int) * width);
	coll->contrib_child = xmalloc(sizeof(int) * width);
	coll->children_cnt = reverse_tree_direct_children(nodeid, nodes, width,
							  depth,
							  coll->children_ids);
	if (parent_id == -1) {
		/* if we are the root of the tree:
		 * - we don't have a parent;
		 * - we have large list of all_childrens (we don't want
		 * ourselfs there)
		 */
		coll->parent_host = NULL;
		hostlist_delete_host(hl, pmixp_info_hostname());
		coll->all_children = hl;
		coll->all_children_str =
			hostlist_ranged_string_xmalloc(coll->all_children);
	} else if (parent_id >= 0) {
		/* for all other nodes in the tree we need to know:
		 * - nodename of our parent;
		 * - we don't need a list of all_childrens and hl anymore
		 */
		p = hostlist_nth(hl, parent_id);
		coll->parent_host = xstrdup(p);
		coll->parent_nodeid = pmixp_info_job_hostid(coll->parent_host);
		/* use empty hostlist here */
		coll->all_children = hostlist_create("");
		coll->all_children_str = NULL;
		free(p);
		hostlist_destroy(hl);
	}

	/* Collective state */
	_reset_coll_ufwd(coll);
	_reset_coll_dfwd(coll);
	coll->cbdata = NULL;
	coll->cbfunc = NULL;

	/* init fine grained lock */
	slurm_mutex_init(&coll->lock);

	return SLURM_SUCCESS;
err_exit:
	return SLURM_ERROR;
}

void pmixp_coll_free(pmixp_coll_t *coll)
{
	if (NULL != coll->procs) {
		xfree(coll->procs);
	}
	if (NULL != coll->parent_host) {
		xfree(coll->parent_host);
	}
	hostlist_destroy(coll->all_children);
	if (coll->all_children_str) {
		xfree(coll->all_children_str);
	}
#ifdef PMIXP_COLL_DEBUG
	hostlist_destroy(coll->all_nodes);
#endif
	if (NULL != coll->contrib_child) {
		xfree(coll->contrib_child);
	}
	free_buf(coll->ufwd_buf);
	free_buf(coll->dfwd_buf);
}

static void _ufwd_sent_cb(int rc, pmixp_p2p_ctx_t ctx, void *cb_data)
{
	pmixp_coll_t *coll = (pmixp_coll_t *)cb_data;

	if( PMIXP_P2P_REGULAR == ctx ){
		/* lock the collective */
		slurm_mutex_lock(&coll->lock);
	}

	xassert(PMIXP_COLL_UPFWD == coll->state);

	/* Change  the status */
	if( SLURM_SUCCESS == rc ){
		coll->ufwd_status = PMIXP_COLL_SND_DONE;
	} else {
		coll->ufwd_status = PMIXP_COLL_SND_FAILED;
	}

	if( PMIXP_P2P_REGULAR == ctx ){
		/* progress, in the inline case progress
		 * will be invoked by the caller */
		_progress_coll(coll);

		/* unlock the collective */
		slurm_mutex_unlock(&coll->lock);
	}
}

static void _dfwd_sent_cb(int rc, pmixp_p2p_ctx_t ctx, void *cb_data)
{
	pmixp_coll_t *coll = (pmixp_coll_t *)cb_data;

	if( PMIXP_P2P_REGULAR == ctx ){
		/* lock the collective */
		slurm_mutex_lock(&coll->lock);
	}

	xassert(PMIXP_COLL_UPFWD == coll->state);

	/* Change  the status */
	if( SLURM_SUCCESS == rc ){
		coll->dfwd_complete_cnt++;
		/* if all childrens + local callbacks was invoked */
		if ((coll->children_cnt + 1) == coll->dfwd_complete_cnt) {
			coll->dfwd_status = PMIXP_COLL_SND_DONE;
		}
	} else {
		coll->dfwd_status = PMIXP_COLL_SND_FAILED;
	}

	if( PMIXP_P2P_REGULAR == ctx ){
		/* progress, in the inline case progress
		 * will be invoked by the caller */
		_progress_coll(coll);

		/* unlock the collective */
		slurm_mutex_unlock(&coll->lock);
	}
}

static void _libpmix_cb(void *cb_data)
{
	pmixp_coll_t *coll = (pmixp_coll_t *)cb_data;

	/* lock the collective */
	slurm_mutex_lock(&coll->lock);

	xassert(PMIXP_COLL_DOWNFWD == coll->state);

	coll->dfwd_complete_cnt++;
	_progress_coll(coll);

	/* unlock the collective */
	slurm_mutex_unlock(&coll->lock);
}

static int _progress_collect(pmixp_coll_t *coll)
{
	pmixp_ep_t ep = {0};
	int rc;

	xassert(PMIXP_COLL_COLLECT == coll->state);

	ep.type = PMIXP_EP_NONE;
#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("0x%p: state=%s, local=%d, child_cntr=%d",
		    coll, pmixp_coll_state2str(coll->state),
		    (int)coll->contrib_local, coll->contrib_children);
#endif
	/* lock the collective */
	pmixp_coll_sanity_check(coll);

	if (PMIXP_COLL_COLLECT != coll->state) {
		/* In case of race condition between libpmix and
		 * slurm threads we can be called
		 * after we moved to the next step. */
		return 0;
	}

	if (!coll->contrib_local || coll->contrib_children != coll->children_cnt) {
		/* Not yet ready to go to the next step */
		return 0;
	}

	coll->state = PMIXP_COLL_UPFWD;

	/* The root of the collective will have parent_host == NULL */
	if (NULL != coll->parent_host) {
		ep.type = PMIXP_EP_NOIDEID;
		ep.ep.nodeid = coll->parent_nodeid;
		coll->ufwd_status = PMIXP_COLL_SND_ACTIVE;
		PMIXP_DEBUG("0x%p: send data to %s:%d",
			    coll, coll->parent_host, coll->parent_nodeid);
	} else {
		/* move data from input buffer to the output */
		char *src = get_buf_data(coll->ufwd_buf) + coll->ufwd_offset;
		char *dst = get_buf_data(coll->dfwd_buf) + coll->dfwd_offset;
		size_t size = get_buf_offset(coll->ufwd_buf) -
				coll->ufwd_offset;
		memcpy(dst, src, size);
		set_buf_offset(coll->dfwd_buf, coll->dfwd_offset + size);
		/* no need to send */
		coll->ufwd_status = PMIXP_COLL_SND_DONE;
	}

	if (PMIXP_EP_NONE != ep.type) {
		rc = pmixp_server_send_nb(&ep, PMIXP_MSG_FAN_IN, coll->seq,
					  coll->ufwd_buf,
					  _ufwd_sent_cb, coll);

		if (SLURM_SUCCESS != rc) {
			char *nodename = strdup("unknown");
#ifdef PMIXP_COLL_DEBUG
			nodename = hostlist_nth(coll->all_children,
						ep.ep.nodeid);
#endif
			PMIXP_ERROR("Cannot send data (size = %lu), "
				    "to %s:%d",
				    (uint64_t) get_buf_offset(coll->ufwd_buf),
				    nodename, ep.ep.nodeid);
			coll->ufwd_status = PMIXP_COLL_SND_FAILED;
			free(nodename);
		}
	}

	/* events observed - need another iteration */
	return true;
}

static int _progress_ufwd(pmixp_coll_t *coll)
{
	pmixp_ep_t ep[coll->children_cnt];
	int ep_cnt = 0;
	int rc, i;

	xassert(PMIXP_COLL_UPFWD == coll->state);

	switch (coll->ufwd_status) {
	case PMIXP_COLL_SND_FAILED:
		/* something went wrong with upward send.
		 * notify libpmix about that and abort
		 * collective */
		if (coll->cbfunc) {
			coll->cbfunc(PMIX_ERROR, NULL, 0, coll->cbdata,
				     NULL, NULL);
		}
		_reset_coll(coll);
		/* Don't need to do anything else */
		return false;
	case PMIXP_COLL_SND_ACTIVE:
		/* still waiting for the send completion */
		return false;
	case PMIXP_COLL_SND_DONE:
		if (coll->contrib_parent) {
			/* all-set to go to the next stage */
			break;
		}
	default:
		/* Should not happen, fatal error */
		abort();
	}

	/* We now can upward part for the next collective */
	_reset_coll_ufwd(coll);

	/* move to the next state */
	coll->state = PMIXP_COLL_DOWNFWD;
	if (!pmixp_info_srv_direct_conn()) {
		ep[ep_cnt].type = PMIXP_EP_HLIST;
		ep[ep_cnt].ep.hostlist = coll->all_children_str;
		ep_cnt++;
	} else {
		for(i=0; i<coll->children_cnt; i++){
			ep[i].type = PMIXP_EP_NOIDEID;
			ep[i].ep.nodeid = coll->children_ids[i];
			ep_cnt++;
		}
	}

	for(i=0; i < ep_cnt; i++){
		rc = pmixp_server_send_nb(&ep[i], PMIXP_MSG_FAN_OUT, coll->seq,
					  coll->dfwd_buf,
					  _dfwd_sent_cb, coll);

		if (SLURM_SUCCESS != rc) {
			if (PMIXP_EP_NOIDEID == ep[i].type){
				char *nodename = NULL;
#ifdef PMIXP_COLL_DEBUG
				nodename = hostlist_nth(coll->all_children,
							ep[i].ep.nodeid);
#else
				nodename = strdup("unknown");
#endif
				PMIXP_ERROR("Cannot send data (size = %lu), "
				    "to %s:%d",
				    (uint64_t) get_buf_offset(coll->dfwd_buf),
				    nodename, ep[i].ep.nodeid);
				free(nodename);
			} else {
				PMIXP_ERROR("Cannot send data (size = %lu), "
				    "to %s",
				    (uint64_t) get_buf_offset(coll->dfwd_buf),
				    ep[i].ep.hostlist);
			}
			coll->dfwd_status = PMIXP_COLL_SND_FAILED;
		}
	}

	if (coll->cbfunc) {
		char *data = get_buf_data(coll->dfwd_buf) + coll->dfwd_offset;
		size_t size = get_buf_offset(coll->dfwd_buf) -
				coll->dfwd_offset;
		coll->cbfunc(PMIX_SUCCESS, data, size, coll->cbdata,
			     _libpmix_cb, (void *)coll->dfwd_buf);
	}

	/* events observed - need another iteration */
	return true;
}

static int _progress_dfwd(pmixp_coll_t *coll)
{
	xassert(PMIXP_COLL_DOWNFWD == coll->state);
	switch (coll->dfwd_status) {
	case PMIXP_COLL_SND_ACTIVE:
		return false;
	case PMIXP_COLL_SND_FAILED:
		/* something went wrong with upward send.
		 * notify libpmix about that and abort
		 * collective */
		if (coll->cbfunc) {
			coll->cbfunc(PMIX_ERROR, NULL, 0, coll->cbdata,
				     NULL, NULL);
		}
		_reset_coll(coll);
		/* Don't need to do anything else */
		return false;
	case PMIXP_COLL_SND_DONE:
		break;
	default:
		/* Should not happen, fatal error */
		abort();
	}

	_reset_coll(coll);

	return true;
}

static void _progress_coll(pmixp_coll_t *coll)
{
	int ret = 0;
	do {
		switch (coll->state) {
		case PMIXP_COLL_SYNC:
			/* check if any activity was observed */
			if (coll->contrib_local || coll->contrib_children) {
				coll->state = PMIXP_COLL_COLLECT;
			}
			ret = true;
			break;
		case PMIXP_COLL_COLLECT:
			ret = _progress_collect(coll);
			break;
		case PMIXP_COLL_UPFWD:
			ret = _progress_ufwd(coll);
			break;
		case PMIXP_COLL_DOWNFWD:
			ret = _progress_dfwd(coll);
			break;
		}
	} while(ret);
}

int pmixp_coll_contrib_local(pmixp_coll_t *coll, char *data, size_t size)
{
	int ret = SLURM_SUCCESS;

	/* sanity check */
	pmixp_coll_sanity_check(coll);

	/* lock the structure */
	slurm_mutex_lock(&coll->lock);

#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("0x%p: contrib/loc: seqnum=%ud, state=%s, size=%zd",
		    coll, coll->seq, pmixp_coll_state2str(coll->state), size);
#endif

	switch (coll->state) {
	case PMIXP_COLL_SYNC:
		/* change the state */
		coll->ts = time(NULL);
		/* fall-thru */
	case PMIXP_COLL_COLLECT:
		/* sanity check */
		break;
	case PMIXP_COLL_DOWNFWD:
		/* We are waiting for some send requests
		 * to be finished, but local node has started
		 * the next contribution.
		 * This is an OK situation, go ahead and store
		 * it, the buffer with the contribution is not used
		 * now.
		 */
#ifdef PMIXP_COLL_DEBUG
		PMIXP_DEBUG("0x%p: contrib/loc: next coll!", coll);
#endif
		break;
	case PMIXP_COLL_UPFWD:
		/* this is not a correct behavior, respond with an error. */
		ret = SLURM_ERROR;
		goto exit;
	default:
		/* FATAL: should not happen in normal workflow */
		PMIXP_ERROR("0x%p: local contrib while active collective, "
			    "state = %s",
			    coll, pmixp_coll_state2str(coll->state));
		xassert(0);
		abort();
	}

	if (coll->contrib_local) {
		/* Double contribution - reject */
		ret = SLURM_ERROR;
		goto exit;
	}

	/* save & mark local contribution */
	coll->contrib_local = true;
	pmixp_server_buf_reserve(coll->ufwd_buf, size);
	memcpy(get_buf_data(coll->ufwd_buf) + get_buf_offset(coll->ufwd_buf),
	       data, size);
	set_buf_offset(coll->ufwd_buf, get_buf_offset(coll->ufwd_buf) + size);

	/* check if the collective is ready to progress */
	_progress_coll(coll);

#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("0x%p: finish, state=%s",
		    coll, pmixp_coll_state2str(coll->state));
#endif

exit:
	/* unlock the structure */
	slurm_mutex_unlock(&coll->lock);
	return ret;
}

int pmixp_coll_contrib_child(pmixp_coll_t *coll, uint32_t nodeid,
			     uint32_t seq, Buf buf)
{
	char *data_src = NULL, *data_dst = NULL;
	uint32_t size;

	/* lock the structure */
	slurm_mutex_lock(&coll->lock);

#ifdef PMIXP_COLL_DEBUG
	char *nodename = hostlist_nth(coll->all_nodes, nodeid);
	PMIXP_DEBUG("0x%p: contrib/rem: node=%s, nodeid=%ud, "
		    "state=%s, size=%ud",
		    coll, nodename, nodeid, pmixp_coll_state2str(coll->state),
		    remaining_buf(buf));
#endif

	pmixp_coll_sanity_check(coll);
	if (coll->nprocs > nodeid) {
		/* protect ourselfs if we are running with no asserts */
		PMIXP_ERROR("0x%p: bad nodeid=%ud, nprocs=%zd",
			    coll, nodeid, coll->nprocs);
		goto proceed;
	}

	switch (coll->state) {
	case PMIXP_COLL_SYNC:
		/* change the state */
		coll->ts = time(NULL);
		/* fall-thru */
	case PMIXP_COLL_COLLECT:
		/* sanity check */
		if (coll->seq != seq) {
			/* FATAL: should not happen in normal workflow */
			PMIXP_ERROR("0x%p: unexpected contribution seq = %d,"
				    " coll->seq = %d, state=%s",
				    coll, seq, coll->seq,
				    pmixp_coll_state2str(coll->state));
			xassert(coll->seq == seq);
			abort();
		}
		break;
	case PMIXP_COLL_UPFWD:
		/* FATAL: should not happen in normal workflow */
		PMIXP_ERROR("0x%p: unexpected contrib from %s:%d, state = %s",
			    coll, nodename, nodeid,
			    pmixp_coll_state2str(coll->state));
		xassert(0);
		abort();
	case PMIXP_COLL_DOWNFWD:
#ifdef PMIXP_COLL_DEBUG
		/* It looks like a retransmission attempt when remote side
		 * identified transmission failure, but we actually successfuly
		 * received the message */
		PMIXP_DEBUG("0x%p: next collective contrib: node=%s, "
			    "nodeid=%ud, seq=%ud, cur_seq=%ud, state=%s",
			    coll, nodename, nodeid, seq, coll->seq,
			    pmixp_coll_state2str(coll->state));
#endif
		if ((coll->seq +1) != seq) {
			/* should not happen in normal workflow */
			PMIXP_ERROR("0x%p: unexpected contribution seq = %d,"
				    " coll->seq = %d, state=%s",
				    coll, seq, coll->seq,
				    pmixp_coll_state2str(coll->state));
			xassert((coll->seq +1) == seq);
			abort();
		}
		goto proceed;
	default:
		/* should not happen in normal workflow */
		PMIXP_ERROR("0x%p: unknown collective state %s",
			    coll, pmixp_coll_state2str(coll->state));
		abort();
	}

	/* Because of possible timeouts/delays in transmission we
	 * can receive a contribution second time. Avoid duplications
	 * by checking our records. */
	if (coll->contrib_child[nodeid]) {
		/* May be 0 or 1. If grater - transmission skew, ignore.
		 * NOTE: this output is not on the critical path -
		 * don't preprocess it out */
		PMIXP_DEBUG("0x%p: multiple contributions from %s:%d",
			    coll, nodename, nodeid);
		/* this is duplication, skip. */
		goto proceed;
	}

	data_src = get_buf_data(buf) + get_buf_offset(buf);
	size = remaining_buf(buf);
	pmixp_server_buf_reserve(coll->ufwd_buf, size);
	data_dst = get_buf_data(coll->ufwd_buf) + get_buf_offset(coll->ufwd_buf);
	memcpy(data_dst, data_src, size);
	set_buf_offset(coll->ufwd_buf, get_buf_offset(coll->ufwd_buf) + size);

	/* increase number of individual contributions */
	coll->contrib_child[nodeid] = true;
	/* increase number of total contributions */
	coll->contrib_children++;

proceed:
	_progress_coll(coll);

#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("0x%p: finish: node=%s, state=%s",
		    coll, nodename, pmixp_coll_state2str(coll->state));
	free(nodename);
#endif
	/* unlock the structure */
	slurm_mutex_unlock(&coll->lock);

	return SLURM_SUCCESS;
}

int pmixp_coll_contrib_parent(pmixp_coll_t *coll, uint32_t nodeid,
			     uint32_t seq, Buf buf)
{
	char *data_src = NULL, *data_dst = NULL;
	uint32_t size;

	/* lock the structure */
	slurm_mutex_lock(&coll->lock);

#ifdef PMIXP_COLL_DEBUG
	char *nodename = hostlist_nth(coll->all_nodes, nodeid);
	PMIXP_DEBUG("0x%p: contrib/rem: node=%s, nodeid=%ud, "
		    "state=%s, size=%ud", coll, nodename, nodeid,
		    pmixp_coll_state2str(coll->state), remaining_buf(buf));
#endif

	pmixp_coll_sanity_check(coll);
	if (coll->nprocs > nodeid) {
		/* protect ourselfs if we are running with no asserts */
		PMIXP_ERROR("0x%p: bad nodeid=%ud, nprocs=%zd",
			    coll, nodeid, coll->nprocs);
		goto proceed;
	}

	switch (coll->state) {
	case PMIXP_COLL_SYNC:
	case PMIXP_COLL_COLLECT:
		/* It looks like a retransmission attempt when remote side
		 * identified transmission failure, but we actually successfuly
		 * received the message */
#ifdef PMIXP_COLL_DEBUG
		PMIXP_DEBUG("0x%p: prev collective contrib: node=%s, "
			    "nodeid=%ud, seq=%ud, cur_seq=%ud, state=%s",
			    coll, nodename, nodeid, seq, coll->seq,
			    pmixp_coll_state2str(coll->state));
#endif
		/* sanity check */
		if ((coll->seq - 1) != seq) {
			/* FATAL: should not happen in normal workflow */
			PMIXP_ERROR("0x%p: unexpected contribution seq = %d,"
				    " coll->seq = %d, state=%s",
				    coll, seq, coll->seq,
				    pmixp_coll_state2str(coll->state));
			xassert((coll->seq - 1) == seq);
			abort();
		}
		goto proceed;
	case PMIXP_COLL_UPFWD:
		/* Although the probability of this is quite low it's still can
		 * happen that we receive the response before
		 * FATAL: should not happen in normal workflow */
		PMIXP_ERROR("0x%p: unexpected contribution from %s, state = %s",
			    coll, nodename, pmixp_coll_state2str(coll->state));
		xassert(0);
		abort();
	case PMIXP_COLL_DOWNFWD:
#ifdef PMIXP_COLL_DEBUG
		PMIXP_DEBUG("0x%p: next collective contrib from %s",
			    coll, nodename);
#endif
		if ((coll->seq +1) != seq) {
			/* should not happen in normal workflow */
			PMIXP_ERROR("0x%p: unexpected contribution seq = %d,"
				    " coll->seq = %d, state=%s",
				    coll, seq, coll->seq,
				    pmixp_coll_state2str(coll->state));
			xassert((coll->seq +1) == seq);
			abort();
		}
		break;
	default:
		/* should not happen in normal workflow */
		PMIXP_ERROR("0x%p: unknown collective state %s",
			    coll, pmixp_coll_state2str(coll->state));
		abort();
	}

	/* Because of possible timeouts/delays in transmission we
	 * can receive a contribution second time. Avoid duplications
	 * by checking our records. */
	if (coll->contrib_parent) {
		/* May be 0 or 1. If grater - transmission skew, ignore.
		 * NOTE: this output is not on the critical path -
		 * don't preprocess it out */
		PMIXP_DEBUG("0x%p: multiple contributions from parent %s:%d",
			    coll, nodename, nodeid);
		/* this is duplication, skip. */
		goto proceed;
	}
	coll->contrib_parent = true;

	data_src = get_buf_data(buf) + get_buf_offset(buf);
	size = remaining_buf(buf);
	pmixp_server_buf_reserve(coll->dfwd_buf, size);

	data_dst = get_buf_data(coll->dfwd_buf) +
			get_buf_offset(coll->dfwd_buf);
	memcpy(data_dst, data_src, size);
	set_buf_offset(coll->dfwd_buf,
		       get_buf_offset(coll->dfwd_buf) + size);
proceed:
	_progress_coll(coll);

#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("0x%p: finish: node=%s, state=%s",
		    coll, nodename, pmixp_coll_state2str(coll->state));
	free(nodename);
#endif
	/* unlock the structure */
	slurm_mutex_unlock(&coll->lock);

	return SLURM_SUCCESS;
}

void pmixp_coll_reset_if_to(pmixp_coll_t *coll, time_t ts)
{
	/* lock the */
	slurm_mutex_lock(&coll->lock);

	if (PMIXP_COLL_SYNC == coll->state) {
		goto unlock;
	}

	if (ts - coll->ts > pmixp_info_timeout()) {
		/* respond to the libpmix */
		if (coll->contrib_local && coll->cbfunc) {
			/* Call the callback only if:
			 * - we were asked to do that (coll->cbfunc != NULL);
			 * - local contribution was received.
			 * TODO: we may want to mark this event to respond with
			 * to the next local request immediately and with the
			 * proper (status == PMIX_ERR_TIMEOUT)
			 */
			coll->cbfunc(PMIX_ERR_TIMEOUT, NULL, 0, coll->cbdata, NULL,
				     NULL);
		}
		/* drop the collective */
		_reset_coll(coll);
		/* report the timeout event */
		PMIXP_ERROR("Collective timeout!");
	}
unlock:
	/* unlock the structure */
	slurm_mutex_unlock(&coll->lock);
}
