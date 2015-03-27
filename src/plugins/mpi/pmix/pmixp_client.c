/*****************************************************************************\
 **  pmix_client.c - PMIx client communication code
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
#include "pmixp_state.h"
#include "pmixp_io.h"
#include "pmixp_nspaces.h"
#include "pmixp_debug.h"
#include "pmixp_coll.h"
#include "pmixp_server.h"

#include <pmix_server.h>

static int finalize_fn(const char nspace[], int rank, void* server_object,
		       pmix_op_cbfunc_t cbfunc, void *cbdata);
static int abort_fn(const char nspace[], int rank, void *server_object,
		    int status, const char msg[],
		    pmix_op_cbfunc_t cbfunc, void *cbdata);
static int fencenb_fn(const pmix_range_t ranges[], size_t nranges,
		      int collect_data,
		      pmix_modex_cbfunc_t cbfunc, void *cbdata);
static int store_modex_fn(const char nspace[], int rank, void *server_object,
			  pmix_scope_t scope, pmix_modex_data_t *data);
static int get_modexnb_fn(const char nspace[], int rank,
                          pmix_modex_cbfunc_t cbfunc, void *cbdata);
static int publish_fn(pmix_scope_t scope, pmix_persistence_t persist,
                      const pmix_info_t info[], size_t ninfo,
                      pmix_op_cbfunc_t cbfunc, void *cbdata);
static int lookup_fn(pmix_scope_t scope, int wait, char **keys,
                     pmix_lookup_cbfunc_t cbfunc, void *cbdata);
static int unpublish_fn(pmix_scope_t scope, char **keys,
                        pmix_op_cbfunc_t cbfunc, void *cbdata);
static int spawn_fn(const pmix_app_t apps[], size_t napps,
                    pmix_spawn_cbfunc_t cbfunc, void *cbdata);
static int connect_fn(const pmix_range_t ranges[], size_t nranges,
                      pmix_op_cbfunc_t cbfunc, void *cbdata);
static int disconnect_fn(const pmix_range_t ranges[], size_t nranges,
                         pmix_op_cbfunc_t cbfunc, void *cbdata);

pmix_server_module_t _slurm_pmix_cb = {
    finalize_fn,
    abort_fn,
    fencenb_fn,
    store_modex_fn,
    get_modexnb_fn,
    publish_fn,
    lookup_fn,
    unpublish_fn,
    spawn_fn,
    connect_fn,
    disconnect_fn
};

static void errhandler(pmix_status_t status, pmix_range_t ranges[],
                       size_t nranges, pmix_info_t info[], size_t ninfo);

int pmixp_libpmix_init(struct sockaddr_un *address)
{
    int rc;

    setenv("TMPDIR","/home/artpol/slurm_tmp/",1);

    /* setup the server library */
    if (PMIX_SUCCESS != (rc = PMIx_server_init(&_slurm_pmix_cb, false))) {
	PMIXP_ERROR_STD("PMIx_server_init failed with error %d\n", rc);
	return SLURM_ERROR;
    }

    /* register the errhandler */
    PMIx_Register_errhandler(errhandler);

    /* retrieve the rendezvous address */
    if (PMIX_SUCCESS != ( rc = PMIx_get_rendezvous_address(address) ) ) {
	PMIXP_ERROR_STD("PMIx_get_rendezvous_address failed with error: %d", rc);
	return SLURM_ERROR;
    }
    return 0;
}

static void errhandler(pmix_status_t status,
		       pmix_range_t ranges[], size_t nranges,
		       pmix_info_t info[], size_t ninfo)
{
	// TODO: do something more sophisticated here
	// FIXME: use proper specificator for nranges
	PMIXP_ERROR_STD("Error handler invoked: status = %d, nranges = %d", status, (int)nranges);
}

#define PMIXP_ALLOC_KEY(kvp, key_str) {				\
	char *key = key_str;					\
	kvp = (pmix_info_t*)xmalloc(sizeof(pmix_info_t));	\
	(void)strncpy(kvp->key, key, PMIX_MAX_KEYLEN);		\
}

int pmixp_libpmix_job_set()
{
	List lresp;
	pmix_info_t *info;
	int ninfo;
	char *p = NULL;
	ListIterator it;
	pmix_info_t *kvp;
	uint32_t tmp;
	int i, rc;
	uid_t uid = getuid();
	gid_t gid = getgid();

	// Use list to safely expand/reduce key-value pairs.
	lresp = list_create(pmixp_xfree_buffer);

	/* The name of the current host */
	PMIXP_ALLOC_KEY(kvp, PMIX_HOSTNAME);
	PMIX_VAL_SET(&kvp->value, string, pmixp_info_hostname());
	list_append(lresp, kvp);

	/* Setup temprary directory */
	/* TODO: consider all ways of setting TMPDIR here: fixed, prolog, what else? */
	p = getenv("TMPDIR");
	if( NULL == p ){
		p = "/tmp/";
	}
	PMIXP_ALLOC_KEY(kvp, PMIX_TMPDIR);
	PMIX_VAL_SET(&kvp->value, string, p);
	list_append(lresp, kvp);

	/* jobid assigned by scheduler */
	p = NULL;
	xstrfmtcat(p, "%d.%d", pmixp_info_jobid(), pmixp_info_stepid());
	PMIXP_ALLOC_KEY(kvp, PMIX_JOBID);
	PMIX_VAL_SET(&kvp->value, string, p);
	xfree(p);
	list_append(lresp, kvp);

	/* process identification ranks */
	// TODO: what to put here for slurm?
	// FIXME: This information supposed to be related to the precise rank
/*
	PMIXP_ALLOC_KEY(kvp, PMIX_GLOBAL_RANK);
	PMIX_VAL_SET(&kvp->value, uint32_t, pmixp_info_task_id(taskid));
	list_append(lresp, kvp);

	PMIXP_ALLOC_KEY(kvp, PMIX_RANK);
	PMIX_VAL_SET(&kvp->value, uint32_t, pmixp_info_task_id(taskid));
	list_append(lresp, kvp);

	PMIXP_ALLOC_KEY(kvp, PMIX_LOCAL_RANK);
	PMIX_VAL_SET(&kvp->value, uint32_t, taskid);
	list_append(lresp, kvp);

	PMIXP_ALLOC_KEY(kvp, PMIX_APP_RANK);
	PMIX_VAL_SET(&kvp->value, uint32_t, pmixp_info_task_id(taskid));
	list_append(lresp, kvp);
*/
	PMIXP_ALLOC_KEY(kvp, PMIX_APPNUM);
	PMIX_VAL_SET(&kvp->value, uint32_t, 0);
	list_append(lresp, kvp);

	PMIXP_ALLOC_KEY(kvp, PMIX_NODE_RANK);
	PMIX_VAL_SET(&kvp->value, uint32_t, pmixp_info_nodeid());
	list_append(lresp, kvp);

	/* size information */
	PMIXP_ALLOC_KEY(kvp, PMIX_UNIV_SIZE);
	PMIX_VAL_SET(&kvp->value, uint32_t, pmixp_info_tasks_uni());
	list_append(lresp, kvp);

	PMIXP_ALLOC_KEY(kvp, PMIX_JOB_SIZE);
	PMIX_VAL_SET(&kvp->value, uint32_t, pmixp_info_tasks());
	list_append(lresp, kvp);

	PMIXP_ALLOC_KEY(kvp, PMIX_LOCAL_SIZE);
	PMIX_VAL_SET(&kvp->value, uint32_t, pmixp_info_tasks_loc());
	list_append(lresp, kvp);

	// TODO: fix it in future
	PMIXP_ALLOC_KEY(kvp, PMIX_NODE_SIZE);
	PMIX_VAL_SET(&kvp->value, uint32_t, pmixp_info_tasks_loc());
	list_append(lresp, kvp);

	PMIXP_ALLOC_KEY(kvp, PMIX_MAX_PROCS);
	PMIX_VAL_SET(&kvp->value, uint32_t, pmixp_info_tasks_uni());
	list_append(lresp, kvp);

	/* offset information */
	// TODO: Fix this in future once Spawn will be implemented
	PMIXP_ALLOC_KEY(kvp, PMIX_NPROC_OFFSET);
	PMIX_VAL_SET(&kvp->value, uint32_t, 0);
	list_append(lresp, kvp);

	PMIXP_ALLOC_KEY(kvp, PMIX_APPLDR);
	PMIX_VAL_SET(&kvp->value, uint32_t, 0);
	list_append(lresp, kvp);

	xstrfmtcat(p,"%u", pmixp_info_taskid(0));
	tmp = pmixp_info_taskid(0);
	for(i=1;i< pmixp_info_tasks_loc();i++){
		uint32_t rank = pmixp_info_taskid(i);
		xstrfmtcat(p,",%u", rank);
		if( tmp > rank ){
			tmp = rank;
		}
	}

	PMIXP_ALLOC_KEY(kvp, PMIX_LOCAL_PEERS);
	PMIX_VAL_SET(&kvp->value, string, p);
	xfree(p);
	list_append(lresp, kvp);

	PMIXP_ALLOC_KEY(kvp, PMIX_LOCALLDR);
	PMIX_VAL_SET(&kvp->value, uint32_t, tmp);
	list_append(lresp, kvp);

	PMIXP_ALLOC_KEY(kvp, PMIX_PROC_MAP);
	PMIX_VAL_SET(&kvp->value, string, pmixp_info_task_map());
	list_append(lresp, kvp);

	ninfo = list_count(lresp);
	info = xmalloc(sizeof(pmix_info_t) * ninfo);
	it = list_iterator_create(lresp);
	i = 0;
	while( NULL != (kvp = list_next(it) ) ){
		info[i] = *kvp;
		i++;
	}
	list_destroy(lresp);

	rc = PMIx_server_register_nspace(pmixp_info_namespace(),
					 pmixp_info_tasks_loc(), info, ninfo);
	xfree(info);
	if( PMIX_SUCCESS != rc ){
		PMIXP_ERROR("Cannot register namespace %s, nlocalproc=%d, "
			       "ninfo = %d", pmixp_info_namespace(),
			       pmixp_info_tasks_loc(), ninfo );
		return SLURM_ERROR;
	} else {
		PMIXP_DEBUG("task initialization");
		for(i=0;i<pmixp_info_tasks_loc();i++){
			rc = PMIx_server_register_client(pmixp_info_namespace(),
						    pmixp_info_taskid(i),
						    uid, gid,
						    &pmixp_state_cli(i)->localid);
			if( PMIX_SUCCESS != rc ){
				PMIXP_ERROR("Cannot register client %d(%d) in namespace %s",
					       pmixp_info_taskid(i), i, pmixp_info_namespace() );
				return SLURM_ERROR;
			}
		}
	}

	return SLURM_SUCCESS;
}


static uint32_t payload_size_cb(void *buf)
{
    return (uint32_t)PMIx_message_payload_size(buf);
}

static void _auth_send_cb(int sd, void *srv_obj, char *payload, size_t size)
{
	int shutdown;
	pmixp_write_buf(sd, payload, size, &shutdown, true);
}

static void _send_cb(int sd, void *srv_obj, char *payload, size_t size)
{
	int localid = 0;
	pmixp_io_engine_t *eng = NULL;

	for( ; localid < pmixp_state_cli_count(); localid++ ){
		eng = pmixp_state_cli_io(localid);
		if( sd == eng->sd){
			break;
		}
	}
	if( pmixp_state_cli_count() <= localid ){
		// not found
		return;
	}
	void *msg = xmalloc(size);
	memcpy(msg, payload, size);
	pmix_io_send_enqueue(eng, msg);
}

// ----------------------------------- 8< -----------------------------------------------------------//

static bool _peer_readable(eio_obj_t *obj);
static int _peer_read(eio_obj_t *obj, List objs);
static bool _peer_writable(eio_obj_t *obj);
static int _peer_write(eio_obj_t *obj, List objs);
static struct io_operations peer_ops = {
	.readable     = _peer_readable,
	.handle_read  = _peer_read,
	.writable     = _peer_writable,
	.handle_write = _peer_write
};

// Nonblocking message processing
void pmix_client_new_conn(int sd)
{
	int rc;
	eio_obj_t *obj;
	pmixp_io_engine_header_t cli_header;
	int localid, rank;

	cli_header.net_size = PMIx_message_hdr_size();
	cli_header.host_size = PMIx_message_hdr_size();
	cli_header.pack_hdr_cb = cli_header.unpack_hdr_cb = NULL;
	cli_header.pay_size_cb = payload_size_cb;

	PMIXP_DEBUG("New connection on fd = %d", sd);

	/* authenticate the connection */
	if (PMIX_SUCCESS != (rc = PMIx_server_authenticate_client(sd, &rank, _auth_send_cb))) {
		PMIXP_DEBUG("Cannot authentificate new connection: rc = %d", rc);
		return;
	}

	// Setup fd
	fd_set_nonblocking(sd);
	fd_set_close_on_exec(sd);

	// Setup message engine. Push the header we just received to
	// ensure integrity of msgengine
	localid = pmixp_info_taskid2localid(rank);
	pmixp_state_cli_connected(localid);
	pmixp_io_engine_t *me = pmixp_state_cli_io(localid);
	pmix_io_init(me, sd, cli_header);

	obj = eio_obj_create(sd, &peer_ops, (void*)(long)localid);
	eio_new_obj( pmixp_info_io(), obj);
}


static bool _peer_readable(eio_obj_t *obj)
{
	PMIXP_DEBUG("fd = %d", obj->fd);
	if (obj->shutdown == true) {
		if (obj->fd != -1) {
			close(obj->fd);
			obj->fd = -1;
		}
		PMIXP_DEBUG("    false, shutdown");
		return false;
	}
	return true;
}

static int _peer_read(eio_obj_t *obj, List objs)
{
	PMIXP_DEBUG("fd = %d", obj->fd);
	uint32_t localid = (int)(long)(obj->arg);
	pmixp_io_engine_t *eng = pmixp_state_cli_io(localid);
	int rc;

	// Read and process all received messages
	while( 1 ){
		pmix_io_rcvd(eng);
		if( pmix_io_finalized(eng) ){
			PMIXP_DEBUG("Connection with task %d finalized", localid);
			eio_remove_obj(obj, objs);
			// TODO: do some client state cleanup
			break;
		}
		if( pmix_io_rcvd_ready(eng) ){
			char hdr[PMIx_message_hdr_size()];
			void *msg = pmix_io_rcvd_extract(eng, hdr);
			// TODO: process return codes
			rc = PMIx_server_process_msg(eng->sd, hdr, msg, _send_cb);
			if( PMIX_SUCCESS != rc ){
				PMIXP_ERROR("Error processing message: %d", rc);
			}
		}else{
			// No more complete messages
			break;
		}
	}
/* Deal with client connection close.
	// Check if we still have the connection
	if( pmix_io_finalized(eng) ){
		_finalize_client(localid, obj, objs);
	}
*/
	return 0;
}


static bool _peer_writable(eio_obj_t *obj)
{
	PMIXP_DEBUG("fd = %d", obj->fd);
	if (obj->shutdown == true) {
		PMIXP_ERROR("We shouldn't be here if connection shutdowned");
		return false;
	}
	uint32_t taskid = (int)(long)(obj->arg);
	pmixp_io_engine_t *me = pmixp_state_cli_io(taskid);
	if( pmix_io_send_pending(me) )
		return true;
	return false;
}

static int _peer_write(eio_obj_t *obj, List objs)
{
	PMIXP_DEBUG("fd = %d", obj->fd);
	uint32_t taskid = (int)(long)(obj->arg);
	pmixp_io_engine_t *me = pmixp_state_cli_io(taskid);
	pmix_io_send_progress(me);
	return 0;
}

static int finalize_fn(const char nspace[], int rank, void* server_object,
		pmix_op_cbfunc_t cbfunc, void *cbdata)
{
	PMIXP_DEBUG("called");

	/* Have somathing to do with terminated ?
	inline static void pmixp_state_cli_finalize(uint32_t localid)
	{
		pmixp_state_cli_sanity_check(localid);
		client_state_t *cli = &pmixp_state.cli_state[localid];
		cli->sd = -1;
		cli->state = PMIX_CLI_UNCONNECTED;
		if( !pmix_io_finalized( &cli->eng ) ){
			pmix_io_finalize(&cli->eng, 0);
		}
	}


int _finalize_client(uint32_t taskid, eio_obj_t *obj, List objs)
{
	// Don't track this process anymore
	eio_remove_obj(obj, objs);
	// Reset client state
	pmixp_state_cli_finalize(taskid);
	return 0;
}
	*/

	cbfunc(PMIX_SUCCESS, cbdata);

	return PMIX_SUCCESS;
}

static int abort_fn(const char nspace[], int rank, void *server_object,
		    int status, const char msg[],
		    pmix_op_cbfunc_t cbfunc, void *cbdata)
{
	PMIXP_DEBUG("called: status = %d, msg = %s",
		      status, msg);
	if (NULL != cbfunc) {
		cbfunc(PMIX_SUCCESS, cbdata);
	}

	return PMIX_SUCCESS;
}

static int fencenb_fn(const pmix_range_t ranges[], size_t nranges,
		      int collect_data,
		      pmix_modex_cbfunc_t cbfunc, void *cbdata)
{
	PMIXP_DEBUG("called");
	pmixp_coll_t *coll;
	pmixp_coll_type_t type = PMIXP_COLL_TYPE_FENCE;

	if( !collect_data ) {
		type = PMIXP_COLL_TYPE_FENCE_EMPT;
	}
	coll = pmixp_state_coll_find(type, ranges, nranges);

	if( NULL == coll ){
		coll = pmixp_state_coll_new(type, ranges, nranges);
	}

	if( NULL == coll ){
		goto error;
	}

	if( SLURM_SUCCESS != pmixp_coll_contrib_loc(coll, collect_data) ){
		goto error;
	}
	pmixp_coll_set_callback(coll, cbfunc, cbdata);
	return PMIX_SUCCESS;
error:
	cbfunc(PMIX_ERROR,NULL,0,cbdata);
	return PMIX_ERROR;
}

static int store_modex_fn(const char nspace[], int rank, void *server_object,
			  pmix_scope_t scope, pmix_modex_data_t *data)
{
	PMIXP_DEBUG("called: rank = %d", rank);
	pmixp_nspace_add_blob(nspace, scope, rank, data->blob, data->size);

	return PMIX_SUCCESS;
}

static int get_modexnb_fn(const char nspace[], int rank,
			  pmix_modex_cbfunc_t cbfunc, void *cbdata)
{
	PMIXP_DEBUG("called");
	List modex_list = list_create(pmixp_xfree_buffer);
	pmix_modex_data_t *modex_data, *mptr;
	ListIterator it;
	size_t ndata;
	int i, rc;

	// TODO: Data might be missing and we need to wait for them
	rc = pmixp_nspace_rank_blob(nspace, PMIX_LOCAL, rank, modex_list);
	if( SLURM_SUCCESS != rc ){
		cbfunc(PMIX_ERROR, NULL, 0, cbdata);
		return SLURM_ERROR;
	}

	rc = pmixp_nspace_rank_blob(nspace, PMIX_GLOBAL, rank, modex_list);
	if( SLURM_SUCCESS != rc ){
		cbfunc(PMIX_ERROR, NULL, 0, cbdata);
		return SLURM_ERROR;
	}

	rc = pmixp_nspace_rank_blob(nspace, PMIX_REMOTE, rank, modex_list);
	if( SLURM_SUCCESS != rc ){
		cbfunc(PMIX_ERROR, NULL, 0, cbdata);
		return SLURM_ERROR;
	}

	ndata = list_count(modex_list);
	modex_data = xmalloc(ndata * sizeof(*modex_data));
	it = list_iterator_create(modex_list);
	i = 0;
	while( NULL != (mptr = list_next(it) ) ){
		modex_data[i] = *mptr;
		i++;
	}
	list_destroy(modex_list);

	cbfunc(PMIX_SUCCESS, modex_data, ndata, cbdata);
	xfree(modex_data);

	return PMIX_SUCCESS;
}



static int publish_fn(pmix_scope_t scope, pmix_persistence_t persist,
		      const pmix_info_t info[], size_t ninfo,
		      pmix_op_cbfunc_t cbfunc, void *cbdata)
{
	PMIXP_DEBUG("called");
	if (NULL != cbfunc) {
		cbfunc(PMIX_SUCCESS, cbdata);
	}
	return PMIX_SUCCESS;
}

static int lookup_fn(pmix_scope_t scope, int wait, char **keys,
		     pmix_lookup_cbfunc_t cbfunc, void *cbdata)
{
	PMIXP_DEBUG("called");
	if (NULL != cbfunc) {
		cbfunc(PMIX_SUCCESS, NULL, 0, NULL, cbdata);
	}
	return PMIX_SUCCESS;
}

static int unpublish_fn(pmix_scope_t scope, char **keys,
			pmix_op_cbfunc_t cbfunc, void *cbdata)
{
	PMIXP_DEBUG("called");
	if (NULL != cbfunc) {
		cbfunc(PMIX_SUCCESS, cbdata);
	}
	return PMIX_SUCCESS;
}

static int spawn_fn(const pmix_app_t apps[], size_t napps,
		    pmix_spawn_cbfunc_t cbfunc, void *cbdata)
{
	PMIXP_DEBUG("called");
	if (NULL != cbfunc) {
		cbfunc(PMIX_SUCCESS, "foobar", cbdata);
	}
	return PMIX_SUCCESS;
}

static int connect_fn(const pmix_range_t ranges[], size_t nranges,
		      pmix_op_cbfunc_t cbfunc, void *cbdata)
{
	PMIXP_DEBUG("called");
	if (NULL != cbfunc) {
		cbfunc(PMIX_SUCCESS, cbdata);
	}
	return PMIX_SUCCESS;
}

static int disconnect_fn(const pmix_range_t ranges[], size_t nranges,
			 pmix_op_cbfunc_t cbfunc, void *cbdata)
{
	PMIXP_DEBUG("called");
	if (NULL != cbfunc) {
		cbfunc(PMIX_SUCCESS, cbdata);
	}
	return PMIX_SUCCESS;
}

