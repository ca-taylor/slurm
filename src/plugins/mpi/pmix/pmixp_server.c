/*****************************************************************************\
 **  pmix_server.c - PMIx server side functionality
 *****************************************************************************
 *  Copyright (C) 2014-2015 Artem Polyakov. All rights reserved.
 *  Copyright (C) 2015-2017 Mellanox Technologies. All rights reserved.
 *  Written by Artem Polyakov <artpol84@gmail.com, artemp@mellanox.com>.
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
#include "pmixp_info.h"
#include "pmixp_coll.h"
#include "pmixp_debug.h"
#include "pmixp_io.h"
#include "pmixp_client.h"
#include "pmixp_server.h"
#include "pmixp_nspaces.h"
#include "pmixp_state.h"
#include "pmixp_client.h"
#include "pmixp_dmdx.h"
#include "pmixp_conn.h"
#include "pmixp_dconn.h"

#include <pmix_server.h>

/*
 * --------------------- I/O protocol -------------------
 */

/*
#include <time.h>
#define GET_TS ({ \
    struct timespec ts;                     \
    double ret;                             \
    clock_gettime(CLOCK_MONOTONIC, &ts);    \
    ret = ts.tv_sec + 1E-9 * ts.tv_nsec;    \
    ret;                                    \
})
*/
double send_start;

struct my_timings {
    double vals[1024];
    int count;
} serv_read = {{0}}, 
  new_msg = {{0}}, 
  process_req = {{0}}, 
  PP_start = {{0}}, 
  PP_send = {{0}},
  PP_complete = {{0}},
  PP_inc = {{0}};   

#define PMIXP_SERVER_MSG_MAGIC 0xCAFECA11
typedef struct {
	uint32_t magic;
	uint32_t type;
	uint32_t seq;
	uint32_t nodeid;
	uint32_t msgsize;
} pmixp_base_hdr_t;

#define PMIXP_BASE_HDR_SIZE (5 * sizeof(uint32_t))

typedef struct {
	pmixp_base_hdr_t base_hdr;
	uint16_t rport;		/* STUB: remote port for persistent connection */
} pmixp_slurm_shdr_t;
#define PMIXP_SLURM_API_SHDR_SIZE (PMIXP_BASE_HDR_SIZE + sizeof(uint16_t))

#define PMIXP_MAX_SEND_HDR PMIXP_SLURM_API_SHDR_SIZE

typedef struct {
	uint32_t size;		/* Has to be first (appended by SLURM API) */
	pmixp_slurm_shdr_t shdr;
} pmixp_slurm_rhdr_t;
#define PMIXP_SLURM_API_RHDR_SIZE (sizeof(uint32_t) + PMIXP_SLURM_API_SHDR_SIZE)

#define PMIXP_SERVER_BUF_MAGIC 0xCA11CAFE
Buf pmixp_server_buf_new(void)
{
	Buf buf = create_buf(xmalloc(PMIXP_MAX_SEND_HDR), PMIXP_MAX_SEND_HDR);
#ifndef NDEBUG
	/* Makesure that we only use buffers allocated through
	 * this call, because we reserve the space for the
	 * header here
	 */
	xassert( PMIXP_MAX_SEND_HDR >= sizeof(uint32_t));
	uint32_t tmp = PMIXP_SERVER_BUF_MAGIC;
	pack32(tmp, buf);
#endif

	/* Skip header. It will be filled right before the sending */
	set_buf_offset(buf, PMIXP_MAX_SEND_HDR);
	return buf;
}

size_t pmixp_server_buf_reset(Buf buf)
{
#ifndef NDEBUG
	xassert( PMIXP_MAX_SEND_HDR <= get_buf_offset(buf) );
	set_buf_offset(buf,0);
	/* Restore the protection magic number
	 */
	uint32_t tmp = PMIXP_SERVER_BUF_MAGIC;
	pack32(tmp, buf);
#endif
	set_buf_offset(buf, PMIXP_MAX_SEND_HDR);
	return PMIXP_MAX_SEND_HDR;
}


static void *_buf_finalize(Buf buf, void *nhdr, size_t hsize,
			  size_t *dsize)
{
	char *ptr = get_buf_data(buf);
	size_t offset = PMIXP_MAX_SEND_HDR - hsize;
#ifndef NDEBUG
	Buf tbuf = create_buf(ptr, get_buf_offset(buf));
	xassert(PMIXP_MAX_SEND_HDR >= hsize);
	xassert(PMIXP_MAX_SEND_HDR <= get_buf_offset(buf));
	uint32_t tmp;
	unpack32(&tmp, tbuf);
	xassert(PMIXP_SERVER_BUF_MAGIC == tmp);
	tbuf->head = NULL;
	free_buf(tbuf);
#endif
	/* Enough space for any header was reserved at the
	 * time of buffer initialization in `pmixp_server_new_buf`
	 * put the header in place and return proper pointer
	 */
	if( 0 != hsize ){
		memcpy(ptr + offset, nhdr, hsize);
	}
	*dsize = get_buf_offset(buf) - offset;
	return ptr + offset;
}

static void _base_hdr_pack(Buf packbuf, pmixp_base_hdr_t *hdr)
{
	pack32(hdr->magic, packbuf);
	pack32(hdr->type, packbuf);
	pack32(hdr->seq, packbuf);
	pack32(hdr->nodeid, packbuf);
	pack32(hdr->msgsize, packbuf);
}

static int _base_hdr_unpack(Buf packbuf, pmixp_base_hdr_t *hdr)
{
	if (unpack32(&hdr->magic, packbuf)) {
		return -EINVAL;
	}
	xassert(PMIXP_SERVER_MSG_MAGIC == hdr->magic);

	if (unpack32(&hdr->type, packbuf)) {
		return -EINVAL;
	}

	if (unpack32(&hdr->seq, packbuf)) {
		return -EINVAL;
	}

	if (unpack32(&hdr->nodeid, packbuf)) {
		return -EINVAL;
	}

	if (unpack32(&hdr->msgsize, packbuf)) {
		return -EINVAL;
	}
	return 0;
}

static void _slurm_hdr_pack(Buf packbuf, pmixp_slurm_shdr_t *hdr)
{
	_base_hdr_pack(packbuf, &hdr->base_hdr);
	pack16(hdr->rport, packbuf);
}

static int _slurm_hdr_unpack(Buf packbuf, pmixp_slurm_rhdr_t *hdr)
{
	if (unpack32(&hdr->size, packbuf)) {
		return -EINVAL;
	}

	if (_base_hdr_unpack(packbuf, &hdr->shdr.base_hdr)) {
		return -EINVAL;
	}

	if (unpack16(&hdr->shdr.rport, packbuf)) {
		return -EINVAL;
	}

	return 0;
}

/* SLURM protocol I/O header */
static uint32_t _slurm_proto_msize(void *buf);
static int _slurm_pack_hdr(void *host, void *net);
static int _slurm_proto_unpack_hdr(void *net, void *host);
static void _slurm_new_msg(pmixp_conn_t *conn,
			   void *_hdr, void *msg);
static int _slurm_send(pmixp_ep_t *ep,
		       pmixp_base_hdr_t bhdr, Buf buf);

pmixp_io_engine_header_t _slurm_proto = {
	/* generic callbacks */
	.payload_size_cb = _slurm_proto_msize,
	/* receiver-related fields */
	.recv_on = 1,
	.recv_host_hsize = sizeof(pmixp_slurm_rhdr_t),
	.recv_net_hsize = PMIXP_SLURM_API_RHDR_SIZE, /*need to skip user ID*/
	.recv_padding = sizeof(uint32_t),
	.hdr_unpack_cb = _slurm_proto_unpack_hdr,
};

/* direct protocol I/O header */
static uint32_t _direct_msize(void *hdr);
static int _direct_hdr_pack(void *host, void *net);
static int _direct_hdr_unpack(void *net, void *host);
static void *_direct_hdr_ptr(void *msg);
static void *_direct_payload_ptr(void *msg);
static void _direct_msg_free(void *msg);
static void _direct_new_msg(pmixp_conn_t *conn, void *_hdr, void *msg);
static void _direct_send(pmixp_dconn_t *dconn, pmixp_ep_t *ep,
			 pmixp_base_hdr_t bhdr, Buf buf,
			pmixp_server_sent_cb_t complete_cb, void *cb_data);


pmixp_io_engine_header_t _direct_proto = {
	/* generic callback */
	.payload_size_cb = _direct_msize,
	/* receiver-related fields */
	.recv_on = 1,
	.recv_host_hsize = sizeof(pmixp_base_hdr_t),
	.recv_net_hsize = PMIXP_BASE_HDR_SIZE,
	.recv_padding = 0, /* no padding for the direct proto */
	.hdr_unpack_cb = _direct_hdr_unpack,
	/* transmitter-related fields */
	.send_on = 1,
	.send_host_hsize = sizeof(pmixp_base_hdr_t),
	.send_net_hsize = PMIXP_BASE_HDR_SIZE,
	.hdr_pack_cb = _direct_hdr_pack,
	.hdr_ptr_cb = _direct_hdr_ptr,
	.payload_ptr_cb = _direct_payload_ptr,
	.msg_free_cb = _direct_msg_free
};


/*
 * --------------------- Initi/Finalize -------------------
 */

static volatile int _was_initialized = 0;

int pmixp_stepd_init(const stepd_step_rec_t *job, char ***env)
{
	char *path;
	int fd, rc;
	uint16_t port;

	if (SLURM_SUCCESS != (rc = pmixp_info_set(job, env))) {
		PMIXP_ERROR("pmixp_info_set(job, env) failed");
		goto err_info;
	}

	/* Create UNIX socket for slurmd communication */
	path = pmixp_info_nspace_usock(pmixp_info_namespace());
	if (NULL == path) {
		PMIXP_ERROR("pmixp_info_nspace_usock: out-of-memory");
		rc = SLURM_ERROR;
		goto err_path;
	}
	if ((fd = pmixp_usock_create_srv(path)) < 0) {
		PMIXP_ERROR("pmixp_usock_create_srv");
		rc = SLURM_ERROR;
		goto err_usock;
	}
	fd_set_close_on_exec(fd);
	pmixp_info_srv_usock_set(path, fd);


	/* Create TCP socket for slurmd communication */
	if (0 > net_stream_listen(&fd, &port)) {
		PMIXP_ERROR("net_stream_listen");
		goto err_tsock;
	}
	pmixp_info_srv_tsock_set(port, fd);

	pmixp_conn_init(_slurm_proto, _direct_proto);
	pmixp_dconn_init(pmixp_info_nodes(), _direct_proto);

	if (SLURM_SUCCESS != (rc = pmixp_nspaces_init())) {
		PMIXP_ERROR("pmixp_nspaces_init() failed");
		goto err_nspaces;
	}

	if (SLURM_SUCCESS != (rc = pmixp_state_init())) {
		PMIXP_ERROR("pmixp_state_init() failed");
		goto err_state;
	}

	if (SLURM_SUCCESS != (rc = pmixp_dmdx_init())) {
		PMIXP_ERROR("pmixp_dmdx_init() failed");
		goto err_dmdx;
	}

	if (SLURM_SUCCESS != (rc = pmixp_libpmix_init())) {
		PMIXP_ERROR("pmixp_libpmix_init() failed");
		goto err_lib;
	}

	if (SLURM_SUCCESS != (rc = pmixp_libpmix_job_set())) {
		PMIXP_ERROR("pmixp_libpmix_job_set() failed");
		goto err_job;
	}

	xfree(path);
	_was_initialized = 1;
	return SLURM_SUCCESS;

err_job:
	pmixp_libpmix_finalize();
err_lib:
	pmixp_dmdx_finalize();
err_dmdx:
	pmixp_state_finalize();
err_state:
	pmixp_nspaces_finalize();
err_nspaces:
	close(pmixp_info_srv_tsock_fd());
err_tsock:
	close(pmixp_info_srv_usock_fd());
err_usock:
	xfree(path);
err_path:
	pmixp_info_free();
err_info:
	return rc;
}

int pmixp_stepd_finalize(void)
{
	char *path;
	if (!_was_initialized) {
		/* nothing to do */
		return 0;
	}

	pmixp_conn_fini();
	pmixp_dconn_fini();

	pmixp_libpmix_finalize();
	pmixp_dmdx_finalize();
	pmixp_state_finalize();
	pmixp_nspaces_finalize();

	/* close TCP socket */
	close(pmixp_info_srv_tsock_fd());

	/* cleanup the UNIX socket */
	PMIXP_DEBUG("Remove PMIx plugin usock");
	close(pmixp_info_srv_usock_fd());
	path = pmixp_info_nspace_usock(pmixp_info_namespace());
	unlink(path);
	xfree(path);

	/* free the information */
	pmixp_info_free();
	return SLURM_SUCCESS;
}

void pmixp_server_cleanup(void)
{
	pmixp_conn_cleanup();
}

/*
 * --------------------- Generic I/O functionality -------------------
 */

static bool _serv_readable(eio_obj_t *obj);
static int _serv_read(eio_obj_t *obj, List objs);
static bool _serv_writable(eio_obj_t *obj);
static int _serv_write(eio_obj_t *obj, List objs);
static void _process_server_request(pmixp_base_hdr_t *hdr, void *payload);


static struct io_operations slurm_peer_ops = {
	.readable = _serv_readable,
	.handle_read = _serv_read
};

static struct io_operations direct_peer_ops = {
	.readable	= _serv_readable,
	.handle_read	= _serv_read,
	.writable	= _serv_writable,
	.handle_write	= _serv_write
};


double serv_read_delay = 0, serv_write_delay = 0;


static bool _serv_readable(eio_obj_t *obj)
{
	/* sanity check */
	xassert(NULL != obj );
	if( obj->shutdown ){
		if (obj->fd != -1) {
			close(obj->fd);
			obj->fd = -1;
		}
		return false;
	}
/*
    if( 0 != serv_read_delay ){
        PMIXP_ERROR("Diff: %lf", GET_TS - serv_read_delay);
    }
    serv_read_delay = GET_TS;
*/

	return true;
}

static int _serv_read(eio_obj_t *obj, List objs)
{
    static int count = 0;
	/* sanity check */
	xassert(NULL != obj );
	/* We should delete connection right when it  was closed or failed */
	xassert(false == obj->shutdown);

    serv_read.vals[serv_read.count++] = GET_TS();

	PMIXP_DEBUG("fd = %d", obj->fd);
	pmixp_conn_t *conn = (pmixp_conn_t *)obj->arg;
	bool proceed = true;

	/* debug stub */
	pmixp_debug_hang(0);

	/* Read and process all received messages */
	while (proceed) {
		if( !pmixp_conn_progress_rcv(conn) ){
			proceed = 0;
		}
		if( !pmixp_conn_is_alive(conn) ){
			obj->shutdown = true;
			PMIXP_DEBUG("Connection closed fd = %d", obj->fd);
			/* cleanup after this connection */
			eio_remove_obj(obj, objs);
			pmixp_conn_return(conn);
			proceed = 0;
		}
	}

//PMIXP_ERROR("_serv_read(): %lf", GET_TS - start);
/*
    if( 0 != serv_read_delay && 0.01 < (GET_TS - serv_read_delay)){
        PMIXP_ERROR("count = %d ts = %lf Diff: %lf / %lf", count, GET_TS, GET_TS - serv_read_delay,
                        GET_TS - start);
    }
*/
    count++;
	return 0;
}

static bool _serv_writable(eio_obj_t *obj)
{
    bool ret;
	/* sanity check */
	xassert(NULL != obj );
	/* We should delete connection right when it  was closed or failed */
	if( obj->shutdown ){
		if (obj->fd != -1) {
			close(obj->fd);
			obj->fd = -1;
		}
		ret = false;
		goto exit;
	}

	/* get I/O engine */
	pmixp_conn_t *conn = (pmixp_conn_t *)obj->arg;
	pmixp_io_engine_t *eng = conn->eng;

	/* debug stub */
	pmixp_debug_hang(0);
    
    /* Invoke cleanup callbacks if any */
    pmixp_io_send_cleanup(eng);
    
	/* check if we have something to send */
	if( pmixp_io_send_pending(eng) ){
		ret = true;
		goto exit;
	}
    ret = false;


exit:
/*
    if( 0 != serv_write_delay ){
        PMIXP_ERROR("Diff: [%d] %lf", (int)ret, GET_TS - serv_write_delay);
    }
    serv_write_delay = GET_TS;
*/
    return ret;

}

static int _serv_write(eio_obj_t *obj, List objs)
{
	/* sanity check */
	static int count = 0;
	xassert(NULL != obj );
	/* We should delete connection right when it  was closed or failed */
	xassert(false == obj->shutdown);

	PMIXP_DEBUG("fd = %d", obj->fd);
	pmixp_conn_t *conn = (pmixp_conn_t *)obj->arg;

	/* debug stub */
	pmixp_debug_hang(0);

	/* progress sends */
	pmixp_conn_progress_snd(conn);

	/* if we are done with this connection - remove it */
	if( !pmixp_conn_is_alive(conn) ){
		obj->shutdown = true;
		PMIXP_DEBUG("Connection finalized fd = %d", obj->fd);
		/* cleanup after this connection */
		eio_remove_obj(obj, objs);
		pmixp_conn_return(conn);
	}
/*
    if( 0 != serv_write_delay && (GET_TS - serv_write_delay)> 0.01){
        PMIXP_ERROR("[%d] Diff: %lf", count, GET_TS - serv_write_delay);
    }
*/
count++;
	return 0;
}

static volatile int pingpong_count = 0;

int pmixp_server_ppcount()
{
	return pingpong_count;
}

struct pp_cbdata
{
    Buf buf;
    double start;
    int size;
} cbdata;

void pingpong_complete(int rc, pmixp_srv_cb_context_t ctx, void *data)
{
    struct pp_cbdata *d = (struct pp_cbdata*)data;
    free_buf(d->buf);
    PP_complete.vals[PP_complete.count++] = GET_TS();
}

int pmixp_server_pingpong(const char *host, int size)
{
    PP_start.vals[PP_start.count++] = GET_TS();
	Buf buf = pmixp_server_buf_new();
	int rc;
	pmixp_ep_t ep;

	grow_buf(buf, size);
	ep.type = PMIXP_EP_HNAME;
	ep.ep.hostname = (char*)host;
	cbdata.buf = buf;
	cbdata.start = GET_TS();
	cbdata.size = size;
//	packdouble(cbdata.start, buf);
	set_buf_offset(buf,get_buf_offset(buf) + size);

    PP_send.vals[PP_send.count++] = GET_TS();

    rc = pmixp_server_send_nb(&ep, PMIXP_MSG_PINGPONG,
			    pingpong_count, buf, pingpong_complete, (void*)&cbdata);
	if (SLURM_SUCCESS != rc) {
		PMIXP_ERROR("Was unable to wait for the parent %s to become alive", host);
	}
	
	double norm = 1487117625;

    if( (11 == pingpong_count) ){
        int i = 1;
        int flag = 1;
        while(flag){
            flag = 0;
            if(i < serv_read.count ){
                PMIXP_ERROR("_serv_read: %lf", serv_read.vals[i] - norm);
                flag++;
            }
            if( (i-1) < new_msg.count ){
                PMIXP_ERROR("_new_msg: %lf", new_msg.vals[i-1] - norm);
                flag++;
            }

            if( i < process_req.count ){
                PMIXP_ERROR("_serv_req: %lf", process_req.vals[i] - norm);
                flag++;
            }
            if( i < PP_start.count ){
                PMIXP_ERROR("_pp_start: %lf", PP_start.vals[i] - norm);
                flag++;
            }

            if( i < PP_send.count ){
                PMIXP_ERROR("_pp_send: %lf", PP_send.vals[i] - norm);
                flag++;
            }

            if( i < PP_inc.count ){
                PMIXP_ERROR("_pp_inc: %lf", PP_inc.vals[i] - norm);
                flag++;
            }
            if( i < PP_complete.count ){
                PMIXP_ERROR("_pp_complete: %lf", PP_complete.vals[i] - norm);
                flag++;
            }
            i++;
        }
    }
	return rc;
}

static void _process_server_request(pmixp_base_hdr_t *hdr, void *payload)
{
    process_req.vals[process_req.count++] = GET_TS();
        

	char *nodename = pmixp_info_job_host(hdr->nodeid);
	Buf buf = create_buf(payload, hdr->msgsize);
	int rc;
	if( NULL == nodename ){
		goto exit;
	}

	switch (hdr->type) {
	case PMIXP_MSG_FAN_IN:
	case PMIXP_MSG_FAN_OUT: {
		pmixp_coll_t *coll;
		pmix_proc_t *procs = NULL;
		size_t nprocs = 0;
		pmixp_coll_type_t type = 0;

		rc = pmixp_coll_unpack_ranges(buf, &type, &procs, &nprocs);
		if (SLURM_SUCCESS != rc) {
			PMIXP_ERROR("Bad message header from node %s", nodename);
			goto exit;
		}
		coll = pmixp_state_coll_get(type, procs, nprocs);
		xfree(procs);

		PMIXP_DEBUG("FENCE collective message from node \"%s\", type = %s, seq = %d",
			    nodename, (PMIXP_MSG_FAN_IN == hdr->type) ? "fan-in" : "fan-out",
			    hdr->seq);
		rc = pmixp_coll_check_seq(coll, hdr->seq, nodename);
		if (PMIXP_COLL_REQ_FAILURE == rc) {
			/* this is unexepable event: either something went
			 * really wrong or the state machine is incorrect.
			 * This will 100% lead to application hang.
			 */
			PMIXP_ERROR("Bad collective seq. #%d from %s, current is %d",
				    hdr->seq, nodename, coll->seq);
			pmixp_debug_hang(0); /* enable hang to debug this! */
			slurm_kill_job_step(pmixp_info_jobid(), pmixp_info_stepid(),
					    SIGKILL);

			break;
		} else if (PMIXP_COLL_REQ_SKIP == rc) {
			PMIXP_DEBUG("Wrong collective seq. #%d from %s, current is %d, skip this message",
				    hdr->seq, nodename, coll->seq);
			goto exit;
		}

		if (PMIXP_MSG_FAN_IN == hdr->type) {
			pmixp_coll_contrib_node(coll, nodename, buf);
			goto exit;
		} else {
			coll->root_buf = buf;
			pmixp_coll_bcast(coll);
			/* buf will be free'd by the PMIx callback so protect the data by
			 * voiding the buffer.
			 * Use the statement below instead of (buf = NULL) to maintain
			 * incapsulation - in general `buf` is not a pointer, but opaque type.
			 */
			buf = create_buf(NULL, 0);
		}

		break;
	}
	case PMIXP_MSG_DMDX: {
		pmixp_dmdx_process(buf, nodename, hdr->seq);
		/* buf will be free'd by pmixp_dmdx_process or the PMIx callback so
		 * protect the data by voiding the buffer.
		 * Use the statement below instead of (buf = NULL) to maintain
		 * incapsulation - in general `buf` is not a pointer, but opaque type.
		 */
		buf = create_buf(NULL, 0);
		break;
	}
	case PMIXP_MSG_PINGPONG: {
		/* this is just health ping.
		 * TODO: can we do something more sophisticated?
		 */
/*
        double start;
        unpackdouble(&start, buf);
        PMIXP_ERROR("Delivery time: %lf", GET_TS - start);
*/

		if( pmixp_info_nodeid() == 1 ){
  //          PMIXP_ERROR("reply: %lf", GET_TS());
			pmixp_server_pingpong(pmixp_info_job_host(0), hdr->msgsize);
		}

        PP_inc.vals[PP_inc.count++] = GET_TS();
		pingpong_count++;

		break;
	}
	default:
		PMIXP_ERROR("Unknown message type %d", hdr->type);
		break;
	}

exit:
	free_buf(buf);
	if( NULL != nodename ){
		xfree(nodename);
	}
}

void pmixp_server_sent_buf_cb(int rc, pmixp_srv_cb_context_t ctx, void *data)
{
	Buf buf = (Buf)data;
	free_buf(buf);
	return;
}

int pmixp_server_send_nb(pmixp_ep_t *ep, pmixp_srv_cmd_t type,
			 uint32_t seq, Buf buf,
			 pmixp_server_sent_cb_t complete_cb,
			 void *cb_data)
{
	pmixp_base_hdr_t bhdr;
	int rc = SLURM_ERROR;
	pmixp_dconn_t *dconn = NULL;

	bhdr.magic = PMIXP_SERVER_MSG_MAGIC;
	bhdr.type = type;
	bhdr.msgsize = get_buf_offset(buf) - PMIXP_MAX_SEND_HDR;
	bhdr.seq = seq;
	/* Store global nodeid that is
	 *  independent from exact collective */
	bhdr.nodeid = pmixp_info_nodeid_job();

	/* if direct connection is not enabled
	 * always use SLURM protocol
	 */
	if (!pmixp_info_srv_direct_conn()) {
		goto send_slurm;
	}

	switch (ep->type) {
	case PMIXP_EP_HLIST:
		goto send_slurm;
	case PMIXP_EP_HNAME:{
		int hostid = pmixp_info_job_hostid(ep->ep.hostname);
		xassert(0 <= hostid);
		dconn = pmixp_dconn_lock(hostid);
		switch (pmixp_dconn_state(dconn)) {
		case PMIXP_DIRECT_PORT_SENT:
		case PMIXP_DIRECT_CONNECTED:
			/* keep the lock here and proceed
			 * to the direct send
			 */
			goto send_direct;
		case PMIXP_DIRECT_INIT:
			pmixp_dconn_req_sent(dconn);
			pmixp_dconn_unlock(dconn);
			goto send_slurm;
		default:{
			pmixp_dconn_unlock(dconn);
			/* this is a bug! */
			pmixp_dconn_state_t state = pmixp_dconn_state(dconn);
			PMIXP_ERROR("Bad direct connection state: %d",
				    (int)pmixp_dconn_state(dconn));
			xassert( (state == PMIXP_DIRECT_INIT) ||
				 (state == PMIXP_DIRECT_PORT_SENT) ||
				 (state == PMIXP_DIRECT_CONNECTED) );
			abort();
		}
		}
	}
	default:
		PMIXP_ERROR("Bad value of the endpoint type: %d",
			    (int)ep->type);
		xassert( PMIXP_EP_HLIST == ep->type ||
			 PMIXP_EP_HNAME == ep->type);
		abort();
	}

	return rc;
send_slurm:
	rc = _slurm_send(ep, bhdr, buf);
	complete_cb(rc, PMIXP_SRV_CB_INLINE, cb_data);
	return SLURM_SUCCESS;
send_direct:
	xassert( NULL != dconn );
	_direct_send(dconn, ep, bhdr, buf, complete_cb, cb_data);
	pmixp_dconn_unlock(dconn);
	return SLURM_SUCCESS;
}

/*
 * ------------------- DIRECT communication protocol -----------------------
 */


typedef struct {
	pmixp_base_hdr_t hdr;
	void *payload;
	Buf buf_ptr;
	pmixp_server_sent_cb_t sent_cb;
	void *cbdata;
}_direct_proto_message_t;

/*
 *  Server message processing
 */

static uint32_t _direct_msize(void *buf)
{
	pmixp_base_hdr_t *hdr = (pmixp_base_hdr_t *)buf;
	return hdr->msgsize;
}

/*
 * Unpack message header.
 * Returns 0 on success and -errno on failure
 * Note: asymmetric to _send_pack_hdr because of additional SLURM header
 */
static int _direct_hdr_unpack(void *net, void *host)
{
	pmixp_base_hdr_t *hdr = (pmixp_base_hdr_t *)host;
	Buf packbuf = create_buf(net, PMIXP_BASE_HDR_SIZE);

	if (_base_hdr_unpack(packbuf,hdr)) {
		return -EINVAL;
	}

	/* free the Buf packbuf, but not the memory it points to */
	packbuf->head = NULL;
	free_buf(packbuf);
	return 0;
}

/*
 * Pack message header.
 * Returns packed size
 * Note: asymmetric to _recv_unpack_hdr because of additional SLURM header
 */
static int _direct_hdr_pack(void *host, void *net)
{
	pmixp_base_hdr_t *hdr = (pmixp_base_hdr_t *)host;
	Buf packbuf = create_buf(net, PMIXP_BASE_HDR_SIZE);
	int size = 0;
	_base_hdr_pack(packbuf, hdr);
	size = get_buf_offset(packbuf);
	xassert(size == PMIXP_BASE_HDR_SIZE);
	/* free the Buf packbuf, but not the memory it points to */
	packbuf->head = NULL;
	free_buf(packbuf);
	return size;
}

/*
 * Get te pointer to the message header.
 * Returns packed size
 * Note: asymmetric to _recv_unpack_hdr because of additional SLURM header
 */
static void *_direct_hdr_ptr(void *msg)
{
	_direct_proto_message_t *_msg = (_direct_proto_message_t*)msg;
	return &_msg->hdr;
}

static void *_direct_payload_ptr(void *msg)
{
	_direct_proto_message_t *_msg = (_direct_proto_message_t*)msg;
	return _msg->payload;
}

static void _direct_msg_free(void *_msg)
{
	_direct_proto_message_t *msg = (_direct_proto_message_t*)_msg;
	msg->sent_cb(SLURM_SUCCESS, PMIXP_SRV_CB_REGULAR, msg->cbdata);
	xfree(msg);
}

/*
 * See process_handler_t prototype description
 * on the details of this function output values
 */
static void _direct_new_msg(pmixp_conn_t *conn, void *_hdr, void *msg)
{
	pmixp_base_hdr_t *hdr = (pmixp_base_hdr_t*)_hdr;

    new_msg.vals[new_msg.count++] = GET_TS();

	_process_server_request(hdr, msg);
}

/* Process direct connection closure
 */

static void _direct_return_connection(pmixp_conn_t *conn)
{
	pmixp_dconn_t *dconn = (pmixp_dconn_t *)pmixp_conn_get_data(conn);
	pmixp_dconn_lock(dconn->nodeid);
	pmixp_dconn_disconnect(dconn);
	pmixp_dconn_unlock(dconn);
}

/*
 * Receive the first message identifying initiator
 */
static void
_direct_conn_establish(pmixp_conn_t *conn, void *_hdr, void *msg)
{
	pmixp_io_engine_t *eng = pmixp_conn_get_eng(conn);
	pmixp_base_hdr_t *hdr = (pmixp_base_hdr_t *)_hdr;
	pmixp_dconn_t *dconn = NULL;
	pmixp_conn_t *new_conn;
	eio_obj_t *obj;
	int fd;

	xassert(0 == hdr->msgsize);
	fd = pmixp_io_detach(eng);
	dconn = pmixp_dconn_accept(hdr->nodeid, fd);

	if( NULL == dconn ){
		/* connection was refused because we already
			 * have established connection
			 * It seems that some sort of race condition occured
			 */
	}
	new_conn = pmixp_conn_new_persist(PMIXP_PROTO_DIRECT, pmixp_dconn_engine(dconn),
				      _direct_new_msg, _direct_return_connection, dconn);
	pmixp_dconn_unlock(dconn);
	obj = eio_obj_create(fd, &direct_peer_ops, (void *)new_conn);
	eio_new_obj(pmixp_info_io(), obj);
	/* wakeup this connection to get processed */
	eio_signal_wakeup(pmixp_info_io());
}

void pmixp_server_direct_conn(int fd)
{
	eio_obj_t *obj;
	pmixp_conn_t *conn;
	PMIXP_DEBUG("Request from fd = %d", fd);

	/* Set nonblocking */
	fd_set_nonblocking(fd);
	fd_set_close_on_exec(fd);
	pmixp_fd_set_nodelay(fd);
	conn = pmixp_conn_new_temp(PMIXP_PROTO_DIRECT, fd, _direct_conn_establish);

	/* try to process right here */
	pmixp_conn_progress_rcv(conn);
	if (!pmixp_conn_is_alive(conn)) {
		/* success, don't need this connection anymore */
		pmixp_conn_return(conn);
		return;
	}

	/* If it is a blocking operation: create AIO object to
	 * handle it */
	obj = eio_obj_create(fd, &direct_peer_ops, (void *)conn);
	eio_new_obj(pmixp_info_io(), obj);
	/* wakeup this connection to get processed */
	eio_signal_wakeup(pmixp_info_io());
}

static void
_direct_send(pmixp_dconn_t *dconn, pmixp_ep_t *ep,
			 pmixp_base_hdr_t bhdr, Buf buf,
			pmixp_server_sent_cb_t complete_cb, void *cb_data)
{
	size_t dsize = 0;
	int rc;

	xassert(PMIXP_EP_HNAME == ep->type);
	/* TODO: I think we can avoid locking */
	_direct_proto_message_t *msg = xmalloc(sizeof(*msg));
	msg->sent_cb = complete_cb;
	msg->cbdata = cb_data;
	msg->hdr = bhdr;
	msg->payload = _buf_finalize(buf, NULL, 0, &dsize);
	msg->buf_ptr = buf;
	
	
	rc = pmixp_dconn_send(dconn, msg);
	if (SLURM_SUCCESS != rc) {
		msg->sent_cb(rc, PMIXP_SRV_CB_INLINE, msg->cbdata);
		xfree( msg );
	}
	eio_signal_wakeup(pmixp_info_io());
}

/*
 * ------------------- SLURM communication protocol -----------------------
 */

/*
 * See process_handler_t prototype description
 * on the details of this function output values
 */
static void _slurm_new_msg(pmixp_conn_t *conn,
			   void *_hdr, void *msg)
{
	pmixp_slurm_rhdr_t *hdr = (pmixp_slurm_rhdr_t *)_hdr;

	if( 0 != hdr->shdr.rport ){
		pmixp_dconn_t *dconn;
		dconn = pmixp_dconn_connect(hdr->shdr.base_hdr.nodeid,
					    hdr->shdr.rport);
		if( NULL != dconn ){
			pmixp_conn_t *conn;
			conn = pmixp_conn_new_persist(PMIXP_PROTO_DIRECT,
						      pmixp_dconn_engine(dconn),
						      _direct_new_msg,
						      _direct_return_connection,
						      dconn);
			if( NULL != conn ){
				eio_obj_t *obj;
				obj = eio_obj_create(pmixp_dconn_fd(dconn),
						     &direct_peer_ops,
						     (void *)conn);
				eio_new_obj(pmixp_info_io(), obj);
				eio_signal_wakeup(pmixp_info_io());
				pmixp_dconn_unlock(dconn);

				pmixp_ep_t ep;
				ep.type = PMIXP_EP_HNAME;
				ep.ep.hostname = pmixp_info_job_host(hdr->shdr.base_hdr.nodeid);
				Buf buf = pmixp_server_buf_new();
				pmixp_server_send_nb(&ep, PMIXP_MSG_INIT_DIRECT, 0, buf,
						     pmixp_server_sent_buf_cb, buf);
				xfree(ep.ep.hostname);
			} else {
				pmixp_dconn_unlock(dconn);
			}
		}
	}
	_process_server_request(&hdr->shdr.base_hdr, msg);
}


/*
 * TODO: we need to keep track of the "me"
 * structures created here, because we need to
 * free them in "pmixp_stepd_finalize"
 */
void pmixp_server_slurm_conn(int fd)
{
	eio_obj_t *obj;
	pmixp_conn_t *conn = NULL;

	PMIXP_DEBUG("Request from fd = %d", fd);
	pmixp_debug_hang(0);

	/* Set nonblocking */
	fd_set_nonblocking(fd);
	fd_set_close_on_exec(fd);
	conn = pmixp_conn_new_temp(PMIXP_PROTO_SLURM, fd, _slurm_new_msg);

	/* try to process right here */
	pmixp_conn_progress_rcv(conn);
	if (!pmixp_conn_is_alive(conn)) {
		/* success, don't need this connection anymore */
		pmixp_conn_return(conn);
		return;
	}

	/* If it is a blocking operation: create AIO object to
	 * handle it */
	obj = eio_obj_create(fd, &slurm_peer_ops, (void *)conn);
	eio_new_obj(pmixp_info_io(), obj);
}

/*
 *  Server message processing
 */

static uint32_t _slurm_proto_msize(void *buf)
{
pmixp_slurm_rhdr_t *ptr = (pmixp_slurm_rhdr_t *)buf;
	pmixp_base_hdr_t *hdr = &ptr->shdr.base_hdr;
	xassert(ptr->size == hdr->msgsize + PMIXP_SLURM_API_SHDR_SIZE);
	xassert(hdr->magic == PMIXP_SERVER_MSG_MAGIC);
	return hdr->msgsize;
}

/*
 * Pack message header.
 * Returns packed size
 * Note: asymmetric to _recv_unpack_hdr because of additional SLURM header
 */
static int _slurm_pack_hdr(void *host, void *net)
{
	pmixp_slurm_shdr_t *shdr = (pmixp_slurm_shdr_t *)host;
	Buf packbuf = create_buf(net, PMIXP_SLURM_API_SHDR_SIZE);
	int size = 0;

	_slurm_hdr_pack(packbuf, shdr);
	size = get_buf_offset(packbuf);
	xassert(size == PMIXP_SLURM_API_SHDR_SIZE);
	/* free the Buf packbuf, but not the memory it points to */
	packbuf->head = NULL;
	free_buf(packbuf);
	return size;
}

/*
 * Unpack message header.
 * Returns 0 on success and -errno on failure
 * Note: asymmetric to _send_pack_hdr because of additional SLURM header
 */
static int _slurm_proto_unpack_hdr(void *net, void *host)
{
	pmixp_slurm_rhdr_t *rhdr = (pmixp_slurm_rhdr_t *)host;
	Buf packbuf = create_buf(net, PMIXP_SLURM_API_RHDR_SIZE);
	if (_slurm_hdr_unpack(packbuf, rhdr) ) {
		return -EINVAL;
	}
	/* free the Buf packbuf, but not the memory it points to */
	packbuf->head = NULL;
	free_buf(packbuf);

	return 0;
}

static int _slurm_send(pmixp_ep_t *ep, pmixp_base_hdr_t bhdr, Buf buf)
{
	const char *addr = NULL, *data = NULL, *hostlist = NULL;
	pmixp_slurm_shdr_t hdr;
	char nhdr[sizeof(hdr)];
	size_t hsize = 0, dsize = 0;
	int rc;

	/* setup the header */
	hdr.base_hdr = bhdr;
	addr = pmixp_info_srv_usock_path();

	hdr.rport = 0;
	if (pmixp_info_srv_direct_conn() && PMIXP_EP_HNAME == ep->type) {
		hdr.rport = pmixp_info_srv_tsock_port();
	}

	hsize = _slurm_pack_hdr(&hdr, nhdr);
	data = _buf_finalize(buf, nhdr, hsize, &dsize);

	switch( ep->type ){
	case PMIXP_EP_HLIST:
		hostlist = ep->ep.hostlist;
		rc = pmixp_stepd_send(ep->ep.hostlist, addr,
				 data, dsize, 500, 7, 0);
		break;
	case PMIXP_EP_HNAME:
		hostlist = ep->ep.hostname;
		rc = pmixp_p2p_send(ep->ep.hostname, addr,
			       data, dsize, 500, 7, 0);
		break;
	default:
		PMIXP_ERROR("Bad value of the EP type: %d", (int)ep->type);
		abort();
	}

	if (SLURM_SUCCESS != rc) {
		PMIXP_ERROR("Cannot send message to %s, size = %u, hostlist:\n%s",
			    addr, (uint32_t) dsize, hostlist);
	}
	return rc;
}

