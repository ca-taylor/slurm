/*****************************************************************************\
 **  pmix_server.c - PMIx server side functionality
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
#include "pmixp_info.h"
#include "pmixp_coll.h"
#include "pmixp_debug.h"
#include "pmixp_io.h"
#include "pmixp_client.h"
#include "pmixp_server.h"
#include "pmixp_db.h"
#include "pmixp_state.h"
#include "pmixp_client.h"

#include <pmix_server.h>


#define PMIX_SERVER_MSG_MAGIC 0xCAFECA11
typedef struct {
	uint32_t magic;
	uint32_t type;
	uint32_t gen;
	uint32_t nodeid;
	uint32_t msgsize;
} send_header_t;
// Cannot use sizeof here because of padding
#define SEND_HDR_SIZE (5*sizeof(uint32_t))

typedef struct {
	uint32_t size; // Has to be first (appended by SLURM API)
	send_header_t send_hdr;
} recv_header_t;
#define RCVD_HDR_SIZE (sizeof(uint32_t) + SEND_HDR_SIZE)

static uint32_t _recv_payload_size(void *buf);
static int _send_pack_hdr(void *host, void *net);
static int _recv_unpack_hdr(void *net, void *host);

static bool _serv_readable(eio_obj_t *obj);
static int _serv_read(eio_obj_t *obj, List objs);
static void _process_server_request(recv_header_t *_hdr, void *payload);

void _dmdx_reply_to_node(uint32_t localid, uint32_t nodeid);
void _process_dmdx_request(send_header_t *hdr, void *payload);
int _dmdx_response(send_header_t *hdr, void *payload);

static struct io_operations peer_ops = {
	.readable     = _serv_readable,
	.handle_read  = _serv_read
};

pmixp_io_engine_header_t srv_rcvd_header = {
	.host_size = sizeof(recv_header_t),
	.net_size = RCVD_HDR_SIZE,
	.pack_hdr_cb = NULL,
	.unpack_hdr_cb = _recv_unpack_hdr,
	.pay_size_cb = _recv_payload_size
};

int pmixp_stepd_init(const stepd_step_rec_t *job, char ***env)
{
	struct sockaddr_un address;
	pmix_range_t range;
	pmixp_coll_t *coll = NULL;
	char path[MAX_USOCK_PATH];
	int fd, rc;

	// Create UNIX socket for slurmd communication
	sprintf(path, PMIXP_STEPD_ADDR_FMT, job->jobid, job->stepid );
	if( (fd = pmixp_usock_create_srv(path)) < 0 ){
		return SLURM_ERROR;
	}
	fd_set_close_on_exec(fd);
	pmixp_info_srv_contacts(path, fd);

	if( SLURM_SUCCESS != ( rc = pmixp_info_set(job, env) ) ){
		PMIXP_ERROR("pmixp_info_set(job, env) failed");
		return rc;
	}

	// Initialize clients state structure
	if( SLURM_SUCCESS != (rc = pmixp_state_init()) ){
		PMIXP_ERROR("pmixp_state_init() failed");
		return rc;
	}

	if( SLURM_SUCCESS != (rc = pmixp_nspaces_init()) ){
		PMIXP_ERROR("pmixp_nspaces_init() failed");
		return rc;
	}

	if( ( rc = pmixp_coll_init(env, SEND_HDR_SIZE) ) ){
		PMIXP_ERROR("pmixp_coll_init() failed");
		return rc;
	}

	// Add default collectives
	range.nranks = 0;
	range.ranks = NULL;
	strcpy(range.nspace, pmixp_info_namespace());
	coll = pmixp_state_coll_new(PMIXP_COLL_TYPE_FENCE, &range, 1);
	if( NULL == coll ){
		PMIXP_ERROR("Cannot add default FENCE collective");
		return SLURM_ERROR;
	}

	// Create UNIX socket for client communication
	if( SLURM_SUCCESS != pmixp_libpmix_init(&address) ){
		PMIXP_ERROR("pmixp_libpmix_init() failed");
		return SLURM_ERROR;
	}
	if( (fd = pmixp_usock_create_srv(address.sun_path)) < 0 ){
		close( pmixp_info_srv_fd() );
		PMIXP_ERROR("pmixp_usock_create_srv() failed");
		return SLURM_ERROR;
	}
	fd_set_close_on_exec(fd);
	pmixp_info_cli_contacts(fd);

	// Create UNIX socket for client communication
	if( SLURM_SUCCESS != pmixp_libpmix_job_set() ){
		PMIXP_ERROR("pmixp_libpmix_job_set() failed");
		return SLURM_ERROR;
	}

	return SLURM_SUCCESS;
}

void pmix_server_new_conn(int fd)
{
	eio_obj_t *obj;
	PMIXP_DEBUG("Request from fd = %d", fd);

	// Set nonblocking
	fd_set_nonblocking(fd);
	fd_set_close_on_exec(fd);

	pmixp_io_engine_t *me = xmalloc( sizeof(pmixp_io_engine_t) );
	pmix_io_init(me, fd, srv_rcvd_header);
	// We use slurm_forward_data to send message to stepd's
	// SLURM will put user ID there. We need to skip it.
	pmix_io_rcvd_padding(me, sizeof(uint32_t));

	// TODO: in future try to process the request right here
	// use eio only in case of blocking operation
	// NOW: always defer to debug the blocking case
	obj = eio_obj_create(fd, &peer_ops, (void*)me);
	eio_new_obj(pmixp_info_io(), obj);
}

/*
 *  Server message processing
 */


static uint32_t _recv_payload_size(void *buf)
{
	recv_header_t *ptr = (recv_header_t*)buf;
	send_header_t *hdr = &ptr->send_hdr;
	xassert( ptr->size == (SEND_HDR_SIZE + hdr->msgsize) );
	xassert( hdr->magic == PMIX_SERVER_MSG_MAGIC );
	return hdr->msgsize;
}

/*
 * Pack message header.
 * Returns packed size
 * Note: asymmetric to _recv_unpack_hdr because of additional SLURM header
 */
static int _send_pack_hdr(void *host, void *net)
{
	send_header_t *ptr = (send_header_t*)host;
	Buf packbuf = create_buf(net, sizeof(send_header_t));
	int size = 0;
	pack32(ptr->magic, packbuf);
	pack32(ptr->type, packbuf);
	pack32(ptr->gen, packbuf);
	pack32(ptr->nodeid, packbuf);
	pack32(ptr->msgsize, packbuf);
	size = get_buf_offset(packbuf);
	xassert( size == SEND_HDR_SIZE );
	// free the Buf packbuf, but not the memory to which it points
	packbuf->head = NULL;
	free_buf(packbuf);
	return size;
}

/*
 * Unpack message header.
 * Returns 0 on success and -errno on failure
 * Note: asymmetric to _send_pack_hdr because of additional SLURM header
 */
static int _recv_unpack_hdr(void *net, void *host)
{
	recv_header_t *ptr = (recv_header_t*)host;
	Buf packbuf = create_buf(net, sizeof(recv_header_t));
	if( unpack32(&ptr->size, packbuf) ){
		return -EINVAL;
	}
	if( unpack32(&ptr->send_hdr.magic, packbuf)){
		return -EINVAL;
	}
	xassert( ptr->send_hdr.magic == PMIX_SERVER_MSG_MAGIC );

	if( unpack32(&ptr->send_hdr.type, packbuf)){
		return -EINVAL;
	}

	if( unpack32(&ptr->send_hdr.gen, packbuf)){
		return -EINVAL;
	}

	if( unpack32(&ptr->send_hdr.nodeid, packbuf)){
		return -EINVAL;
	}

	if( unpack32(&ptr->send_hdr.msgsize, packbuf) ){
		return -EINVAL;
	}

	// free the Buf packbuf, but not the memory to which it points
	packbuf->head = NULL;
	free_buf(packbuf);
	return 0;
}

int pmixp_server_send_coll(char *hostlist, pmixp_srv_cmd_t type,
			   const char *addr, uint32_t nodeid,
			   void *data, size_t size)
{
	send_header_t hdr;
	char nhdr[sizeof(send_header_t)];
	size_t hsize;
	int rc;

	hdr.magic = PMIX_SERVER_MSG_MAGIC;
	hdr.type = type;
	hdr.nodeid = nodeid;
	hdr.msgsize = size;
	hdr.gen = 0; // Not used now
	hsize = _send_pack_hdr(&hdr, nhdr);
	memcpy(data,nhdr, hsize);

	rc = pmixp_stepd_send(hostlist, addr, data, size);
	if( SLURM_SUCCESS != rc ){
		PMIXP_ERROR("Cannot send message to %s, size = %u, hostlist:\n%s",
			    addr, (uint32_t)size, hostlist);
	}
	return rc;
}

static bool _serv_readable(eio_obj_t *obj)
{
	// We should delete connection right when it
	// was closed or failed
	xassert( obj->shutdown == false );
	return true;
}

static void _process_server_request(recv_header_t *_hdr, void *payload)
{
	send_header_t *hdr = &_hdr->send_hdr;
	size_t size = hdr->msgsize - SEND_HDR_SIZE;
	switch( hdr->type ){
	case PMIXP_MSG_FAN_IN:
	case PMIXP_MSG_FAN_OUT:{
		pmixp_coll_t *coll;
		int offset = 0;
		pmix_range_t *ranges = NULL;
		size_t nranges = 0;
		pmixp_coll_type_t type = 0;

		offset = pmixp_coll_unpack_ranges(payload, size, &type, &ranges, &nranges);
		if( 0 >= offset ){
			PMIXP_ERROR("Bad message header from node %d",
				    hdr->nodeid);
			return;
		}
		coll = pmixp_state_coll_find(type, ranges, nranges);
		PMIXP_DEBUG("FENCE collective message from node \"%s\", type = %s",
			    pmixp_coll_nodename(coll, hdr->nodeid),
			    (PMIXP_MSG_FAN_IN == hdr->type) ? "fan-in" : "fan-out");

		if( PMIXP_MSG_FAN_IN == hdr->type ){
			pmixp_coll_contrib_node(coll, hdr->nodeid,
						payload + offset,
						hdr->msgsize - offset);
		} else {
			pmixp_coll_fan_out_data(coll, payload + offset,
						hdr->msgsize - offset);
		}
		break;
	}
	default:
		PMIXP_ERROR("Unknown message type %d", hdr->type);
		break;
	}
}

static int _serv_read(eio_obj_t *obj, List objs)
{

	PMIXP_DEBUG("fd = %d", obj->fd);
	pmixp_io_engine_t *me = (pmixp_io_engine_t *)obj->arg;

	//	pmix_debug_hang(1);

	// Read and process all received messages
	while( 1 ){
		pmix_io_rcvd(me);
		if( pmix_io_finalized(me) ){
			obj->shutdown = true;
			//pmix_debug_hang(1);
			PMIXP_DEBUG("Connection finalized fd = %d", obj->fd);
			eio_remove_obj(obj, objs);
			return 0;
		}
		if( pmix_io_rcvd_ready(me) ){
			recv_header_t hdr;
			void *msg = pmix_io_rcvd_extract(me, &hdr);
			_process_server_request(&hdr, msg);
			xfree(msg);
		}else{
			// No more complete messages
			break;
		}
	}
	return 0;
}

