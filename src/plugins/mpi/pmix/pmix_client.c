/*****************************************************************************\
 **  pmix_client.c - PMIx client communication code
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


#include "pmix_common.h"
#include "pmix_state.h"
#include "pmix_io.h"
#include "pmix_db.h"
#include "pmix_debug.h"
#include "pmix_coll.h"

/* header for pmix client-server msgs - must
 * match that in opal/mca/pmix/native! */
typedef struct {
	//opal_identifier_t id;
	uint32_t taskid;
	uint8_t type;
	uint32_t tag;
	size_t nbytes;
} message_header_t;

static bool peer_readable(eio_obj_t *obj);
static int peer_read(eio_obj_t *obj, List objs);
static bool peer_writable(eio_obj_t *obj);
static int peer_write(eio_obj_t *obj, List objs);
static struct io_operations peer_ops = {
	.readable     = peer_readable,
	.handle_read  = peer_read,
	.writable     = peer_writable,
	.handle_write = peer_write
};

static uint32_t payload_size(void *buf)
{
	message_header_t *ptr = (message_header_t*)buf;
	return ptr->nbytes;
}

pmix_io_engine_header_t cli_header = {
	.net_size= sizeof(message_header_t),
	.host_size = sizeof(message_header_t),
	.pack_hdr_cb = NULL,
	.unpack_hdr_cb = NULL,
	.pay_size_cb = payload_size
};

// Unblocking message processing

static void *_new_msg_to_task(uint32_t taskid, uint32_t size, void **payload)
{
	int msize = size + sizeof(message_header_t);
	message_header_t *msg = (message_header_t*)xmalloc( msize );
	msg->nbytes = size;
	msg->taskid = taskid;
	*payload = (void*)(msg + 1);
	return msg;
}

void pmix_client_new_conn(int fd)
{
	message_header_t hdr;
	uint32_t offset = 0;
	eio_obj_t *obj;

	PMIX_DEBUG("New connection on fd = %d", fd);

	// fd was just accept()'ed and is blocking for now.
	// TODO/FIXME: implement this in nonblocking fashion?
	pmix_io_first_header(fd, &hdr, &offset, sizeof(hdr) );
	xassert( offset == sizeof(hdr));

	// Setup fd
	fd_set_nonblocking(fd);
	fd_set_close_on_exec(fd);

	uint32_t taskid = hdr.taskid;
	if( pmix_state_cli_connected(taskid,fd) ){
		PMIX_DEBUG("Bad connection, taskid = %d, fd = %d", taskid, fd);
		close(fd);
		return;
	}

	// Setup message engine. Push the header we just received to
	// ensure integrity of msgengine
	pmix_io_engine_t *me = pmix_state_cli_msghandler(taskid);
	pmix_io_init(me, fd, cli_header);
	pmix_io_add_hdr(me, &hdr);

	obj = eio_obj_create(fd, &peer_ops, (void*)(long)taskid);
	eio_new_obj( pmix_info_io(), obj);
}

static bool peer_readable(eio_obj_t *obj)
{
	PMIX_DEBUG("fd = %d", obj->fd);
	xassert( !pmix_info_is_srun() );
	if (obj->shutdown == true) {
		if (obj->fd != -1) {
			close(obj->fd);
			obj->fd = -1;
		}
		PMIX_DEBUG("    false, shutdown");
		return false;
	}
	return true;
}

static int peer_read(eio_obj_t *obj, List objs)
{
	static int count = 0;
	PMIX_DEBUG("fd = %d", obj->fd);
	uint32_t taskid = (int)(long)(obj->arg);
	pmix_io_engine_t *me = pmix_state_cli_msghandler(taskid);

	// Read and process all received messages
	while( 1 ){
		pmix_io_rcvd(me);
		if( pmix_io_finalized(me) ){
			PMIX_DEBUG("Connection with task %d finalized", taskid);
			obj->shutdown = true;
			eio_remove_obj(obj, objs);
			pmix_state_cli_finalized_set(taskid);
			// TODO: we need to remove this connection from eio
			// Can we just:
			// 1. find the node in objs that corresponds to obj
			// 2. list_node_destroy (objs, node)
			return 0;
		}
		if( pmix_io_rcvd_ready(me) ){
			message_header_t hdr;
			void *msg = pmix_io_rcvd_extract(me, &hdr);
			xassert( hdr.taskid == taskid );
			count++;
			if(count == 1){
				xfree(msg);
				int size = sizeof(message_header_t) + 2*sizeof(int);
				void *ptr = xmalloc(size);
				message_header_t *hdr = (message_header_t*)ptr;
				hdr->nbytes = 2*sizeof(int);
				hdr->taskid = taskid;
				*(int*)(hdr+1) = pmix_info_task_id(taskid);
				*((int*)(hdr+1) + 1) = pmix_info_tasks();
				pmix_io_send_enqueue(me, ptr);
				if( pmix_io_finalized(me) ){
					PMIX_DEBUG("Connection with task %d finalized", taskid);
					obj->shutdown = true;
					eio_remove_obj(obj, objs);
					pmix_state_cli_finalized_set(taskid);
					// TODO: we need to remove this connection from eio
					// Can we just:
					// 1. find the node in objs that corresponds to obj
					// 2. list_node_destroy (objs, node)
					return 0;
				}
				xfree(msg);
			} else if( count == 2 ){
				pmix_coll_task_contrib(taskid, msg, hdr.nbytes);
			} else {
				// Fin out what info he wants
				int gtaskid = *(int*)msg;
				void *blob, *payload;
				int size = pmix_db_get_blob(gtaskid, &blob);
				void *resp = _new_msg_to_task(taskid, size, &payload);
				memcpy(payload, blob, size);
				pmix_io_send_enqueue(me, resp);
				if( pmix_io_finalized(me) ){
					PMIX_DEBUG("Connection with task %d finalized", taskid);
					obj->shutdown = true;
					eio_remove_obj(obj, objs);
					pmix_state_cli_finalized_set(taskid);
					// TODO: we need to remove this connection from eio
					// Can we just:
					// 1. find the node in objs that corresponds to obj
					// 2. list_node_destroy (objs, node)
					return 0;
				}
				xfree(msg);
			}
		}else{
			// No more complete messages
			break;
		}
	}
	return 0;
}


static bool peer_writable(eio_obj_t *obj)
{
	xassert( !pmix_info_is_srun() );
	PMIX_DEBUG("fd = %d", obj->fd);
	if (obj->shutdown == true) {
		if (obj->fd != -1) {
			close(obj->fd);
			obj->fd = -1;
		}
		PMIX_DEBUG("    false, shutdown");
		return false;
	}
	uint32_t taskid = (int)(long)(obj->arg);
	pmix_io_engine_t *me = pmix_state_cli_msghandler(taskid);
	//pmix_comm_fd_write_ready(obj->fd);
	if( pmix_io_send_pending(me) )
		return true;
	return false;
}

static int peer_write(eio_obj_t *obj, List objs)
{
	xassert( !pmix_info_is_srun() );

	PMIX_DEBUG("fd = %d", obj->fd);

	uint32_t taskid = (int)(long)(obj->arg);

	pmix_io_engine_t *me = pmix_state_cli_msghandler(taskid);
	pmix_io_send_progress(me);

	return 0;
}

void pmix_client_fence_notify()
{
	uint ltask = 0;
	for(ltask = 0; ltask < pmix_info_ltasks(); ltask++){
		int *payload;
		void *msg = _new_msg_to_task(ltask, sizeof(int), (void**)&payload);
		pmix_io_engine_t *me = pmix_state_cli_msghandler(ltask);
		*payload = 1;
		pmix_io_send_enqueue(me, msg);
		pmix_state_task_coll_finish(ltask);
	}
}
