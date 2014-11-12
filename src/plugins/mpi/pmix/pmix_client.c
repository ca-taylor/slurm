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

// ----------------------------------- 8< -----------------------------------------------------------//

/*
 * PMIx library definitions
 */

#define PMIX_VERSION "1.0"

/* header for pmix client-server msgs - must
 * match that in opal/mca/pmix/native! */
#define PMIX_CLIENT_HDR_MAGIC 0xdeadbeef
typedef struct {
	uint32_t magic;
	uint32_t taskid;
	uint8_t type;
	uint32_t tag;
	size_t nbytes;
} message_header_t;

// From OMPI pmix_server implementation
/* define some commands */
#define PMIX_ABORT_CMD        1
#define PMIX_FENCE_CMD        2
#define PMIX_FENCENB_CMD      3
#define PMIX_PUT_CMD          4
#define PMIX_GET_CMD          5
#define PMIX_GETNB_CMD        6
#define PMIX_FINALIZE_CMD     7
#define PMIX_GETATTR_CMD      8

/* define some message types */
#define PMIX_USOCK_IDENT  1
#define PMIX_USOCK_USER   2

typedef struct {
	char hname[32];
	char tmpdir[256];
	char jobid[32];
	uint32_t appnum;
	uint32_t rank; //FIXME: what is it for?
	uint32_t grank;
	uint32_t apprank;
	uint32_t offset;
	uint16_t lrank;
	uint16_t nrank;
	uint64_t lldr;
	uint32_t aldr;
	uint32_t usize;
	uint32_t jsize;
	uint32_t lsize;
	uint32_t nsize;
	uint32_t msize;
} job_attr_t;



// ----------------------------------- 8< -----------------------------------------------------------//

int _process_message(message_header_t hdr, void *msg);
int _establish_connection(message_header_t hdr, void *msg);
inline void _fill_job_attributes(uint32_t taskid, job_attr_t *jattr);
int _process_cli_request(message_header_t hdr, void *msg);
int _postpone_cli_request(message_header_t hdr, void *msg);

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
	xassert(ptr->magic == PMIX_CLIENT_HDR_MAGIC);
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

static void *_allocate_msg_to_task(uint32_t taskid, uint8_t type, uint32_t tag, uint32_t size, void **payload)
{
	int msize = size + sizeof(message_header_t);
	message_header_t *msg = (message_header_t*)xmalloc( msize );
	msg->magic = PMIX_CLIENT_HDR_MAGIC;
	msg->nbytes = size;
	msg->taskid = taskid;
	msg->tag = tag;
	msg->type = type;
	*payload = (void*)(msg + 1);
	return msg;
}

#define _new_msg_connecting(id, size, pay) _allocate_msg_to_task(id, PMIX_USOCK_IDENT, 0, size, pay)
#define _new_msg(id, size, pay) _allocate_msg_to_task(id, PMIX_USOCK_USER, 0, size, pay)
#define _new_msg_tag(id, tag, size, pay) _allocate_msg_to_task(id, PMIX_USOCK_USER, tag, size, pay)

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
	if( pmix_state_cli_connecting(taskid,fd) ){
		PMIX_DEBUG("Bad connection, taskid = %d, fd = %d", taskid, fd);
		close(fd);
		return;
	}

	// Setup message engine. Push the header we just received to
	// ensure integrity of msgengine
	pmix_io_engine_t *me = pmix_state_cli_io(taskid);
	pmix_io_init(me, fd, cli_header);
	pmix_io_add_hdr(me, &hdr);

	obj = eio_obj_create(fd, &peer_ops, (void*)(long)taskid);
	eio_new_obj( pmix_info_io(), obj);
}

int _finalize_client(uint32_t taskid, eio_obj_t *obj, List objs)
{
	// Don't track this process anymore
	eio_remove_obj(obj, objs);
	// Reset client state
	pmix_state_cli_finalize(taskid);
	return 0;
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
	PMIX_DEBUG("fd = %d", obj->fd);
	uint32_t taskid = (int)(long)(obj->arg);
	pmix_io_engine_t *eng = pmix_state_cli_io(taskid);

	// Read and process all received messages
	while( 1 ){
		pmix_io_rcvd(eng);
		if( pmix_io_finalized(eng) ){
			PMIX_DEBUG("Connection with task %d finalized", taskid);
			break;
		}
		if( pmix_io_rcvd_ready(eng) ){
			message_header_t hdr;
			void *msg = pmix_io_rcvd_extract(eng, &hdr);
			xassert( hdr.taskid == taskid );
//			{
//				static int delay = 1;
//				pmix_debug_hang(delay);
//			}
			if( _process_message(hdr, msg) ){
				break;
			}
		}else{
			// No more complete messages
			break;
		}
	}

	// Check if we still have the connection
	if( pmix_io_finalized(eng) ){
		_finalize_client(taskid, obj, objs);
	}
	return 0;
}


static bool peer_writable(eio_obj_t *obj)
{
	xassert( !pmix_info_is_srun() );
	PMIX_DEBUG("fd = %d", obj->fd);
	if (obj->shutdown == true) {
		PMIX_ERROR_NO(0,"We shouldn't be here if connection shutdowned");
		return false;
	}
	uint32_t taskid = (int)(long)(obj->arg);
	pmix_io_engine_t *me = pmix_state_cli_io(taskid);
	if( pmix_io_send_pending(me) )
		return true;
	return false;
}

static int peer_write(eio_obj_t *obj, List objs)
{
	xassert( !pmix_info_is_srun() );

	PMIX_DEBUG("fd = %d", obj->fd);

	uint32_t taskid = (int)(long)(obj->arg);
	pmix_io_engine_t *me = pmix_state_cli_io(taskid);
	pmix_io_send_progress(me);
	return 0;
}

void pmix_client_fence_notify()
{
	uint ltask = 0;
	// FIXME: here we will need to iterate through postponed requests
	// and answer them
	// This will work for both blocking and non-blocking fence
	// By now we only send ones to all clients
	for(ltask = 0; ltask < pmix_info_ltasks(); ltask++){
		int *payload;
		void *msg = _new_msg(ltask, sizeof(int), (void**)&payload);
		pmix_io_engine_t *me = pmix_state_cli_io(ltask);
		*payload = 0;
		pmix_io_send_enqueue(me, msg);
		pmix_state_task_coll_finish(ltask);
	}
}

int _process_message(message_header_t hdr, void *msg)
{
	int rc;

	switch(  pmix_state_cli(hdr.taskid) ){
	case PMIX_CLI_UNCONNECTED:
		PMIX_ERROR_NO(0,"We shouldn't be here. Obvious programmer mistake");
		xassert(0);
		break;
	case PMIX_CLI_ACK:
		// process connection establishment
		rc = _establish_connection(hdr, msg);
		xfree(msg);
		break;
	case PMIX_CLI_OPERATE:
		return _process_cli_request(hdr, msg);
	case PMIX_CLI_COLL:
		// We are in the middle of collective. DB is inconsistent.
		return _postpone_cli_request(hdr,msg);
	}
	return rc;
}

int _establish_connection(message_header_t hdr, void *msg)
{
	uint32_t taskid = hdr.taskid;
	pmix_io_engine_t *eng = pmix_state_cli_io(taskid);
	char *version;
	void *rmsg, *payload;
	int size;

	if (hdr.type != PMIX_USOCK_IDENT) {
		PMIX_ERROR_NO(0,"Invalid message header type: %d from ltask=%d, fd = %d",
					  hdr.type, taskid, pmix_state_cli_fd(taskid));
		return SLURM_ERROR;
	}

	PMIX_DEBUG("Connection from ltask %d established", taskid);

	/* check that this is from a matching version */
	version = (char*)(msg);
	if (0 != strcmp(version, PMIX_VERSION ) ) {
		PMIX_ERROR_NO(0,"PMIx version mismatch");
		return SLURM_ERROR;
	}


	/* check security token */
	// TODO: Check with Ralph, skip by now

	// Send ack to a client
	size = sizeof(PMIX_VERSION) + 1;
	rmsg = _new_msg_connecting(taskid, size, &payload);
	strcpy((char*)payload, PMIX_VERSION);
	pmix_io_send_enqueue(eng, rmsg);

	// Switch to PMIX_CLI_OPERATE state
	pmix_state_cli_connected(taskid);
	return SLURM_SUCCESS;
}

inline void _fill_job_attributes(uint32_t taskid, job_attr_t *jattr)
{
	char *p = NULL;

	// Precise numbers first

	/* name of the host this proc is on */
	strcpy(jattr->hname, pmix_info_this_host());
	/* top-level tmp dir assigned to session (Might be ovverrided by prolog!?) */
	p = getenv("TMPDIR");
	if( p ){
		strcpy(jattr->tmpdir, p);
	} else {
		jattr->tmpdir[0] = '\0';
	}
	/* jobid assigned by scheduler */
	sprintf(jattr->jobid, "%d.%d", pmix_info_jobid(), pmix_info_stepid());
	/* process rank within the job */
	jattr->grank = pmix_info_task_id(taskid);
	/* rank on this node within this job */
	jattr->lrank = taskid;
	/* procs in this job */
	jattr->jsize = pmix_info_tasks();
	/* procs in this job on this node */
	jattr->lsize = pmix_info_ltasks();

	/* #procs in this namespace */
	jattr->usize = pmix_info_tasks_uni();
	/* max #procs for this job */
	jattr->msize = jattr->jsize;

	// Now set "emulated" values for simplest implementation
	// where we don't spawn

	// -----------------------------------------------------------------------------------

	// If my understanding is correct this value is important for
	// MPI_Spawn'ed processes which have global rank offset <> 0.
	// By now it will be 0
	/* starting global rank of this job */
	jattr->offset = 0;

	// -----------------------------------------------------------------------------------

	// If we spawn several job steps might be located on one node. Thusthis value might be wrong.
	// Need to implement stepd's intra-node communication to exchange this info

	/* Lowest rank on this node */
	jattr->lldr = pmix_info_task_id(0);
	/* rank on this node spanning all jobs */
	jattr->nrank = jattr->lrank; // True if we don't spawn
	/* #procs across all jobs on this node */
	jattr->nsize = jattr->lsize; // The same here


	// -----------------------------------------------------------------------------------

	// srun is able to launch multi-app configurations.
	// See MULTIPLE PROGRAM CONFIGURATION section of srun's man
	// Address this in the future !?

	/* app number within the job */
	jattr->appnum = 0;
	/* rank within this app */
	jattr->apprank = jattr->grank;
	/* lowest rank in this app within this job */
	jattr->aldr = 0; // Do not distinguich by now

	// -----------------------------------------------------------------------------------

	// TODO: Actually we can calculate usize from SLURM's environment:
	// SLURM_JOB_CPUS_PER_NODE='4(x6)'
	// SLURM_JOB_NODELIST='cndev[1-4,8-9]'
	//
	// ? What's the difference between maxprocs and universe size?
	// I think for SLURM they'll be equal.




	// -----------------------------------------------------------------------------------
}

int _process_cli_request(message_header_t hdr, void *msg)
{
	uint32_t taskid = hdr.taskid;
	uint32_t tag = hdr.tag;
	pmix_io_engine_t *eng = pmix_state_cli_io(taskid);
	int *ptr = (int*)msg;
	void *rmsg, *payload;
	int rc = 0;

	switch( ptr[0] ){
	case PMIX_GETATTR_CMD:{
		job_attr_t jattr;
		_fill_job_attributes(taskid, &jattr);
		rmsg = _new_msg_tag(taskid, tag, sizeof(job_attr_t), (void**)&payload);
		memcpy(payload, &jattr, sizeof(jattr));
		pmix_io_send_enqueue(eng, rmsg);
		goto free_message;
	}
	case PMIX_GET_CMD:{
		// Currently we just put the GID of the requested process
		// in the first 4 bytes of the message
		int gtaskid = *((int*)msg + 1);
		if( gtaskid >= pmix_info_tasks() ){
			// return error!
			rmsg = _new_msg_tag(taskid, tag, 1, &payload);
			*(char*)payload = 0;
			pmix_io_send_enqueue(eng, rmsg);
			goto free_message;
		}

		void *blob, *payload;
		int size = pmix_db_get_blob(gtaskid, &blob);
		if( blob == NULL ){
			// we don't have proper data now. Postpone until we'll get them
			// return without xfree()ing msg!
			return _postpone_cli_request(hdr, msg);
		}
		rmsg = _new_msg_tag(taskid, tag, size, &payload);
		memcpy(payload, blob, size);
		pmix_io_send_enqueue(eng, rmsg);
		goto free_message;
	}
	case PMIX_FENCE_CMD:
	case PMIX_FENCENB_CMD:{
		// remove cmd contribution
		int size = hdr.nbytes - sizeof(uint32_t);
		pmix_coll_task_contrib(taskid, (void*)&ptr[1], size);
		break;
	}
	case PMIX_FINALIZE_CMD:{
	}
	case PMIX_ABORT_CMD:{
	}

	default:
		break;
	}

free_message:
	xfree(msg);
	return rc;
}

int _postpone_cli_request(message_header_t hdr, void *msg)
{
	// Implement request postpone functionality
	return SLURM_SUCCESS;
}
