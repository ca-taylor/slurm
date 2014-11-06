#include "pmix_common.h"
#include "pmix_state.h"
#include "pmix_msg.h"
#include "pmix_db.h"
#include "pmix_debug.h"
#include "pmix_coll.h"

typedef struct {
	uint32_t taskid;
	size_t nbytes;
} cli_msg_hdr_t;

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

// Unblocking message processing


static uint32_t payload_size(void *buf)
{
	cli_msg_hdr_t *ptr = (cli_msg_hdr_t*)buf;
	return ptr->nbytes;
}

static void *_new_msg_to_task(uint32_t taskid, uint32_t size, void **payload)
{
	int msize = size + sizeof(cli_msg_hdr_t);
	cli_msg_hdr_t *msg = (cli_msg_hdr_t*)xmalloc( msize );
	msg->nbytes = size;
	msg->taskid = taskid;
	*payload = (void*)(msg + 1);
	return msg;
}

void pmix_client_request(int fd)
{
	cli_msg_hdr_t hdr;
	uint32_t offset = 0;
	eio_obj_t *obj;

	PMIX_DEBUG("Request from fd = %d", fd);

	// fd was just accept()'ed and is blocking for now.
	// TODO/FIXME: implement this in nonblocking fashion?
	pmix_nbmsg_first_header(fd, &hdr, &offset, sizeof(hdr) );
	xassert( offset == sizeof(hdr));

	// Set nonblocking
	fd_set_nonblocking(fd);

	uint32_t taskid = hdr.taskid;
	if( pmix_state_cli_connected(taskid,fd) ){
		PMIX_DEBUG("Bad connection, taskid = %d, fd = %d", taskid, fd);
		close(fd);
		return;
	}

	// Setup message engine. Push the header we just received to
	// ensure integrity of msgengine
	pmix_msgengine_t *me = pmix_state_cli_msghandler(taskid);
	pmix_nbmsg_init(me, fd, sizeof(hdr),payload_size);
	pmix_nbmsg_add_hdr(me, &hdr);

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
	pmix_msgengine_t *me = pmix_state_cli_msghandler(taskid);

	// Read and process all received messages
	while( 1 ){
		pmix_nbmsg_rcvd(me);
		if( pmix_nbmsg_finalized(me) ){
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
		if( pmix_nbmsg_rcvd_ready(me) ){
			cli_msg_hdr_t hdr;
			void *msg = pmix_nbmsg_rcvd_extract(me, &hdr);
			xassert( hdr.taskid == taskid );
			count++;
			if(count == 1){
				xfree(msg);
				int size = sizeof(cli_msg_hdr_t) + 2*sizeof(int);
				void *ptr = xmalloc(size);
				cli_msg_hdr_t *hdr = (cli_msg_hdr_t*)ptr;
				hdr->nbytes = 2*sizeof(int);
				hdr->taskid = taskid;
				*(int*)(hdr+1) = pmix_info_task_id(taskid);
				*((int*)(hdr+1) + 1) = pmix_info_tasks();
				pmix_nbmsg_send_enqueue(me, ptr);
				if( pmix_nbmsg_finalized(me) ){
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
				pmix_nbmsg_send_enqueue(me, resp);
				if( pmix_nbmsg_finalized(me) ){
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
	pmix_msgengine_t *me = pmix_state_cli_msghandler(taskid);
	//pmix_comm_fd_write_ready(obj->fd);
	if( pmix_nbmsg_send_pending(me) )
		return true;
	return false;
}

static int peer_write(eio_obj_t *obj, List objs)
{
	xassert( !pmix_info_is_srun() );

	PMIX_DEBUG("fd = %d", obj->fd);

	uint32_t taskid = (int)(long)(obj->arg);

	pmix_msgengine_t *me = pmix_state_cli_msghandler(taskid);
	pmix_nbmsg_send_progress(me);

	return 0;
}

void pmix_client_fence_notify()
{
	uint ltask = 0;
	for(ltask = 0; ltask < pmix_info_ltasks(); ltask++){
		int *payload;
		void *msg = _new_msg_to_task(ltask, sizeof(int), (void**)&payload);
		pmix_msgengine_t *me = pmix_state_cli_msghandler(ltask);
		*payload = 1;
		pmix_nbmsg_send_enqueue(me, msg);
		pmix_state_task_coll_finish(ltask);
	}
}
