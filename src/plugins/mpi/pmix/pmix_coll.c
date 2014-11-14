/*****************************************************************************\
 **  pmix_coll.c - PMIx collective primitives
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
#include "src/slurmd/common/reverse_tree_math.h"
#include "pmix_info.h"
#include "pmix_debug.h"
#include "pmix_state.h"
#include "pmix_server.h"
#include "pmix_db.h"

void **node_data = NULL;
int *node_sizes = NULL;
void **local_data = NULL;
int *local_sizes = NULL;

static void list_free_data(void *x)
{
	xfree(x);
}

//// TODO: make this function more effective
//// use math instead of brut force :) for exascale!
//static int _node_childrens(int **child_array, int width, int id, int max_node_id)
//{
//	int child, *ptr;
//	List child_ids = list_create(list_free_data);

//	for(child = id+1; child < max_node_id; child++){
//		int parent_id, child_num, depth, max_depth;
//		reverse_tree_info(child, max_node_id, width, &parent_id, &child_num,
//						  &depth, &max_depth);
//		if( parent_id == id ){
//			ptr = xmalloc(sizeof(int));
//			*ptr = child;
//			list_enqueue(child_ids, ptr);
//		}
//	}

//	int n = list_count(child_ids);
//	int count = 0;
//	*child_array = xmalloc(n*sizeof(int));
//	while( (ptr = list_dequeue(child_ids) ) ){
//		(*child_array)[count++] = *ptr - 1;
//		xfree(ptr);
//	}
//	return count;
//}

char *_pack_the_data()
{
	// Join all the pieces in the one message
	uint32_t cum_size = 0;
	uint32_t i;
	for(i = 0; i < pmix_info_childs(); i++ ){
		cum_size += node_sizes[i];
	}
	for(i = 0; i < pmix_info_ltasks(); i++ ){
		cum_size += local_sizes[i];
	}

	char *payload;
	char *msg = pmix_server_alloc_msg( cum_size , (void**)&payload);

	for(i = 0; i < pmix_info_childs(); i++ ){
		memcpy(payload, node_data[i], node_sizes[i]);
		payload += node_sizes[i];
		xfree(node_data[i]);
		node_data[i] = NULL;
		xassert( pmix_state_node_contrib_cancel(i) );
	}

	for(i = 0; i < pmix_info_ltasks(); i++ ){
		memcpy(payload, local_data[i], local_sizes[i]);
		payload += local_sizes[i];
		xfree(local_data[i]);
		local_data[i] = NULL;
		xassert( pmix_state_task_contrib_cancel(i) );
	}
	return msg;
}

static void _forward()
{
	xassert( pmix_state_coll_local_ok() );

	// pmix_debug_hang(1);

	pmix_state_coll_forwad();

	char *msg = _pack_the_data();
	void *msg_begin = pmix_server_msg_start(msg);
	uint32_t size = pmix_server_msg_size(msg);

	switch( pmix_info_parent_type() ){
		case PMIX_PARENT_ROOT:
			// We have complete dataset. Broadcast it to others
			pmix_server_msg_setcmd(msg, PMIX_FENCE_RESP);
			pmix_server_msg_finalize(msg);
			slurm_forward_data(pmix_info_step_hosts(), (char*)pmix_info_srv_addr(), size, msg_begin);
			break;
		case PMIX_PARENT_SRUN:{
				int fd, rc;
				pmix_server_msg_setcmd(msg, PMIX_FENCE);
				pmix_server_msg_finalize(msg);
				fd = slurm_open_stream(pmix_info_parent_addr(), true);
				if (fd < 0){
					PMIX_ERROR("Cannot send collective data to srun (slurm_open_stream)");
				}

				rc = slurm_msg_sendto(fd, msg_begin, size, SLURM_PROTOCOL_NO_SEND_RECV_FLAGS);
				if (rc != pmix_server_msg_size(msg)){ /* all data sent */
					PMIX_ERROR("Cannot send collective data to srun (slurm_msg_sendto)");
				}
				close(fd);
				break;
			}
		case PMIX_PARENT_STEPD:
			pmix_server_msg_setcmd(msg, PMIX_FENCE);
			pmix_server_msg_finalize(msg);
			slurm_forward_data(pmix_info_parent_host(), (char*)pmix_info_srv_addr(), size, msg_begin);
			break;
		default:
			PMIX_ERROR("Inconsistent parent type value");
			xassert(0);
	}
}
/* TODO: Completely remove
void pmix_coll_node_contrib(uint32_t nodeid, void *msg, uint32_t size)
{
  PMIX_DEBUG("Receive collective message from node %d", nodeid);
  int idx = pmix_info_is_child_no(nodeid);
  if( idx < 0 ){
	PMIX_ERROR("%d: The node %d shouldn't send it's data directly to me",
			   pmix_info_nodeid(), nodeid);
	xfree(msg);
	return;
  }
  if( pmix_state_node_contrib_ok(idx) ){
	PMIX_ERROR("%d: The node %d already contributed to this collective",
			   pmix_info_nodeid(), nodeid);
	xfree(msg);
	return;
  }
  node_data[idx] = msg;
  node_sizes[idx] = size;
  if( pmix_state_coll_local_ok() ){
	_forward();
  }
}

void pmix_coll_task_contrib(uint32_t taskid, void *msg, uint32_t size)
{
  PMIX_DEBUG("%d: Local task contribution %d", pmix_info_nodeid(), taskid);
   if( pmix_state_task_contrib_ok(taskid) ){
	PMIX_ERROR("%d: The task %d already contributed to this collective",
			   pmix_info_nodeid(), taskid);
	return;
  }
  local_data[taskid]  = msg;
  local_sizes[taskid] = size;
  if( pmix_state_coll_local_ok() ){
	_forward();
  }
}
*/

/*
 * Based on ideas provided by Hongjia Cao <hjcao@nudt.edu.cn> in PMI2 plugin
 */
int pmix_coll_init(char ***env)
{
	uint32_t nodeid = pmix_info_nodeid();
	uint32_t nodes = pmix_info_nodes();
	int parent_id, child_cnt, depth, max_depth;
	int *children, width, i;
	char *p;

	PMIX_DEBUG("Start");
	// FIXME: By now just use SLURM defaults. Make it flexible as PMI2 in future.
	width = slurm_get_tree_width();
	reverse_tree_info(nodeid + 1, nodes + 1, width, &parent_id, &child_cnt,
					  &depth, &max_depth);
	parent_id--;
	xassert( parent_id >= -2 ); // parent_id can't be less that -2!

	// We interested in amount of direct childs
	children = xmalloc( sizeof(int) * width);
	child_cnt = reverse_tree_direct_children(nodeid + 1, nodes + 1, width, depth, children);
	for(i=0;i<child_cnt;i++){
		children[i]--;
	}

	{
		int i;
		PMIX_DEBUG("Have %d childrens", child_cnt);
		char buf[1024];
		for(i=0;i<child_cnt; i++){
			sprintf(buf,"%s %d", buf, children[i]);
		}
		PMIX_DEBUG("%s", buf);
	}

	if( pmix_info_coll_tree_set(children, child_cnt) ){
		return SLURM_ERROR;
	}

	if( parent_id == -2 ){
		// this is srun.
		pmix_info_parent_set_root();
	}else if( parent_id == -1 ){
		// srun is our parent
		p = getenvp(*env, PMIX_SRUN_HOST_ENV);
		if (!p) {
			PMIX_ERROR("Environment variable %s not found", PMIX_SRUN_HOST_ENV);
			return SLURM_ERROR;
		}
		char *phost = p;
		p = getenvp(*env, PMIX_SRUN_PORT_ENV);
		if (!p) {
			PMIX_ERROR("Environment variable %s not found", PMIX_SRUN_PORT_ENV);
			return SLURM_ERROR;
		}
		uint16_t port = atoi(p);
		unsetenvp(*env, PMIX_SRUN_PORT_ENV);
		pmix_info_parent_set_srun(phost, port);
	} else if( parent_id >= 0 ){
		char *phost = pmix_info_nth_host_name(parent_id);
		pmix_info_parent_set_stepd(phost);
	}

	// Collectove data
	uint32_t size = sizeof(void*) * pmix_info_childs();
	node_data = xmalloc(size);
	memset(node_data, 0, size);

	size = sizeof(int) *  pmix_info_childs();
	node_sizes = xmalloc( size );
	memset(node_sizes, 0, size);

	size = sizeof(void*) * pmix_info_ltasks();
	local_data = xmalloc(size);
	memset(local_data, 0, size);

	size = sizeof(int) *  pmix_info_ltasks();
	local_sizes = xmalloc(size);
	memset(local_sizes, 0, size);

	return SLURM_SUCCESS;
}

void pmix_coll_node_contrib(uint32_t nodeid, void *msg, uint32_t size)
{
	int idx = pmix_info_is_child_no(nodeid);
	PMIX_DEBUG("Receive collective message from node %d", nodeid);
	if( idx < 0 ){

		PMIX_ERROR("The node %s [%d] shouldn't send it's data directly to me",
				   pmix_info_nth_host_name(nodeid), nodeid);
		xfree(msg);
		return;
	}
	if( !pmix_state_node_contrib_ok(idx) ){
		PMIX_ERROR("The node %s [%d] already contributed to this collective",
				   pmix_info_nth_child_name(idx), nodeid);
		xfree(msg);
		return;
	}
	node_data[idx] = msg;
	node_sizes[idx] = size;
	if( pmix_state_coll_local_ok() ){
		_forward();
	}
}

void pmix_coll_task_contrib(uint32_t taskid, void *msg, uint32_t size)
{
	PMIX_DEBUG("Local task contribution %d", taskid);
	if( !pmix_state_task_contrib_ok(taskid) ){
		PMIX_ERROR_NO(0,"The task %d already contributed to this collective", taskid);
		return;
	}
	uint32_t full_size = sizeof(int)*2 + size;
	local_data[taskid]  = xmalloc( full_size );
	*((int*)local_data[taskid] ) = pmix_info_task_id(taskid);
	*((int*)local_data[taskid] + 1 ) = size;
	memcpy((void*)((int*)local_data[taskid] + 2), msg, size );
	local_sizes[taskid] = full_size;

	if( pmix_state_coll_local_ok() ){
		_forward();
	}
}


void pmix_coll_update_db(void *msg, uint32_t size)
{
	int i = 0;
	char *pay = (char*)msg;

	pmix_db_update_init();
	while( i < size ){
		int taskid = *(int*)pay;
		pay += sizeof(int);
		int blob_size = *(int*)pay;
		pay += sizeof(int);
		int *blob = xmalloc(blob_size);
		memcpy(blob, pay, blob_size);
		pay += blob_size;
		pmix_db_add_blob(taskid, blob, blob_size );
		i += blob_size + 2*sizeof(int);
	}
	pmix_db_update_verify();
}
