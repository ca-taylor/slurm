/*****************************************************************************\
 **  pmix_agent.c - PMIx agent thread
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

#include <pthread.h>
#include <sched.h>
#include <poll.h>
#include <arpa/inet.h>
#include <time.h>

#include "pmixp_common.h"
#include "pmixp_server.h"
#include "pmixp_client.h"
#include "pmixp_state.h"
#include "pmixp_debug.h"
#include "pmixp_nspaces.h"
#include "pmixp_utils.h"

#define MAX_RETRIES 5

static pthread_t _agent_tid = 0;
static int _agent_is_running = 0;
static eio_handle_t *_io_handle = NULL;

static bool _server_conn_readable(eio_obj_t *obj);
static int  _server_conn_read(eio_obj_t *obj, List objs);

static struct io_operations srv_ops = {
	.readable    = &_server_conn_readable,
	.handle_read = &_server_conn_read
};

static bool _server_conn_readable(eio_obj_t *obj)
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

static int
_server_conn_read(eio_obj_t *obj, List objs)
{
	int fd;
	struct sockaddr addr;
	socklen_t size = sizeof(addr);
	int shutdown = 0;

	PMIXP_DEBUG("fd = %d", obj->fd);

	while (1) {
		// Return early if fd is not now ready
		if (!pmixp_fd_read_ready(obj->fd, &shutdown ) ){
			if( shutdown ){
				obj->shutdown = true;
				if( shutdown < 0 ){
					PMIXP_ERROR_NO(shutdown, "sd=%d failure", obj->fd);
				}
			}
			return 0;
		}

		while ((fd = accept(obj->fd, &addr, &size)) < 0) {
			if (errno == EINTR)
				continue;
			if (errno == EAGAIN)    /* No more connections */
				return 0;
			if ((errno == ECONNABORTED) ||
					(errno == EWOULDBLOCK)) {
				return 0;
			}
			PMIXP_ERROR_STD("accept()ing connection sd=%d", obj->fd);
			return 0;
		}

		PMIXP_DEBUG("accepted connection: sd=%d", fd);
		/* read command from socket and handle it */
		pmix_server_new_conn(fd);
	}
	return 0;
}

/*
 * main loop of agent thread
 */
static void *_agent(void * unused)
{
	PMIXP_DEBUG("Start agent thread");
	eio_obj_t *srv_obj;

	_io_handle = eio_handle_create(0);

	srv_obj = eio_obj_create(pmixp_info_srv_fd(), &srv_ops, (void *)(-1));
	eio_new_initial_obj(_io_handle, srv_obj);

	pmixp_info_io_set(_io_handle);

	_agent_is_running = 1;
	eio_handle_mainloop(_io_handle);
	_agent_is_running = 0;

	PMIXP_DEBUG("agent thread exit");

	eio_handle_destroy(_io_handle);
	return NULL;
}

int pmix_agent_start(void)
{
	int retries = 0;
	pthread_attr_t attr;

	slurm_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
	while ((errno = pthread_create(&_agent_tid, &attr, _agent, NULL))) {
		if (++retries > MAX_RETRIES) {
			PMIXP_ERROR_STD("pthread_create error");
			slurm_attr_destroy(&attr);
			return SLURM_ERROR;
		}
		sleep(1);
	}
	slurm_attr_destroy(&attr);

	/* wait for the agent thread to initialize */
	while( !_agent_is_running ){
		sched_yield();
	}

	PMIXP_DEBUG("started agent thread (%lu)", (unsigned long) _agent_tid);

	return SLURM_SUCCESS;
}

int pmix_agent_stop(void)
{
	eio_signal_shutdown(_io_handle);
	/* wait for the agent thread to stop */
	while( _agent_is_running ){
		sched_yield();
	}
	return 0;
}
