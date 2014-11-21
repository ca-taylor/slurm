/*****************************************************************************\
 **  pmix_agent.c - PMIx agent thread
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

#include <pthread.h>
#include <poll.h>
#include <arpa/inet.h>

#include "pmix_common.h"
#include "pmix_server.h"
#include "pmix_client.h"
#include "pmix_state.h"
#include "pmix_debug.h"
#include "pmix_db.h"
#include "pmix_utils.h"

#define MAX_RETRIES 5

static pthread_t pmix_agent_tid = 0;
static eio_handle_t *pmix_io_handle = NULL;

static bool _server_conn_readable(eio_obj_t *obj);
static int  _server_conn_read(eio_obj_t *obj, List objs);

static struct io_operations srv_ops = {
	.readable    = &_server_conn_readable,
	.handle_read = &_server_conn_read,
};


static bool _cli_conn_readable(eio_obj_t *obj);
static int  _cli_conn_read(eio_obj_t *obj, List objs);
/* static bool _task_writable(eio_obj_t *obj); */
/* static int  _task_write(eio_obj_t *obj, List objs); */
static struct io_operations cli_ops = {
	.readable    =  &_cli_conn_readable,
	.handle_read =  &_cli_conn_read,
};


static bool _server_conn_readable(eio_obj_t *obj)
{
	PMIX_DEBUG("fd = %d", obj->fd);
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

static int
_server_conn_read(eio_obj_t *obj, List objs)
{
	int fd;
	struct sockaddr addr;
	struct sockaddr_in *sin;
	socklen_t size = sizeof(addr);
	char buf[INET_ADDRSTRLEN];
	int shutdown = 0;

	PMIX_DEBUG("fd = %d", obj->fd);

	while (1) {
		// Return early if fd is not now ready
		if (!pmix_fd_read_ready(obj->fd, &shutdown ) ){
			if( shutdown ){
				obj->shutdown = true;
				if( shutdown < 0 ){
					PMIX_ERROR_NO(shutdown, "sd=%d failure", obj->fd);
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
			PMIX_ERROR("accept()ing connection sd=%d", obj->fd);
			return 0;
		}

		if( pmix_info_is_srun() ){
			sin = (struct sockaddr_in *) &addr;
			inet_ntop(AF_INET, &sin->sin_addr, buf, INET_ADDRSTRLEN);
			PMIX_DEBUG("accepted connection: ip=%s sd=%d", buf, fd);
		} else {
			PMIX_DEBUG("accepted connection: sd=%d", fd);
		}
		/* read command from socket and handle it */
		pmix_server_new_conn(fd);
	}
	return 0;
}

// Client request processing
static bool _cli_conn_readable(eio_obj_t *obj)
{
	// FIXME: What should we do in addition?
	PMIX_DEBUG("fd = %d", obj->fd);
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

static int _cli_conn_read(eio_obj_t *obj, List objs)
{
	int fd;
	int shutdown;

	PMIX_DEBUG("fd = %d", obj->fd);

	while (1) {
		// Return early if fd is not now ready
		if (!pmix_fd_read_ready(obj->fd, &shutdown)){
			if( shutdown ){
				// The error occurs or fd was closed
				if( shutdown < 0 ){
					obj->shutdown = true;
					PMIX_ERROR_NO(-shutdown, "sd=%d failure", obj->fd);
				}
			}
			return 0;
		}

		while ((fd = accept(obj->fd, NULL, 0)) < 0) {
			if (errno == EINTR)
				continue;
			if (errno == EAGAIN)    /* No more connections */
				return 0;
			if ((errno == ECONNABORTED) ||
					(errno == EWOULDBLOCK)) {
				return 0;
			}
			PMIX_ERROR("unable to accept new connection");
			return 0;
		}

		if( pmix_info_is_srun() ){
			PMIX_ERROR("srun shouldn't be directly connected to clients");
			return 0;
		}

		/* read command from socket and handle it */
		pmix_client_new_conn(fd);
	}
	return 0;
}


/*
 * main loop of agent thread
 */
static void *_agent(void * unused)
{
	PMIX_DEBUG("Start agent thread");
	eio_obj_t *srv_obj, *cli_obj;

	pmix_io_handle = eio_handle_create();

	//fd_set_nonblocking(tree_sock);
	srv_obj = eio_obj_create(pmix_info_srv_fd(), &srv_ops, (void *)(-1));
	eio_new_initial_obj(pmix_io_handle, srv_obj);

	/* for stepd, add the sockets to tasks */
	if( pmix_info_is_stepd() ) {
		cli_obj = eio_obj_create(pmix_info_cli_fd(), &cli_ops, (void*)(long)(-1));
		eio_new_initial_obj(pmix_io_handle, cli_obj);
	}

	pmix_state_init();
	pmix_db_init();
	pmix_info_io_set(pmix_io_handle);

	eio_handle_mainloop(pmix_io_handle);

	PMIX_DEBUG("agent thread exit");

	//pmix_state_destroy();
	eio_handle_destroy(pmix_io_handle);
	return NULL;
}


int pmix_agent_start(void)
{
	int retries = 0;
	pthread_attr_t attr;

	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
	while ((errno = pthread_create(&pmix_agent_tid, &attr, _agent, NULL))) {
		if (++retries > MAX_RETRIES) {
			PMIX_ERROR("pthread_create error");
			slurm_attr_destroy(&attr);
			return SLURM_ERROR;
		}
		sleep(1);
	}
	slurm_attr_destroy(&attr);
	PMIX_DEBUG("started agent thread (%lu)", (unsigned long) pmix_agent_tid);

	return SLURM_SUCCESS;
}

void pmix_agent_task_cleanup()
{
	close(pmix_info_cli_fd());
	close(pmix_info_srv_fd());
}
