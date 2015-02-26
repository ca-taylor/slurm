/*****************************************************************************\
 **	pmix_utils.c - Various PMIx utility functions
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
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <errno.h>
#include <poll.h>
#include <time.h>

#include "pmixp_common.h"
#include "pmixp_utils.h"
#include "pmixp_debug.h"

#define PMIX_MAX_RETRY 7

void pmix_xfree_buffer(void *x){
	xfree(x);
}

int pmix_usock_create_srv(char *path)
{
	static struct sockaddr_un sa;
	int ret = 0;

	if( strlen(path) >= sizeof(sa.sun_path) ){
		PMIX_ERROR("The size of UNIX sockety path is greater than possible: "
				   "%lu, max %lu", (unsigned long)strlen(path), (unsigned long)sizeof(sa.sun_path)-1);
		return -1;
	}

	int fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if( fd < 0 ){
		PMIX_ERROR("Cannot create UNIX socket");
		return fd;
	}

	memset(&sa, 0, sizeof(sa));
	sa.sun_family = AF_UNIX;

	strcpy(sa.sun_path, path);
	if( ret = bind(fd, (struct sockaddr*)&sa, SUN_LEN(&sa)) ){
		PMIX_ERROR("Cannot bind() UNIX socket %s", path);
		goto err_fd;
	}

	if( (ret = listen(fd, 64)) ){
		PMIX_ERROR("Cannot bind() UNIX socket %s", path);
		goto err_bind;

	}
	return fd;

err_bind:
	close(fd);
	unlink(path);
	return ret;

err_fd:
	close(fd);
	return ret;
}

size_t pmix_read_buf(int fd, void *buf, size_t count, int *shutdown, bool blocking)
{
	ssize_t ret, offs = 0;

	*shutdown = 0;

	if( !blocking && !pmix_fd_read_ready(fd, shutdown) ){
		return 0;
	}

	while( count - offs > 0 ) {
		ret = read(fd, (char*)buf + offs, count - offs);
		if( ret > 0 ){
			offs += ret;
			continue;
		} else if( ret == 0 ){
			// connection closed.
			*shutdown = 1;
			return offs;
		}
		switch( errno ){
		case EINTR:
			continue;
		case EWOULDBLOCK:
			return offs;
		default:
			PMIX_ERROR("blocking=%d",blocking);
			*shutdown = -errno;
			return offs;
		}
	}
	return offs;
}

size_t pmix_write_buf(int fd, void *buf, size_t count, int *shutdown)
{
	ssize_t ret, offs = 0;

	*shutdown = 0;

	if( !pmix_fd_write_ready(fd, shutdown) ){
		return 0;
	}

	while( count - offs > 0 ) {
		ret = write(fd, (char*)buf + offs, count - offs);
		if( ret > 0 ){
			offs += ret;
			continue;
		}
		switch( errno ){
		case EINTR:
			continue;
		case EWOULDBLOCK:
			return offs;
		default:
			*shutdown = -errno;
			return offs;
		}
	}
	return offs;
}

bool pmix_fd_read_ready(int fd, int *shutdown)
{
	struct pollfd pfd[1];
	int    rc;
	pfd[0].fd     = fd;
	pfd[0].events = POLLIN;

	// Drop shutdown before the check
	*shutdown = 0;

	rc = poll(pfd, 1, 10);
	if( rc < 0 ){
		*shutdown = -errno;
		return false;
	}
	bool ret = ((rc == 1) && (pfd[0].revents & POLLIN));
	if( !ret && (pfd[0].revents & ( POLLERR | POLLHUP | POLLNVAL))){
		if( pfd[0].revents & ( POLLERR | POLLNVAL) ){
			*shutdown = -EBADF;
		} else {
			// POLLHUP - normal connection close
			*shutdown = 1;
		}
	}
	return ret;
}


bool pmix_fd_write_ready(int fd, int *shutdown)
{
	struct pollfd pfd[1];
	int    rc;
	pfd[0].fd     = fd;
	pfd[0].events = POLLOUT;
	rc = poll(pfd, 1, 10);
	if( rc < 0 ){
		*shutdown = -errno;
		return false;
	}
	if( pfd[0].revents & ( POLLERR | POLLHUP | POLLNVAL) ){
		if( pfd[0].revents & ( POLLERR | POLLNVAL) ){
			*shutdown = -EBADF;
		} else {
			// POLLHUP - normal connection close
			*shutdown = 1;
		}
	}
	return ((rc == 1) && (pfd[0].revents & POLLOUT));
}


int pmix_stepd_send(char *nodelist, char *address, uint32_t len, char *data)
{
	int retry = 0, rc;
	unsigned int delay = 100; /* in milliseconds */
	while (1) {
		if (retry == 1) {
			PMIX_DEBUG("send failed, rc=%d, retrying", rc);
		}

		rc = slurm_forward_data(nodelist, address, len, data);

		if (rc == SLURM_SUCCESS)
			break;
		retry++;
		if (retry >= PMIX_MAX_RETRY )
			break;
		/* wait with constantly increasing delay */
		struct timespec ts = { (delay/1000), ((delay % 1000) * 1000000) };
		nanosleep(&ts, NULL);
		delay *= 2;
	}
	return rc;
}

int pmix_srun_send(slurm_addr_t *addr, uint32_t len, char *data)
{
	int retry = 0, rc;
	unsigned int delay = 100; /* in milliseconds */
	int fd;
	fd = slurm_open_stream(addr, true);
	if (fd < 0){
		PMIX_ERROR("Cannot send collective data to srun (slurm_open_stream)");
		return SLURM_ERROR;
	}

	while (1) {
		if (retry == 1) {
			PMIX_DEBUG("send failed, rc=%d, retrying", rc);
		}

		rc = slurm_msg_sendto(fd, data, len, SLURM_PROTOCOL_NO_SEND_RECV_FLAGS);
		if (rc == len ){ /* all data sent */
			rc = SLURM_SUCCESS;
			break;
		}
		rc = SLURM_ERROR;

		retry++;
		if (retry >= PMIX_MAX_RETRY )
			break;
		/* wait with constantly increasing delay */
		struct timespec ts = { (delay/1000), ((delay % 1000) * 1000000) };
		nanosleep(&ts, NULL);
		delay *= 2;
	}
	close(fd);

	return rc;
}
