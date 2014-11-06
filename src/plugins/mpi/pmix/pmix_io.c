/*****************************************************************************\
 **  pmix_io.c - PMIx non-blocking IO routines
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

#include "pmix_common.h"
#include "pmix_io.h"
#include "pmix_debug.h"
#include "pmix_utils.h"

void pmix_io_init(pmix_io_engine_t *mstate, int fd, uint32_t _hsize, msg_pay_size_cb_t cb)
{
	// Initialize general options
	mstate->magic = PMIX_MSGSTATE_MAGIC;
	mstate->error = 0;
	mstate->fd = fd;
	mstate->hdr_size = _hsize;
	mstate->pay_size_cb = cb;
	mstate->operating = true;
	// Init receiver
	mstate->rcvd_header = xmalloc(_hsize);
	mstate->rcvd_pay_size = 0;
	mstate->rcvd_payload = NULL;
	mstate->rcvd_hdr_offs = mstate->rcvd_pay_offs = 0;
	mstate->rcvd_padding = 0;
	// Init transmitter
	mstate->send_current = NULL;
	mstate->send_offs = mstate->send_size = 0;
	mstate->send_queue = list_create(pmix_xfree_buffer);
}

void pmix_io_finalize(pmix_io_engine_t *mstate, int error)
{
	if( !mstate->operating ){
		return;
	}
	// Free transmitter
	if( list_count(mstate->send_queue) ){
		list_destroy(mstate->send_queue);
	}
	if( NULL != mstate->send_current ){
		xfree(mstate->send_current);
	}
	mstate->send_current = NULL;
	mstate->send_offs = mstate->send_size = 0;

	// Free receiver
	if( NULL != mstate->rcvd_payload){
		free(mstate->rcvd_payload);
	}
	xfree(mstate->rcvd_header);
	mstate->rcvd_header = NULL;
	mstate->rcvd_pay_size = 0;
	mstate->rcvd_payload = NULL;
	mstate->rcvd_hdr_offs = mstate->rcvd_pay_offs = 0;

	mstate->operating = false;
}

// Receiver

int pmix_io_first_header(int fd, void *buf, uint32_t *_offs, uint32_t len)
{
	bool shutdown;
	uint32_t offs = *_offs;

	// FIXME: is it ok to use this function for blocking receive?
	offs += pmix_read_buf(fd, buf + offs, len, &shutdown, true);
	*_offs = offs;
	if( shutdown ){
		return SLURM_ERROR;
	}
	return 0;
}

inline static void _pmix_rcvd_next_message(pmix_io_engine_t *mstate)
{
	xassert( mstate->magic == PMIX_MSGSTATE_MAGIC );
	xassert( mstate->rcvd_header != NULL );
	xassert( mstate->operating );

	mstate->rcvd_pad_recvd = 0;
	mstate->rcvd_hdr_offs = 0;
	mstate->rcvd_pay_offs = mstate->rcvd_pay_size = 0;
	mstate->rcvd_payload = NULL;

}

inline static void _pmix_rcvd_swithch_to_body(pmix_io_engine_t *mstate)
{
	xassert(mstate->magic == PMIX_MSGSTATE_MAGIC);
	xassert( mstate->operating );
	xassert(mstate->hdr_size == mstate->rcvd_hdr_offs);

	mstate->rcvd_pay_offs = 0;
	mstate->rcvd_pay_size = mstate->pay_size_cb(mstate->rcvd_header);
	mstate->rcvd_payload = xmalloc(mstate->rcvd_pay_size);
}

void pmix_io_add_hdr(pmix_io_engine_t *mstate, void *buf)
{
	xassert(mstate->magic == PMIX_MSGSTATE_MAGIC);
	xassert( mstate->operating );

	memcpy(mstate->rcvd_header, buf, mstate->hdr_size);
	mstate->rcvd_hdr_offs = mstate->hdr_size;
	_pmix_rcvd_swithch_to_body(mstate);
}

void pmix_io_rcvd(pmix_io_engine_t *mstate)
{
	size_t size, to_recv;
	bool shutdown;
	int fd = mstate->fd;

	xassert(mstate->magic == PMIX_MSGSTATE_MAGIC);
	xassert( mstate->operating );

	if( pmix_io_rcvd_ready(mstate) ){
		// nothing to do,
		// first the current message has to be extracted
		return;
	}

	// Drop padding first so it won't corrupt the message
	if( mstate->rcvd_padding && mstate->rcvd_pad_recvd < mstate->rcvd_padding ){
		char buf[mstate->rcvd_padding];
		size = mstate->rcvd_padding;
		to_recv = size - mstate->rcvd_pad_recvd;
		mstate->rcvd_pad_recvd += pmix_read_buf(fd, buf, to_recv, &shutdown, false);
		if( shutdown ){
			pmix_io_finalize(mstate, 0);
			return;
		}
		if( mstate->rcvd_pad_recvd < size ){
			// normal return. receive another portion of header later
			return;
		}
	}

	if( mstate->rcvd_hdr_offs < mstate->hdr_size ){
		// need to finish with the header
		size = mstate->hdr_size;
		to_recv = size - mstate->rcvd_hdr_offs;
		mstate->rcvd_hdr_offs += pmix_read_buf(fd, mstate->rcvd_header + mstate->rcvd_hdr_offs,
										   to_recv, &shutdown, false);
		if( shutdown ){
			pmix_io_finalize(mstate, 0);
			return;
		}
		if( mstate->rcvd_hdr_offs < size ){
			// normal return. receive another portion of header later
			return;
		}
		// if we are here then header is received and we can adjust buffer
		_pmix_rcvd_swithch_to_body(mstate);
		// go ahared with body receive
	}
	// we are receiving the body
	xassert( mstate->rcvd_hdr_offs == mstate->hdr_size );
	if( mstate->rcvd_pay_size == 0 ){
		// zero-byte message. exit. next time we will hit pmix_nbmsg_rcvd_ready
		return;
	}
	size = mstate->rcvd_pay_size;
	to_recv = size - mstate->rcvd_pay_offs;
	mstate->rcvd_pay_offs += pmix_read_buf(fd, mstate->rcvd_payload + mstate->rcvd_pay_offs,
									   to_recv, &shutdown, false);
	if( shutdown ){
		pmix_io_finalize(mstate, 0);
		return;
	}
	if( mstate->rcvd_pay_offs  == size ){
		// normal return. receive another portion later
		PMIX_DEBUG("Message is ready for processing!");
		return;
	}

}

void *pmix_io_rcvd_extract(pmix_io_engine_t *mstate, void *header)
{
	xassert(mstate->magic == PMIX_MSGSTATE_MAGIC );
	xassert( mstate->operating );
	xassert( pmix_io_rcvd_ready(mstate) );

	void *ptr = mstate->rcvd_payload;
	memcpy(header, mstate->rcvd_header, (size_t)mstate->hdr_size);
	// Drop message state to receive new one
	_pmix_rcvd_next_message(mstate);
	return ptr;
}

// Transmitter


inline static void _pmix_send_setup_current(pmix_io_engine_t *mstate, void *msg)
{
	xassert(mstate->magic == PMIX_MSGSTATE_MAGIC );
	xassert( mstate->operating );

	mstate->send_current = msg;
	mstate->send_offs = 0;
	mstate->send_size = mstate->hdr_size + mstate->pay_size_cb(msg);
}

void pmix_io_send_enqueue(pmix_io_engine_t *mstate, void *msg)
{
	xassert(mstate->magic == PMIX_MSGSTATE_MAGIC );
	xassert( mstate->operating );
	if( mstate->send_current == NULL ){
		_pmix_send_setup_current(mstate, msg);
	}else{
		list_enqueue(mstate->send_queue, msg);
	}
	pmix_io_send_progress(mstate);
}

bool pmix_io_send_pending(pmix_io_engine_t *mstate)
{
	xassert(mstate->magic == PMIX_MSGSTATE_MAGIC );
	xassert( mstate->operating );

	if( mstate->send_size && mstate->send_offs == mstate->send_size ){
		// The current message is send. Cleanup current msg
		xassert(mstate->send_current != NULL);
		xfree(mstate->send_current);
		mstate->send_current = NULL;
		mstate->send_offs = mstate->send_size = 0;
	}
	if( mstate->send_current == NULL ){
		// Try next element
		int n = list_count(mstate->send_queue);
		if( n == 0 ){
			// Nothing to do
			return false;
		}
		mstate->send_current = list_dequeue(mstate->send_queue);
		xassert( mstate->send_current != NULL );
		mstate->send_size = mstate->hdr_size + mstate->pay_size_cb(mstate->send_current);
	}
	return true;
}

void pmix_io_send_progress(pmix_io_engine_t *mstate)
{
	xassert(mstate->magic == PMIX_MSGSTATE_MAGIC );
	xassert( mstate->operating );
	int fd = mstate->fd;

	while( pmix_io_send_pending(mstate) ){
		// try to send everything
		// FIXME: maybe set some restriction on number of messages sended at once
		bool shutdown = false;
		uint32_t to_write = mstate->send_size - mstate->send_offs;
		int cnt = pmix_write_buf(fd, mstate->send_current + mstate->send_offs, to_write, &shutdown);
		if( shutdown ){
			pmix_io_finalize(mstate,0);
			return;
		}
		if( cnt == 0 ){
			break;
		}
		mstate->send_offs += cnt;
	}
}
