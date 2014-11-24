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

void pmix_io_init(pmix_io_engine_t *eng, int fd, pmix_io_engine_header_t header)
{
	// Initialize general options
	eng->magic = PMIX_MSGSTATE_MAGIC;
	eng->error = 0;
	eng->fd = fd;
	eng->header = header;
	eng->operating = true;

	if( header.pack_hdr_cb == NULL && header.unpack_hdr_cb == NULL ){
		xassert( header.host_size == header.net_size );
	}

	// Init receiver
	eng->rcvd_hdr = xmalloc(eng->header.net_size);
	if( eng->header.unpack_hdr_cb ){
		eng->rcvd_hdr_host = xmalloc(eng->header.host_size);
	} else {
		eng->rcvd_hdr_host = eng->rcvd_hdr;
	}

	eng->rcvd_pay_size = 0;
	eng->rcvd_payload = NULL;
	eng->rcvd_hdr_offs = eng->rcvd_pay_offs = 0;
	eng->rcvd_padding = 0;
	// Init transmitter
	eng->send_current = NULL;
	if( eng->header.pack_hdr_cb ){
		eng->send_hdr_net = xmalloc(eng->header.net_size);
	}
	eng->send_hdr_size = eng->send_hdr_offs = 0;
	eng->send_payload = NULL;
	eng->send_pay_size = eng->send_pay_offs = 0;
	eng->send_queue = list_create(pmix_xfree_buffer);
}

void pmix_io_finalize(pmix_io_engine_t *eng, int error)
{
	if( !eng->operating ){
		return;
	}
	eng->operating = false;

	// Free transmitter
	if( list_count(eng->send_queue) ){
		list_destroy(eng->send_queue);
	}
	if( NULL != eng->send_current ){
		xfree(eng->send_current);
	}
	eng->send_current = NULL;
	eng->send_payload = NULL;
	eng->send_pay_size = eng->send_pay_offs = 0;
	if( eng->header.pack_hdr_cb ){
		xfree( eng->send_hdr_net );
	}
	eng->send_hdr_size = eng->send_hdr_offs = 0;

	// Free receiver
	if( NULL != eng->rcvd_payload){
		xfree(eng->rcvd_payload);
	}

	xfree( eng->rcvd_hdr );
	if( eng->header.unpack_hdr_cb ){
		xfree( eng->rcvd_hdr_host );
	}
	eng->rcvd_hdr = NULL;
	eng->rcvd_hdr_host = NULL;

	eng->rcvd_pay_size = 0;
	eng->rcvd_payload = NULL;
	eng->rcvd_hdr_offs = eng->rcvd_pay_offs = 0;


	if( error < 0 ){
		eng->error = -error;
	} else {
		eng->error = 0;
	}
}

// Receiver

int pmix_io_first_header(int fd, void *buf, uint32_t *_offs, uint32_t len)
{
	int shutdown;
	uint32_t offs = *_offs;

	offs += pmix_read_buf(fd, buf + offs, len, &shutdown, true);
	*_offs = offs;
	if( shutdown ){
		if( shutdown < 0 ){
			PMIX_ERROR_NO(shutdown, "Unexpected connection close");
		} else {
			PMIX_DEBUG("Unexpected connection close");
		}
		return SLURM_ERROR;
	}
	return 0;
}

inline static void _rcvd_next_message(pmix_io_engine_t *eng)
{
	xassert( eng->magic == PMIX_MSGSTATE_MAGIC );
	xassert( eng->rcvd_hdr != NULL );
	xassert( eng->operating );

	eng->rcvd_pad_recvd = 0;
	eng->rcvd_hdr_offs = 0;
	eng->rcvd_pay_offs = eng->rcvd_pay_size = 0;
	eng->rcvd_payload = NULL;

}

inline static int _rcvd_swithch_to_body(pmix_io_engine_t *eng)
{
	int rc;
	xassert(eng->magic == PMIX_MSGSTATE_MAGIC);
	xassert( eng->operating );
	xassert(eng->header.net_size == eng->rcvd_hdr_offs);


	eng->rcvd_pay_offs = eng->rcvd_pay_size = 0;
	eng->rcvd_payload = NULL;
	if( eng->header.unpack_hdr_cb ){
		// If this is inter-node communication - unpack the buffer first
		if( (rc = eng->header.unpack_hdr_cb(eng->rcvd_hdr, eng->rcvd_hdr_host)) ){
			PMIX_ERROR_NO(rc, "Cannot unpack message header");
			return rc;
		}
	}
	eng->rcvd_pay_size = eng->header.pay_size_cb(eng->rcvd_hdr_host);
	eng->rcvd_payload = xmalloc(eng->rcvd_pay_size);
	return 0;
}

inline static bool _rcvd_have_padding(pmix_io_engine_t *eng)
{
	return eng->rcvd_padding && eng->rcvd_pad_recvd < eng->rcvd_padding;
}

inline static bool _rcvd_need_header(pmix_io_engine_t *eng)
{
	return eng->rcvd_hdr_offs < eng->header.net_size;
}


/*
 * Add already unpacked (converted from net to host format) header.
 */
void pmix_io_add_hdr(pmix_io_engine_t *eng, void *buf)
{
	int rc;
	xassert(eng->magic == PMIX_MSGSTATE_MAGIC);
	xassert( eng->operating );

	memcpy(eng->rcvd_hdr_host, buf, eng->header.host_size);
	eng->rcvd_hdr_offs = eng->header.net_size;
	if( (rc = _rcvd_swithch_to_body(eng) ) ){
		pmix_io_finalize(eng, rc);
	}
}

void pmix_io_rcvd(pmix_io_engine_t *eng)
{
	size_t size, remain;
	void *offs;
	int shutdown;
	int fd = eng->fd;

	xassert(eng->magic == PMIX_MSGSTATE_MAGIC);

	if( pmix_io_finalized(eng) ){
		return;
	}

	if( pmix_io_rcvd_ready(eng) ){
		// nothing to do,
		// first the current message has to be extracted
		return;
	}

	// Drop padding first so it won't corrupt the message
	if( _rcvd_have_padding(eng) ){
		char buf[eng->rcvd_padding];
		size = eng->rcvd_padding;
		remain = size - eng->rcvd_pad_recvd;
		eng->rcvd_pad_recvd += pmix_read_buf(fd, buf, remain, &shutdown, false);
		if( shutdown ){
			pmix_io_finalize(eng, 0);
			return;
		}
		if( eng->rcvd_pad_recvd < size ){
			// normal return. receive another portion of header later
			return;
		}
	}

	if( _rcvd_need_header(eng) ){
		// need to finish with the header
		size = eng->header.net_size;
		remain = size - eng->rcvd_hdr_offs;
		offs = eng->rcvd_hdr + eng->rcvd_hdr_offs;
		eng->rcvd_hdr_offs += pmix_read_buf(fd, offs, remain, &shutdown, false);
		if( shutdown ){
			pmix_io_finalize(eng, shutdown);
			return;
		}
		if( eng->rcvd_hdr_offs < size ){
			// normal return. receive another portion of header later
			return;
		}
		// if we are here then header is received and we can adjust buffer
		if( (shutdown = _rcvd_swithch_to_body(eng) ) ){
			pmix_io_finalize(eng, shutdown);
			return;
		}
		// go ahared with body receive
	}
	// we are receiving the body
	xassert( eng->rcvd_hdr_offs == eng->header.net_size );
	if( eng->rcvd_pay_size == 0 ){
		// zero-byte message. exit. next time we will hit pmix_nbmsg_rcvd_ready
		return;
	}
	size = eng->rcvd_pay_size;
	remain = size - eng->rcvd_pay_offs;
	eng->rcvd_pay_offs += pmix_read_buf(fd, eng->rcvd_payload + eng->rcvd_pay_offs,
										remain, &shutdown, false);
	if( shutdown ){
		pmix_io_finalize(eng, 0);
		return;
	}
	if( eng->rcvd_pay_offs  == size ){
		// normal return. receive another portion later
		PMIX_DEBUG("Message is ready for processing!");
		return;
	}

}

void *pmix_io_rcvd_extract(pmix_io_engine_t *eng, void *header)
{
	xassert(eng->magic == PMIX_MSGSTATE_MAGIC );
	xassert( eng->operating );
	xassert( pmix_io_rcvd_ready(eng) );

	void *ptr = eng->rcvd_payload;
	memcpy(header, eng->rcvd_hdr_host, (size_t)eng->header.host_size);
	// Drop message state to receive new one
	_rcvd_next_message(eng);
	return ptr;
}

// Transmitter


inline static int _send_set_current(pmix_io_engine_t *eng, void *msg)
{
	xassert(eng->magic == PMIX_MSGSTATE_MAGIC );
	xassert( eng->operating );

	// Set message basis
	eng->send_current = msg;

	// Setup header for sending
	if( eng->header.pack_hdr_cb ){
		eng->send_hdr_size = eng->header.pack_hdr_cb(msg, eng->send_hdr_net);
		xassert( eng->send_hdr_size > 0 );
	}else{
		eng->send_hdr_net = msg;
		eng->send_hdr_size = eng->header.net_size;
	}
	eng->send_hdr_offs = 0;

	// Setup payload for sending
	eng->send_payload = (char*)msg + eng->header.host_size;
	eng->send_pay_size = eng->header.pay_size_cb(msg);
	eng->send_pay_offs = 0;
	return 0;
}

inline static void _send_free_current(pmix_io_engine_t *eng)
{
	xassert(eng->magic == PMIX_MSGSTATE_MAGIC );
	xassert( eng->operating );
	xassert( eng->send_current );

	eng->send_payload = NULL;
	eng->send_pay_size = eng->send_pay_offs = 0;

	if( eng->header.pack_hdr_cb == NULL ){
		eng->send_hdr_net = NULL;
	}
	eng->send_hdr_size = eng->send_hdr_offs = 0;
	xfree( eng->send_current );
	eng->send_current = NULL;
}

inline static int _send_header_ok(pmix_io_engine_t *eng)
{
	xassert(eng->magic == PMIX_MSGSTATE_MAGIC );
	xassert( eng->operating );
	xassert( eng->send_current != NULL );

	return (eng->send_current != NULL) &&
			(eng->send_hdr_size > 0) &&
			(eng->send_hdr_offs == eng->send_hdr_size);
}

inline static int _send_payload_ok(pmix_io_engine_t *eng)
{
	xassert(eng->magic == PMIX_MSGSTATE_MAGIC );
	xassert( eng->operating );

	return (eng->send_current != NULL) && _send_header_ok(eng) &&
			(eng->send_pay_size > 0) && (eng->send_pay_offs == eng->send_pay_size);
}

void pmix_io_send_enqueue(pmix_io_engine_t *eng, void *msg)
{
	xassert(eng->magic == PMIX_MSGSTATE_MAGIC );
	xassert( eng->operating );
	if( eng->send_current == NULL ){
		_send_set_current(eng, msg);
	}else{
		list_enqueue(eng->send_queue, msg);
	}
	pmix_io_send_progress(eng);
}

bool pmix_io_send_pending(pmix_io_engine_t *eng)
{
	int rc;

	xassert(eng->magic == PMIX_MSGSTATE_MAGIC );
	xassert( eng->operating );

	if( _send_payload_ok(eng) ){
		// The current message is send. Cleanup current msg
		_send_free_current(eng);
	}

	if( eng->send_current == NULL ){
		// Try next element
		int n = list_count(eng->send_queue);
		if( n == 0 ){
			// Nothing to do
			return false;
		}
		void *msg = list_dequeue(eng->send_queue);
		xassert( msg != NULL );
		if( (rc = _send_set_current(eng, msg) ) ){
			PMIX_ERROR_NO(rc,"Cannot switch to the next message");
			pmix_io_finalize(eng, rc);
		}
	}
	return true;
}

void pmix_io_send_progress(pmix_io_engine_t *eng)
{
	int fd = eng->fd;
	uint32_t size, remain;
	void *offs;

	xassert(eng->magic == PMIX_MSGSTATE_MAGIC );
	xassert( eng->operating );

	while( pmix_io_send_pending(eng) ){
		// try to send everything untill fd became blockable
		// FIXME: maybe set some restriction on number of messages sended at once
		int shutdown = 0;
		if( !_send_header_ok(eng) ){
			size = eng->send_hdr_size;
			remain = size - eng->send_hdr_offs;
			offs = eng->send_hdr_net + eng->send_hdr_offs;
			int cnt = pmix_write_buf(fd, offs, remain, &shutdown);
			if( shutdown ){
				pmix_io_finalize(eng, shutdown);
				return;
			}
			if( cnt == 0 ){
				break;
			}
			eng->send_hdr_offs += cnt;
			if( !_send_header_ok(eng) ){
				// Go to the next interation and try to finish header reception
				continue;
			}
		}

		if( _send_header_ok(eng) ){
			size = eng->send_pay_size;
			remain = size - eng->send_pay_offs;
			offs = eng->send_payload + eng->send_pay_offs;
			int cnt = pmix_write_buf(fd, offs, remain, &shutdown);
			if( shutdown ){
				pmix_io_finalize(eng, shutdown);
				return;
			}
			if( cnt == 0 ){
				break;
			}
			eng->send_pay_offs += cnt;
		}
	}
}
