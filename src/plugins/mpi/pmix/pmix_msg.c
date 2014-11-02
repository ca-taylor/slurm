#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <errno.h>

#include "pmix_common.h"
#include "pmix_msg.h"
#include "pmix_debug.h"

// Socket processing

int pmix_comm_srvsock_create(char *path)
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

static size_t _read_buf(int fd, void *buf, size_t count, bool *shutdown, bool blocking)
{
  ssize_t ret, offs = 0;

  *shutdown = false;

  if( !blocking && !pmix_comm_fd_read_ready(fd) ){
    return 0;
  }

  while( count - offs > 0 ) {
    ret = read(fd, (char*)buf + offs, count - offs);
    if( ret > 0 ){
      offs += ret;
      continue;
    } else if( ret == 0 ){
      // closed connection. shutdown.
      goto err_exit;
    }
    switch( errno ){
    case EINTR:
      continue;
    case EWOULDBLOCK:
      return offs;
    default:
      PMIX_ERROR("blocking=%d",blocking);
      goto err_exit;
    }
  }
  return offs;
err_exit:
  *shutdown = true;
  return offs;
}
static size_t _write_buf(int fd, void *buf, size_t count, bool *shutdown)
{
  ssize_t ret, offs = 0;

  *shutdown = false;

//  if( !pmix_comm_fd_write_ready(fd) ){
//    return 0;
//  }

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
      goto err_exit;
    }
  }
  return offs;
err_exit:
  *shutdown = true;
  return offs;
}

// Message processing

static void _free_msg(void *x){
  xfree(x);
}

void pmix_nbmsg_init(pmix_msgengine_t *mstate, int fd, uint32_t _hsize, msg_pay_size_cb_t cb)
{
  // Initialize general options
  mstate->magic = PMIX_MSGSTATE_MAGIC;
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
  mstate->send_queue = list_create(_free_msg);
}

void pmix_nbmsg_finalize(pmix_msgengine_t *mstate)
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

int pmix_nbmsg_first_header(int fd, void *buf, uint32_t *_offs, uint32_t len)
{
  bool shutdown;
  uint32_t offs = *_offs;

  // FIXME: is it ok to use this function for blocking receive?
  offs += _read_buf(fd, buf + offs, len, &shutdown, true);
  *_offs = offs;
  if( shutdown ){
    return SLURM_ERROR;
  }
  return 0;
}

inline static void _pmix_rcvd_next_message(pmix_msgengine_t *mstate)
{
  xassert( mstate->magic == PMIX_MSGSTATE_MAGIC );
  xassert( mstate->rcvd_header != NULL );
  xassert( mstate->operating );

  mstate->rcvd_pad_recvd = 0;
  mstate->rcvd_hdr_offs = 0;
  mstate->rcvd_pay_offs = mstate->rcvd_pay_size = 0;
  mstate->rcvd_payload = NULL;

}

inline static void _pmix_rcvd_swithch_to_body(pmix_msgengine_t *mstate)
{
  xassert(mstate->magic == PMIX_MSGSTATE_MAGIC);
  xassert( mstate->operating );
  xassert(mstate->hdr_size == mstate->rcvd_hdr_offs);

  mstate->rcvd_pay_offs = 0;
  mstate->rcvd_pay_size = mstate->pay_size_cb(mstate->rcvd_header);
  mstate->rcvd_payload = xmalloc(mstate->rcvd_pay_size);
}

void pmix_nbmsg_add_hdr(pmix_msgengine_t *mstate, void *buf)
{
  xassert(mstate->magic == PMIX_MSGSTATE_MAGIC);
  xassert( mstate->operating );

  memcpy(mstate->rcvd_header, buf, mstate->hdr_size);
  mstate->rcvd_hdr_offs = mstate->hdr_size;
  _pmix_rcvd_swithch_to_body(mstate);
}

void pmix_nbmsg_rcvd(pmix_msgengine_t *mstate)
{
  size_t size, to_recv;
  bool shutdown;
  int fd = mstate->fd;

  xassert(mstate->magic == PMIX_MSGSTATE_MAGIC);
  xassert( mstate->operating );

  if( pmix_nbmsg_rcvd_ready(mstate) ){
    // nothing to do
    return;
  }

  // Drop padding first so it won't corrupt the message
  if( mstate->rcvd_padding && mstate->rcvd_pad_recvd < mstate->rcvd_padding ){
    char buf[mstate->rcvd_padding];
    size = mstate->rcvd_padding;
    to_recv = size - mstate->rcvd_pad_recvd;
    mstate->rcvd_pad_recvd += _read_buf(fd, buf, to_recv, &shutdown, false);
    if( shutdown ){
      pmix_nbmsg_finalize(mstate);
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
    mstate->rcvd_hdr_offs += _read_buf(fd, mstate->rcvd_header + mstate->rcvd_hdr_offs,
                                       to_recv, &shutdown, false);
    if( shutdown ){
      pmix_nbmsg_finalize(mstate);
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
  mstate->rcvd_pay_offs += _read_buf(fd, mstate->rcvd_payload + mstate->rcvd_pay_offs,
                                     to_recv, &shutdown, false);
  if( shutdown ){
    pmix_nbmsg_finalize(mstate);
    return;
  }
  if( mstate->rcvd_pay_offs  == size ){
    // normal return. receive another portion later
    PMIX_DEBUG("Message is ready for processing!");
    return;
  }

}

void *pmix_nbmsg_rcvd_extract(pmix_msgengine_t *mstate, void *header)
{
  xassert(mstate->magic == PMIX_MSGSTATE_MAGIC );
  xassert( mstate->operating );
  xassert( pmix_nbmsg_rcvd_ready(mstate) );

  void *ptr = mstate->rcvd_payload;
  memcpy(header, mstate->rcvd_header, (size_t)mstate->hdr_size);
  // Drop message state to receive new one
  _pmix_rcvd_next_message(mstate);
  return ptr;
}

// Transmitter


inline static void _pmix_send_setup_current(pmix_msgengine_t *mstate, void *msg)
{
  xassert(mstate->magic == PMIX_MSGSTATE_MAGIC );
  xassert( mstate->operating );

  mstate->send_current = msg;
  mstate->send_offs = 0;
  mstate->send_size = mstate->hdr_size + mstate->pay_size_cb(msg);
}

void pmix_nbmsg_send_enqueue(pmix_msgengine_t *mstate, void *msg)
{
  xassert(mstate->magic == PMIX_MSGSTATE_MAGIC );
  xassert( mstate->operating );
  int fd = mstate->fd;
  if( mstate->send_current == NULL ){
    _pmix_send_setup_current(mstate, msg);
  }else{
    list_enqueue(mstate->send_queue, msg);
  }
  pmix_nbmsg_send_progress(mstate);
}

bool pmix_nbmsg_send_pending(pmix_msgengine_t *mstate)
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

void pmix_nbmsg_send_progress(pmix_msgengine_t *mstate)
{
  xassert(mstate->magic == PMIX_MSGSTATE_MAGIC );
  xassert( mstate->operating );
  int fd = mstate->fd;

  while( pmix_nbmsg_send_pending(mstate) ){
    // try to send everything
    // FIXME: maybe set some restriction on number of messages sended at once
    bool shutdown = false;
    uint32_t to_write = mstate->send_size - mstate->send_offs;
    int cnt = _write_buf(fd, mstate->send_current + mstate->send_offs, to_write, &shutdown);
    if( shutdown ){
      pmix_nbmsg_finalize(mstate);
      return;
    }
    if( cnt == 0 ){
      break;
    }
    mstate->send_offs += cnt;
  }
}
