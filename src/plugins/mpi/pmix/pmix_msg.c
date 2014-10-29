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

static size_t _read_buf(int fd, void *buf, size_t count, bool *shutdown)
{
  ssize_t ret, offs = 0;

  *shutdown = false;

  if( !pmix_comm_fd_is_ready(fd) ){
    return 0;
  }

  while( count - offs > 0 ) {
    ret = read(fd, (char*)buf + offs, count - offs);
    if( ret > 0 ){
      offs += ret;
      continue;
    } else if( ret == 0 ){
      // end of file/message. return
      // FIXME: maybe we should'nt shutdown in this case?
      goto err_exit;
    }
    switch( errno ){
    case EINTR:
      continue;
    case EWOULDBLOCK:
      break;
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

int pmix_msgengine_first_header(int fd, void *buf, uint32_t *_offs, uint32_t len)
{
  bool shutdown;
  uint32_t offs = *_offs;
  ssize_t ret;
  // FIXME: is it ok to use this function for blocking receive?
  offs += _read_buf(fd, buf + offs, len, &shutdown);
  *_offs = offs;
  if( shutdown ){
    return SLURM_ERROR;
  }
  return 0;
}

void pmix_msgengine_init(pmix_msgengine_t *mstate, uint32_t _hsize, msg_pay_size_cb_t cb)
{
  mstate->magic = PMIX_MSGSTATE_MAGIC;
  mstate->hdr_size = _hsize;
  mstate->pay_size_cb = cb;
  mstate->header = xmalloc(_hsize);
  mstate->pay_size = 0;
  mstate->payload = NULL;
  mstate->hdr_offs = mstate->pay_offs = 0;
  mstate->state = PMIX_MSG_HDR;
}

inline static void _swithch_to_body(pmix_msgengine_t *mstate)
{
  xassert(mstate->hdr_size == mstate->hdr_offs);
  xassert(mstate->state == PMIX_MSG_HDR);
  mstate->state = PMIX_MSG_BODY;
  mstate->pay_offs = 0;
  mstate->pay_size = mstate->pay_size_cb(mstate->header);
  mstate->payload = xmalloc(mstate->pay_size);
}

void pmix_msgengine_add_hdr(pmix_msgengine_t *mstate, void *buf)
{
  xassert(mstate->magic == PMIX_MSGSTATE_MAGIC);
  xassert(mstate->state == PMIX_MSG_HDR);
  if( mstate->state != PMIX_MSG_HDR ){
    return;
  }
  memcpy(mstate->header, buf, mstate->hdr_size);
  mstate->hdr_offs = mstate->hdr_size;
  _swithch_to_body(mstate);
}

void pmix_msgengine_rcvd(int fd, pmix_msgengine_t *mstate)
{
  size_t size, to_recv;
  bool shutdown;

  xassert(mstate->magic == PMIX_MSGSTATE_MAGIC);
  xassert( mstate->state != PMIX_MSG_FAIL );

  if( mstate->state == PMIX_MSG_FAIL ){
    // nothing to do
    return;
  }

  switch( mstate->state ){
  case PMIX_MSG_HDR:
    xassert( mstate->hdr_offs < mstate->hdr_size );
    size = mstate->hdr_size;
    to_recv = size - mstate->hdr_offs;
    mstate->hdr_offs += _read_buf(fd, mstate->header + mstate->hdr_offs, to_recv, &shutdown);
    if( shutdown ){
      mstate->state = PMIX_MSG_FAIL;
      return;
    }
    if( mstate->hdr_offs < size ){
      // normal return. receive another portion later
      return;
    }
    // if we are here then header is received and we can adjust buffer
    _swithch_to_body(mstate);
    // go ahared with body receive
  case PMIX_MSG_BODY:
    size = mstate->pay_size;
    to_recv = size - mstate->pay_offs;
    mstate->pay_offs += _read_buf(fd, mstate->payload + mstate->pay_offs, to_recv, &shutdown);
    if( shutdown ){
      mstate->state = PMIX_MSG_FAIL;
      return;
    }
    if( mstate->pay_offs  < size ){
      // normal return. receive another portion later
      return;
    }
    // we are ready with message
    mstate->state = PMIX_MSG_READY;
  case PMIX_MSG_READY:
    // This should not happen
    PMIX_DEBUG("Message is ready for processing!");
  }
}



//-------------------------------------------8<--------------------------------//
// TODO: remove as we don't need it in pmix server
/*
int connect_to_server_usock(char *path)
{
  static struct sockaddr_un sa;

  if( strlen(path) >= sizeof(sa.sun_path) ){
    PMIX_ERROR("The size of UNIX sockety path is greater than possible: "
               "%d, max %d", strlen(path), sizeof(sa.sun_path)-1);
    return -1;
  }

  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if( fd < 0 ){
    PMIX_ERROR("Cannot create socket")
  }

  memset(&sa, 0, sizeof(sa));
  sa.sun_family = AF_UNIX;
  strcpy(sa.sun_path, path);

  if( ret = connect(sd,(struct sockaddr*) &sa, SUN_LEN(&sa) ) ){
  }
  return sd;
}
*/
//-------------------------------------------8<--------------------------------//
