#ifndef COMM_ENGINE_H
#define COMM_ENGINE_H

#include <poll.h>
#include "pmix_common.h"

// Socket management code

int pmix_comm_srvsock_create(char *path);

//--------------------------------8<--------------------------------//

static inline bool pmix_comm_fd_read_ready(int fd, bool *shutdown)
{
  struct pollfd pfd[1];
  int    rc;
  pfd[0].fd     = fd;
  pfd[0].events = POLLIN;
  rc = poll(pfd, 1, 10);
  if( rc < 0 ){
    *shutdown = true;
    return false;
  }
  bool ret = ((rc == 1) && (pfd[0].revents & POLLIN));
  if( !ret && (pfd[0].revents & ( POLLERR | POLLHUP | POLLNVAL))){
    *shutdown = true;
  }
  return ret;
}

static inline bool pmix_comm_fd_write_ready(int fd, bool *shutdown)
{
  struct pollfd pfd[1];
  int    rc;
  pfd[0].fd     = fd;
  pfd[0].events = POLLOUT;
  rc = poll(pfd, 1, 10);
  if( rc < 0 ){
    *shutdown = true;
    return false;
  }
  if( pfd[0].revents & ( POLLERR | POLLHUP | POLLNVAL) ){
    *shutdown = true;
  }
  return ((rc == 1) && (pfd[0].revents & POLLOUT));
}
//--------------------------------8<--------------------------------//

// Message management

typedef uint32_t (*msg_pay_size_cb_t)(void *msg);

typedef struct {
#ifndef NDEBUG
#       define PMIX_MSGSTATE_MAGIC 0xdeadbeef
  int  magic;
#endif
  // User supplied information
  int fd;
  int error;
  uint32_t hdr_size;
  msg_pay_size_cb_t pay_size_cb;
  bool operating;
  // receiver
  uint32_t rcvd_hdr_offs;
  void *rcvd_header;
  uint32_t rcvd_pay_size;
  uint32_t rcvd_pay_offs;
  void *rcvd_payload;
  uint32_t rcvd_padding;
  uint32_t rcvd_pad_recvd;
  // sender
  void *send_current;
  uint32_t send_offs;
  uint32_t send_size;
  List send_queue;
} pmix_msgengine_t;

inline static void pmix_nbmsg_set_padding(pmix_msgengine_t *mstate, uint32_t padsize){
  xassert(mstate->magic == PMIX_MSGSTATE_MAGIC );
  mstate->rcvd_padding = padsize;
}

inline static bool pmix_nbmsg_rcvd_ready(pmix_msgengine_t *mstate){
  xassert(mstate->magic == PMIX_MSGSTATE_MAGIC );
  return (mstate->rcvd_hdr_offs == mstate->hdr_size) && (mstate->rcvd_pay_size == mstate->rcvd_pay_offs);
}

inline static bool pmix_nbmsg_finalized(pmix_msgengine_t *mstate){
  xassert(mstate->magic == PMIX_MSGSTATE_MAGIC );
  return !(mstate->operating);
}

inline static int pmix_nbmsg_error(pmix_msgengine_t *mstate){
  xassert(mstate->magic == PMIX_MSGSTATE_MAGIC );
  return mstate->error;
}

void pmix_nbmsg_init(pmix_msgengine_t *mstate, int fd, uint32_t _hsize, msg_pay_size_cb_t cb);
void pmix_nbmsg_finalize(pmix_msgengine_t *mstate, int error);

// Receiver
int pmix_nbmsg_first_header(int fd, void *buf, uint32_t *_offs, uint32_t len);
void pmix_nbmsg_add_hdr(pmix_msgengine_t *mstate, void *buf);
void pmix_nbmsg_rcvd(pmix_msgengine_t *mstate);
void *pmix_nbmsg_rcvd_extract(pmix_msgengine_t *mstate, void *header);
// Transmitter
void pmix_nbmsg_send_enqueue(pmix_msgengine_t *mstate,void *msg);
void pmix_nbmsg_send_progress(pmix_msgengine_t *mstate);
bool pmix_nbmsg_send_pending(pmix_msgengine_t *mstate);


#endif // COMM_ENGINE_H
