#ifndef COMM_ENGINE_H
#define COMM_ENGINE_H

#include <poll.h>
#include "pmix_common.h"

// Socket management code

int pmix_comm_srvsock_create(char *path);

static inline bool pmix_comm_fd_is_ready(int fd)
{
  struct pollfd pfd[1];
  int    rc;
  pfd[0].fd     = fd;
  pfd[0].events = POLLIN;
  rc = poll(pfd, 1, 10);
  return ((rc == 1) && (pfd[0].revents & POLLIN));
}

// Message management

typedef uint32_t (*msg_pay_size_cb_t)(void *msg);

typedef enum { PMIX_MSG_HDR, PMIX_MSG_BODY, PMIX_MSG_READY, PMIX_MSG_FAIL } engine_states_t;

typedef struct {
#ifndef NDEBUG
#       define PMIX_MSGSTATE_MAGIC 0xdeadbeef
  int  magic;
#endif
  // User supplied information
  uint32_t hdr_size;
  msg_pay_size_cb_t pay_size_cb;
  // dynamically adjusted fields
  engine_states_t state;
  uint32_t hdr_offs;
  void *header;
  uint32_t pay_size;
  uint32_t pay_offs;
  void *payload;
} pmix_msgengine_t;

int pmix_msgengine_first_header(int fd, void *buf, uint32_t *_offs, uint32_t len);
void pmix_msgengine_init(pmix_msgengine_t *mstate, uint32_t _hsize, msg_pay_size_cb_t cb);
void pmix_msgengine_add_hdr(pmix_msgengine_t *mstate, void *buf);
void pmix_msgengine_rcvd(int fd, pmix_msgengine_t *mstate);

inline static bool pmix_msgengine_ready(pmix_msgengine_t *mstate){
  return (mstate->state == PMIX_MSG_READY);
}

inline static bool pmix_msgengine_failed(pmix_msgengine_t *mstate){
  return (mstate->state == PMIX_MSG_FAIL);
}

inline static void *pmix_msgengine_extract(pmix_msgengine_t *mstate, void *header, uint32_t *paysize)
{
  void *ptr = mstate->payload;
  *paysize = mstate->pay_size;

  xassert( mstate->state == PMIX_MSG_READY );
  if( mstate->state != PMIX_MSG_READY ){
    return NULL;
  }
  // Drop message state to receive new one
  mstate->pay_size = mstate->hdr_offs = mstate->pay_offs = 0;
  mstate->state = PMIX_MSG_HDR;
  mstate->payload = NULL;
  memcpy(header, mstate->header, (size_t)mstate->hdr_size);
  return ptr;
}



#endif // COMM_ENGINE_H
