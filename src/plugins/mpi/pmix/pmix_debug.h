#ifndef PMIX_DEBUG_H
#define PMIX_DEBUG_H

#include "pmix_common.h"
#include "pmix_info.h"

#define MAX_MSG_SIZE 1024

#define PMIX_DEBUG(format, args...) {           \
  char file[] = __FILE__;                       \
  char *file_base = strrchr(file, '/');         \
  if( file_base == NULL ){                      \
    file_base = file;                           \
  }                                             \
  debug("%s [%d] %s:%d [%s] mpi/pmix: " format "",    \
         pmix_info_this_host(), pmix_info_nodeid(),    \
         file_base, __LINE__, __FUNCTION__,      \
        ## args);                               \
}

#define PMIX_ERROR(format, args...) {                 \
  char file[] = __FILE__;                             \
  char *file_base = strrchr(file, '/');               \
  if( file_base == NULL ){                            \
    file_base = file;                                 \
  }                                                   \
  error("%s:%d [%s] mpi/pmix: ERROR: " format ": %s (%d)", \
        file_base, __LINE__, __FUNCTION__,            \
        ## args, strerror(errno), errno);             \
}

#ifdef NDEBUG
#define pmix_debug_hang(x)
#else
inline static void
pmix_debug_hang(int delay){
  while( delay ){
    sleep(1);
  }
}
#endif
#endif // PMIX_DEBUG_H
