#ifndef PMIX_DB_H
#define PMIX_DB_H

#include "pmix_common.h"
#include "pmix_info.h"
#include "pmix_debug.h"

typedef struct {
#ifndef NDEBUG
#       define PMIX_MSGSTATE_MAGIC 0xdeadbeef
  int  magic;
#endif
  int *upd;
  void **blobs;
  int *blob_sizes;
} pmix_db_t;

extern pmix_db_t pmix_db;

static inline void pmix_db_init()
{
  int i;
  pmix_db.magic = PMIX_MSGSTATE_MAGIC;
  uint32_t tasks = pmix_info_tasks();
  pmix_db.upd = xmalloc(sizeof(int)*tasks);
  pmix_db.blobs = xmalloc( sizeof(int*) * tasks );
  pmix_db.blob_sizes = xmalloc( sizeof(int) * tasks );
  for(i=0;i<tasks;i++){
    pmix_db.upd[i] = 0;
  }
}

static inline void pmix_db_update_init()
{
  // Mark everybody as non-reported
  int i;
  for(i=0;i < pmix_info_tasks();i++){
    pmix_db.upd[i] = 0;
  }
}

static inline void pmix_db_update_verify()
{
  int i;
  // Everybody has to report
  for(i=0;i < pmix_info_tasks(); i++){
    if( !pmix_db.upd[i] ){
      PMIX_ERROR("Task %d have not reported!", i);
      xassert( pmix_db.upd[i] );
    }
  }
}

static inline void pmix_db_add_blob(int taskid, void *blob, int size)
{
  xassert(pmix_db.magic == PMIX_MSGSTATE_MAGIC);
  pmix_db.upd[taskid] = taskid;
  pmix_db.blobs[taskid] = blob;
  pmix_db.blob_sizes[taskid] = size;
  pmix_db.upd[taskid] = 1;
}

static inline int pmix_db_get_blob(int taskid, void **blob)
{
  xassert(pmix_db.magic == PMIX_MSGSTATE_MAGIC);
  pmix_db.upd[taskid] = taskid;
  *blob = pmix_db.blobs[taskid];
  return pmix_db.blob_sizes[taskid];
}



#endif // PMIX_DB_H
