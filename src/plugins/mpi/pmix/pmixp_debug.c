volatile int pmixp_debug_prof_cnt = 0;
char *pmixp_debug_perf_descr[PMIXP_DEBUG_PROF_MAX];
double pmixp_debug_perf_ts[PMIXP_DEBUG_PROF_MAX];

extern volatile int pmixp_debug_prof_buf_offs = 0;
extern char pmixp_debug_prof_buf[128*PMIXP_DEBUG_PROF_MAX];
