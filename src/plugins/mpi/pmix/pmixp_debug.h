/*****************************************************************************\
 **  pmix_debug.h - PMIx debug primitives
 *****************************************************************************
 *  Copyright (C) 2014-2015 Artem Polyakov. All rights reserved.
 *  Copyright (C) 2015      Mellanox Technologies. All rights reserved.
 *  Written by Artem Polyakov <artpol84@gmail.com, artemp@mellanox.com>.
 *
 *  This file is part of SLURM, a resource management program.
 *  For details, see <https://slurm.schedmd.com/>.
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
#ifndef PMIXP_DEBUG_H
#define PMIXP_DEBUG_H

#include "pmixp_common.h"
#include "pmixp_info.h"

#define MAX_MSG_SIZE 1024

#define PMIXP_DEBUG(format, args...) {				\
	char file[] = __FILE__;					\
	char *file_base = strrchr(file, '/');			\
	if (file_base == NULL) {				\
		file_base = file;				\
	}							\
	debug("%s [%d] %s:%d [%s] mpi/pmix: " format "",	\
		pmixp_info_hostname(), pmixp_info_nodeid(),	\
		file_base, __LINE__, __func__, ## args);	\
}

#define PMIXP_ERROR_STD(format, args...) {			\
	char file[] = __FILE__;					\
	char *file_base = strrchr(file, '/');			\
	if (file_base == NULL) {				\
	file_base = file;					\
	}							\
	error("%s [%d] %s:%d [%s] mpi/pmix: ERROR: " format ": %s (%d)",	\
		pmixp_info_hostname(), pmixp_info_nodeid(),	\
		file_base, __LINE__, __func__,			\
		## args, strerror(errno), errno);		\
}

#define PMIXP_ERROR(format, args...) {				\
	char file[] = __FILE__;					\
	char *file_base = strrchr(file, '/');			\
	if (file_base == NULL) {				\
		file_base = file;				\
	}							\
	error("%s [%d] %s:%d [%s] mpi/pmix: ERROR: " format,	\
		pmixp_info_hostname(), pmixp_info_nodeid(),	\
		file_base, __LINE__, __func__, ## args);	\
}

#define PMIXP_ABORT(format, args...) {				\
	PMIXP_ERROR(format, ##args);				\
	error("%s [%d] %s:%d [%s] mpi/pmix: ERROR: " format,	\
		pmixp_info_hostname(), pmixp_info_nodeid(),	\
		file_base, __LINE__, __func__, ## args);	\
		slurm_kill_job_step(pmixp_info_jobid(),		\
		pmixp_info_stepid(), SIGKILL);			\
}

#define PMIXP_ERROR_NO(err, format, args...) {			\
	char file[] = __FILE__;					\
	char *file_base = strrchr(file, '/');			\
	if (file_base == NULL) {				\
		file_base = file;				\
	}							\
	error("%s [%d] %s:%d [%s] mpi/pmix: ERROR: " format ": %s (%d)", \
		pmixp_info_hostname(), pmixp_info_nodeid(),	\
		file_base, __LINE__, __func__,			\
		## args, strerror(err), err);			\
}

#ifdef NDEBUG
#define pmixp_debug_hang(x)
#else
static inline void _pmixp_debug_hang(int delay)
{
	while (delay) {
		sleep(1);
	}
}

#define pmixp_debug_hang(x) _pmixp_debug_hang(x)

#endif

/* The profile framework.
 * It is by no means production-ready and shouldn't be used
 * for that.
 * It's solely purpose is a lightweight non-universal debugging
 * tool for PMIx plugin performance optimization
 */
#ifdef PMIXP_PROFILE
/* We don't want somebody to suddenly set -DPMIXP_DEBUG_PROFILE=1
 * in GCC command line.
 * User has to be 100% sure and go here and set it to 1 here in the
 * source code
 */
#undef PMIXP_PROFILE
#endif

/* Determine if you want to have profile enabled */
#define PMIXP_PROFILE 1

#if PMIXP_PROFILE

#define PMIXP_PROF_MAX (128*1024)

extern volatile int pmixp_prof_cnt;
extern char *pmixp_prof_descr[PMIXP_PROF_MAX];
extern double pmixp_prof_ts[PMIXP_PROF_MAX];

extern volatile int pmixp_prof_buf_offs;
extern char pmixp_prof_buf[128*PMIXP_PROF_MAX];


#define PMIXP_PROF_GET_TS1() ({			\
	struct timeval tv;			\
	double ret = 0;				\
	gettimeofday(&tv, NULL);		\
	ret = tv.tv_sec + 1E-6*tv.tv_usec;	\
	ret;					\
})

#define PMIXP_PROF_GET_TS2() ({			\
	struct timespec ts;			\
	double ret = 0;				\
	clock_gettime(CLOCK_MONOTONIC, &ts);	\
	ret = ts.tv_sec + 1E-9*ts.tv_nsec;	\
	ret;					\
})

#define PMIXP_PROF_GET_TS() PMIXP_PROF_GET_TS1()

#ifdef __GNUC__

#define PMIXP_ATOMIC_ADD(ptr, x)		\
	__sync_fetch_and_add (ptr, x)

#else

#define PMIXP_ATOMIC_ADD(ptr, x) ({        \
	(*ptr)+=x;                     \
	((*ptr)-x);                   \
})

#endif

#define PMIXP_PROF_ADD_TS(ts, x) {				\
	int idx = PMIXP_ATOMIC_ADD(&pmixp_prof_cnt, 1);	\
	pmixp_prof_descr[idx] = x;					\
	pmixp_prof_ts[idx] = ts;					\
}

#define PMIXP_PROF_ADD(x) PMIXP_PROF_ADD_TS(PMIXP_PROF_GET_TS(), x)

#define PMIXP_PROF_ADD_FMT_TS(ts, format, args...) {			\
	char tmp[128];							\
	int my_offs = 0;						\
	int len = snprintf(tmp, 128, format, ##args);			\
	my_offs = PMIXP_ATOMIC_ADD(&pmixp_prof_buf_offs, len+1);	\
	memcpy(pmixp_prof_buf + my_offs, tmp, len+1);			\
	PMIXP_PROF_ADD_TS(ts, pmixp_prof_buf + my_offs);			\
}

#define PMIXP_PROF_ADD_FMT(format, args...) \
	PMIXP_PROF_ADD_FMT_TS(PMIXP_PROF_GET_TS(), format, ##args)


static inline void _pmixp_prof_output(char *fname) {
	FILE *fp = fopen(fname,"w");
	int i;

	if (!fp) {
		PMIXP_ERROR("Unable to open %s",fname);
		return;
	}

	double base = pmixp_prof_ts[0], prev = pmixp_prof_ts[0];
	fprintf(fp, "<descr> <timestamp> <rel-first> <rel-prev>\n");
	for(i=0; i< pmixp_prof_cnt; i++) {
		fprintf(fp, "%s %.9lf %.9lf %.9lf\n", pmixp_prof_descr[i],
			pmixp_prof_ts[i], (pmixp_prof_ts[i] - prev),
			pmixp_prof_ts[i] - base);
		prev = pmixp_prof_ts[i];
	}
	fclose(fp);
}

#define PMIXP_PROF_OUTPUT(fname)		\
	_pmixp_prof_output(fname)


#else

#define PMIXP_PROF_ADD_TS(ts, x)
#define PMIXP_PROF_ADD(x)
#define PMIXP_PROF_ADD_FMT_TS(ts, format, args...)
#define PMIXP_PROF_ADD_FMT(format, args...)
#define PMIXP_PROF_OUTPUT(fname)

#endif

#endif /* PMIXP_DEBUG_H */
