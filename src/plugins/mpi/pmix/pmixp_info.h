/*****************************************************************************\
 **  pmix_info.h - PMIx various environment information
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


#ifndef PMIX_INFO_H
#define PMIX_INFO_H

#include "pmixp_common.h"

/*
 *  Slurm job and job-step information
 */

typedef struct {
#ifndef NDEBUG
#       define PMIX_INFO_MAGIC 0xCAFEFACE
	int  magic;
#endif
	char nspace[PMIX_MAX_NSLEN];
	uint32_t jobid;       /* Current SLURM job id                         */
	uint32_t stepid;      /* Current step id (or NO_VAL)                  */
	uint32_t nnodes;      /* number of nodes in current step              */
	uint32_t nnodes_job;  /* number of nodes in current job               */
	uint32_t ntasks;      /* total number of tasks in current step        */
	uint32_t ntasks_job;  /* total possible number of tasks in job	      */
	uint32_t ncpus_job;   /* total possible number of cpus in job	      */
	uint32_t *task_cnts;  /* Number of tasks on each node in this step    */
	int node_id;          /* relative position of this node in this step  */
	int node_id_job;      /* relative position of this node in SLURM job  */
	hostlist_t job_hl;
	hostlist_t step_hl;
	char *hostname;
	uint32_t node_tasks;  /* number of tasks on *this* node               */
	uint32_t *gtids;      /* global ids of tasks located on *this* node   */
	char *task_map_packed;  /* string represents packed task mapping information */
} pmix_jobinfo_t;

extern pmix_jobinfo_t _pmixp_job_info;

// Client contact information
void pmixp_info_cli_contacts(int fd);
const char *pmix_info_cli_addr();
int pmix_info_cli_fd();

// slurmd contact information
void pmixp_info_srv_contacts(char *path, int fd);
const char *pmixp_info_srv_addr();
int pmixp_info_srv_fd();

// Dealing with hostnames
static inline char *pmixp_info_hostname(){
	return _pmixp_job_info.hostname;
}

int pmixp_info_resources_set(char ***env);

// Dealing with I/O
void pmixp_info_io_set(eio_handle_t *h);
eio_handle_t *pmixp_info_io();

// Job information
int pmixp_info_set(const stepd_step_rec_t *job, char ***env);

inline static uint32_t pmixp_info_jobid(){
	xassert(_pmixp_job_info.magic == PMIX_INFO_MAGIC );
	return _pmixp_job_info.jobid;
}

inline static uint32_t pmixp_info_stepid(){
	xassert(_pmixp_job_info.magic == PMIX_INFO_MAGIC );
	return _pmixp_job_info.stepid;
}

inline static char *pmixp_info_namespace(){
	xassert(_pmixp_job_info.magic == PMIX_INFO_MAGIC );
	return _pmixp_job_info.nspace;
}

inline static uint32_t pmixp_info_nodeid(){
	// This routine is called from PMIX_DEBUG/ERROR and
	// this CAN happen before initialization. Relax demand to have
	// _pmix_job_info.magic == PMIX_INFO_MAGIC

	// ! xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
	return _pmixp_job_info.node_id;
}

inline static uint32_t pmixp_info_nodes(){
	xassert(_pmixp_job_info.magic == PMIX_INFO_MAGIC );
	return _pmixp_job_info.nnodes;
}

inline static uint32_t pmixp_info_nodes_uni(){
	xassert(_pmixp_job_info.magic == PMIX_INFO_MAGIC );
	return _pmixp_job_info.nnodes_job;
}

inline static uint32_t pmixp_info_tasks(){
	xassert(_pmixp_job_info.magic == PMIX_INFO_MAGIC );
	return _pmixp_job_info.ntasks;
}

inline static uint32_t pmixp_info_tasks_node(uint32_t nodeid){
	xassert(_pmixp_job_info.magic == PMIX_INFO_MAGIC );
	xassert( nodeid < _pmixp_job_info.nnodes);
	return _pmixp_job_info.task_cnts[nodeid];
}

inline static uint32_t *pmixp_info_tasks_cnts(){
	xassert(_pmixp_job_info.magic == PMIX_INFO_MAGIC );
	return _pmixp_job_info.task_cnts;
}


inline static uint32_t pmixp_info_tasks_loc(){
	xassert(_pmixp_job_info.magic == PMIX_INFO_MAGIC );
	return _pmixp_job_info.node_tasks;
}


inline static uint32_t pmixp_info_tasks_uni(){
	xassert(_pmixp_job_info.magic == PMIX_INFO_MAGIC );
	return _pmixp_job_info.ntasks_job;
}

inline static uint32_t pmixp_info_cpus(){
	xassert(_pmixp_job_info.magic == PMIX_INFO_MAGIC );
	return _pmixp_job_info.ncpus_job;
}


inline static uint32_t pmixp_info_taskid(uint32_t localid){
	xassert(_pmixp_job_info.magic == PMIX_INFO_MAGIC );
	xassert( localid < _pmixp_job_info.node_tasks );
	return _pmixp_job_info.gtids[localid];
}

/*
 * Since tasks array in SLURM job structure is uint16_t
 * task local id can't be grater than 2^16. So we can
 * safely return int here. We need (-1) for the not-found case
 */
inline static int pmixp_info_taskid2localid(uint32_t taskid){
	int i;
	xassert(_pmixp_job_info.magic == PMIX_INFO_MAGIC );
	xassert( taskid < _pmixp_job_info.ntasks );

	for(i=0; i<_pmixp_job_info.node_tasks; i++){
		if( _pmixp_job_info.gtids[i] == taskid )
			return i;
	}
	return -1;
}

inline static char *pmixp_info_task_map(){
	return _pmixp_job_info.task_map_packed;
}

inline static hostlist_t pmixp_info_step_hostlist(){
	return _pmixp_job_info.step_hl;
}

#endif // INFO_H
