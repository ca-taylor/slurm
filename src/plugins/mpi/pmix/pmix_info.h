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

#include "pmix_common.h"

/*
 *
 *  Environment information structure and access API
 *
 */

typedef struct {
#ifndef NDEBUG
#       define PMIX_INFO_MAGIC 0xdeadbeef
	int  magic;
#endif
	uint32_t jobid;       /* Current SLURM job id                         */
	uint32_t stepid;      /* Current step id (or NO_VAL)                  */
	uint32_t nnodes;      /* number of nodes in current step              */
	uint32_t nnodes_job;  /* number of nodes in current job               */
	uint32_t ntasks;      /* total number of tasks in current step        */
	uint32_t ntasks_job;  /* total possible number of tasks in job		*/
	uint32_t ncpus_job;   /* total possible number of cpus in job		    */
	uint16_t *task_cnts;  /* Number of tasks on each node in this step    */
	int node_id;          /* relative position of this node in this step  */
	int node_id_job;      /* relative position of this node in SLURM job  */
	uint32_t node_tasks;  /* number of tasks on *this* node               */
	uint32_t *gtids;      /* global ids of tasks located on *this* node   */
	bool	direct_modex;	/* direct modex mode enabled/disabled           */
	uint32_t *task_map;	/* i'th task is located on task_map[i] node     */
} pmix_jobinfo_t;

extern pmix_jobinfo_t _pmix_job_info;

// Client contact information
void pmix_info_cli_contacts_set(char *path, int fd);
const char *pmix_info_cli_addr();
int pmix_info_cli_fd();

// slurmd contact information
void pmix_info_server_contacts_set(char *path, int fd);
const char *pmix_info_srv_addr();
int pmix_info_srv_fd();

// Dealing with hostnames
int pmix_info_resources_set(char ***env);
char *pmix_info_nth_child_name(int idx);
char *pmix_info_nth_host_name(int n);

// Dealing with I/O
void pmix_info_io_set(eio_handle_t *h);
eio_handle_t *pmix_info_io();


// Agent location
static inline int pmix_info_is_srun()
{
	xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
	return (_pmix_job_info.node_id < 0 );
}

static inline int pmix_info_is_stepd()
{
	xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
	return ( _pmix_job_info.node_id >= 0 );
}

// Job information
int pmix_info_job_set_stepd(const stepd_step_rec_t *job, char ***env);
void pmix_info_job_set_srun(const mpi_plugin_client_info_t *job, char ***env);

inline static uint32_t pmix_info_jobid(){
	xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
	return _pmix_job_info.jobid;
}

inline static uint32_t pmix_info_stepid(){
	xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
	return _pmix_job_info.stepid;
}

inline static uint32_t pmix_info_nodeid(){
	// xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
	// This routine is called from PMIX_DEBUG/ERROR and
	// this CAN happen before initialization. Relax demand to have
	// _pmix_job_info.magic == PMIX_INFO_MAGIC
	return _pmix_job_info.node_id;
}

inline static uint32_t pmix_info_nodes(){
	xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
	return _pmix_job_info.nnodes;
}

inline static uint32_t pmix_info_nodes_uni(){
	xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
	return _pmix_job_info.nnodes_job;
}

inline static uint32_t pmix_info_tasks(){
	xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
	return _pmix_job_info.ntasks;
}

inline static uint32_t pmix_info_tasks_uni(){
	xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
	return _pmix_job_info.ntasks_job;
}

inline static uint32_t pmix_info_cpus(){
	xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
	return _pmix_job_info.ncpus_job;
}

inline static uint32_t pmix_info_node_taskcnt(uint32_t i){
	xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
	xassert( i < _pmix_job_info.nnodes);
	return _pmix_job_info.task_cnts[i];
}


inline static uint32_t pmix_info_ltasks(){
	xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
	return _pmix_job_info.node_tasks;
}

inline static uint32_t pmix_info_task_id(uint32_t i){
	xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
	xassert( i < _pmix_job_info.node_tasks );
	return _pmix_job_info.gtids[i];
}

/*
 * Since tasks array in SLURM job structure is uint16_t
 * task local id can't be grater than 2^16. So we can
 * safely return int here. We need (-1) for the not-found case
 */
inline static int pmix_info_lid2gid(uint32_t gid){
	int i;
	xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
	xassert( gid < _pmix_job_info.ntasks );

	for(i=0; i<_pmix_job_info.node_tasks; i++){
		if( _pmix_job_info.gtids[i] == gid )
			return i;
	}
	return -1;
}

inline static bool pmix_info_dmdx(){
	xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
	return _pmix_job_info.direct_modex;
}

inline static uint32_t pmix_info_task_node(uint32_t gid)
{
	xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
	xassert( gid < _pmix_job_info.ntasks );

	return _pmix_job_info.task_map[gid];
}

/*
 *
 *  Collective information structure and access API
 *
 */

typedef enum {PMIX_PARENT_NONE, PMIX_PARENT_ROOT, PMIX_PARENT_SRUN, PMIX_PARENT_STEPD } parent_type_t;



typedef struct {
	parent_type_t _pmix_parent_type;
	char *_pmix_this_host;
	char *_pmix_nodes_list;
	int _pmix_child_num;
	int *_pmix_child_list;
	char *_pmix_parent_host;
	slurm_addr_t *_pmix_parent_addr;
} pmix_collective_info_t;



extern parent_type_t _pmix_parent_type;
extern char *_pmix_this_host;
extern char *_pmix_step_nodes_list;
extern int _pmix_child_num;
extern int *_pmix_child_list;
extern char *_pmix_parent_host;
extern slurm_addr_t *_pmix_parent_addr;

int pmix_info_coll_tree_set(int *childs, int child_cnt);
int  pmix_info_parent_set_root();
int  pmix_info_parent_set_srun(char *phost, uint16_t port);
int pmix_info_parent_set_stepd(char *phost);

inline static parent_type_t pmix_info_parent_type(){
	xassert( _pmix_parent_type != PMIX_PARENT_NONE );
	return _pmix_parent_type;
}

inline static char *pmix_info_step_hosts(){
	// xassert( _pmix_parent_type != PMIX_PARENT_NONE );
	// This information is essential in determinig of the parent type
	// The only thign that's important - did we initialize _pmix_nodes_list itself?
	xassert( _pmix_step_nodes_list != NULL );
	return _pmix_step_nodes_list;
}

inline static char *pmix_info_this_host(){
	// xassert( _pmix_parent_type != PMIX_PARENT_NONE );
	// This routine is called from PMIX_DEBUG/ERROR and
	// this CAN happen before initialization. Relax demand to have
	// _pmix_parent_type != PMIX_PARENT_NONE
	if( _pmix_this_host == NULL ){
		return "unknown";
	} else {
		return _pmix_this_host;
	}
}

inline static char *pmix_info_parent_host()
{
	// Check for initialization
	xassert( _pmix_parent_type != PMIX_PARENT_NONE );
	return _pmix_parent_host;
}

inline static slurm_addr_t *pmix_info_parent_addr()
{
	// Check for initialization
	xassert( _pmix_parent_type != PMIX_PARENT_NONE );
	return _pmix_parent_addr;
}

inline static int pmix_info_childs()
{
	// Check for initialization
	xassert( _pmix_parent_type != PMIX_PARENT_NONE );
	return _pmix_child_num;
}

inline static int pmix_info_nth_child(int n)
{
	// Check for initialization
	xassert( _pmix_parent_type != PMIX_PARENT_NONE );
	xassert( n < _pmix_child_num );
	return _pmix_child_list[n];
}

inline static int pmix_info_is_child_no(int id)
{
	// Check for initialization
	xassert( _pmix_parent_type != PMIX_PARENT_NONE );
	int i;
	for(i=0;i<_pmix_child_num; i++){
		if( id == _pmix_child_list[i] ){
			return i;
		}
	}
	return -1;
}

#endif // INFO_H
