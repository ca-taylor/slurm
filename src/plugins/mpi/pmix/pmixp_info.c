/*****************************************************************************\
 **  pmix_info.c - PMIx various environment information
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

#include <string.h>
#include "pmixp_common.h"
#include "pmixp_debug.h"
#include "pmixp_info.h"

// Client communication
static char *_cli_addr = NULL;
static int _cli_fd = -1;

// Server communication
static char *_server_addr = NULL;
static int _server_fd = -1;

pmix_jobinfo_t _pmix_job_info  = { 0 };

// Collective tree description
char *_pmix_this_host = NULL;
char *_pmix_job_nodes_list = NULL;
char *_pmix_step_nodes_list = NULL;
int _pmix_child_num = -1;
int *_pmix_child_list = NULL;
parent_type_t _pmix_parent_type = PMIX_PARENT_NONE;
char *_pmix_parent_host = NULL;
slurm_addr_t *_pmix_parent_addr = NULL;
uint16_t _pmix_parent_port = -1;


// Client contact information
void pmix_info_cli_contacts_set(char *path, int fd)
{
	int size = strlen(path);
	_cli_addr = xmalloc(size + 1);
	strcpy(_cli_addr,path);
	_cli_fd = fd;
}

const char *pmix_info_cli_addr()
{
	// Check that client address was initialized
	xassert( _cli_addr != NULL );
	return _cli_addr;
}


int pmix_info_cli_fd()
{
	// Check that client fd was created
	xassert( _cli_fd >= 0  );
	return _cli_fd;
}

// stepd global contact information
void pmix_info_server_contacts_set(char *path, int fd)
{
	if( path != NULL ){
		int size = strlen(path);
		_server_addr = xmalloc(size + 1);
		strcpy(_server_addr,path);
	} else {
		_server_addr = NULL;
	}
	_server_fd = fd;
}

const char *pmix_info_srv_addr()
{
	// Check that Server address was initialized
	xassert( _server_addr != NULL );
	return _server_addr;
}

int pmix_info_srv_fd()
{
	// Check that Server fd was created
	xassert( _server_fd >= 0  );
	return _server_fd;
}

// Job information
void pmix_info_job_set_srun(const mpi_plugin_client_info_t *job, char ***env)
{
	int i;

	memset(&_pmix_job_info, 0, sizeof(_pmix_job_info));
#ifndef NDEBUG
	_pmix_job_info.magic = PMIX_INFO_MAGIC;
#endif

	_pmix_step_nodes_list = xstrdup(job->step_layout->node_list);
	// This node info
	_pmix_job_info.jobid      = job->jobid;
	_pmix_job_info.stepid     = job->stepid;
	_pmix_job_info.node_id    = -1; /* srun sign */
	_pmix_job_info.node_tasks =  0; /* srun doesn't manage any tasks */
	_pmix_job_info.ntasks     = job->step_layout->task_cnt;
	_pmix_job_info.nnodes     = job->step_layout->node_cnt;
	_pmix_job_info.task_cnts  = xmalloc( sizeof(*_pmix_job_info.task_cnts) * _pmix_job_info.nnodes);
	for(i = 0; i < _pmix_job_info.nnodes; i++){
		_pmix_job_info.task_cnts[i] = job->step_layout->tasks[i];
	}
	// Export task mapping information
	char *mapping = pack_process_mapping(_pmix_job_info.nnodes, _pmix_job_info.ntasks, _pmix_job_info.task_cnts, job->step_layout->tids);
	setenvf(env, PMIX_SLURM_MAPPING_ENV, "%s", mapping);
	xfree(mapping);
}

int pmix_info_job_set_stepd(const stepd_step_rec_t *job, char ***env)
{
	int i, rc;
#ifndef NDEBUG
	_pmix_job_info.magic = PMIX_INFO_MAGIC;
#endif

	// This node info
	_pmix_job_info.jobid      = job->jobid;
	_pmix_job_info.stepid     = job->stepid;
	_pmix_job_info.node_id    = job->nodeid;
	_pmix_job_info.node_tasks = job->node_tasks;

	// Global info
	_pmix_job_info.ntasks     = job->ntasks;
	_pmix_job_info.nnodes     = job->nnodes;
	_pmix_job_info.task_cnts  = xmalloc( sizeof(*_pmix_job_info.task_cnts) * _pmix_job_info.nnodes);
	for(i = 0; i < _pmix_job_info.nnodes; i++){
		_pmix_job_info.task_cnts[i] = job->task_cnts[i];
	}

	_pmix_job_info.gtids = xmalloc(_pmix_job_info.node_tasks * sizeof(uint32_t));
	for (i = 0; i < job->node_tasks; i ++) {
		_pmix_job_info.gtids[i] = job->task[i]->gtid;
	}

	// Setup hostnames and job-wide info
	if( (rc = pmix_info_resources_set(env)) ){
		return rc;
	}

	return SLURM_SUCCESS;
}

// Data related to Collective tree

int pmix_info_coll_tree_set(int *childs, int child_cnt)
{
	xassert( child_cnt >= 0 );
	xassert( childs != NULL );
	_pmix_child_num = child_cnt;
	_pmix_child_list = childs;
	return SLURM_SUCCESS;
}

int  pmix_info_parent_set_root(int child_num)
{
	_pmix_parent_type = PMIX_PARENT_ROOT;

	return SLURM_SUCCESS;
}

int  pmix_info_parent_set_srun(char *phost, uint16_t port)
{
	_pmix_parent_type = PMIX_PARENT_SRUN;
	_pmix_parent_addr = xmalloc(sizeof(slurm_addr_t));
	slurm_set_addr(_pmix_parent_addr, port, phost);
	return SLURM_SUCCESS;
}

int pmix_info_parent_set_stepd(char *phost)
{
	_pmix_parent_type = PMIX_PARENT_STEPD;
	_pmix_parent_host = phost;
	return SLURM_SUCCESS;
}

static eio_handle_t *_io_handle = NULL;

void pmix_info_io_set(eio_handle_t *h)
{
	_io_handle = h;
}

eio_handle_t *pmix_info_io()
{
	xassert( _io_handle != NULL );
	return _io_handle;
}

/*
 * Job and step nodes/tasks count and hostname extraction routines
 */

/*
 * Derived from src/srun/libsrun/opt.c
 * _get_task_count()
 *
 * FIXME: original _get_task_count has some additinal ckeck
 * for opt.ntasks_per_node & opt.cpus_set
 * Should we care here?
 */
static int _get_task_count(char ***env, uint32_t *tasks, uint32_t *cpus)
{
	char *cpus_per_node = NULL, *cpus_per_task_env = NULL, *end_ptr = NULL;
	int cpus_per_task = 1, cpu_count, node_count, task_count;
	int total_tasks = 0, total_cpus = 0;

	cpus_per_node = getenvp(*env, PMIX_CPUS_PER_NODE_ENV);
	if( cpus_per_node == NULL ){
		PMIX_ERROR_NO(0,"Cannot find %s environment variable", PMIX_CPUS_PER_NODE_ENV);
		return SLURM_ERROR;
	}
	cpus_per_task_env = getenvp(*env, PMIX_CPUS_PER_TASK);
	if( cpus_per_task_env != NULL ){
		cpus_per_task = strtol(cpus_per_task_env, &end_ptr, 10);
	}

	cpu_count = strtol(cpus_per_node, &end_ptr, 10);
	task_count = cpu_count / cpus_per_task;
	while (1) {
		if ((end_ptr[0] == '(') && (end_ptr[1] == 'x')) {
			end_ptr += 2;
			node_count = strtol(end_ptr, &end_ptr, 10);
			task_count *= node_count;
			total_tasks += task_count;
			cpu_count *= node_count;
			total_cpus += cpu_count;
			if (end_ptr[0] == ')')
				end_ptr++;
		} else if ((end_ptr[0] == ',') || (end_ptr[0] == 0))
			total_tasks += task_count;
		else {
			PMIX_ERROR_NO(0,"Invalid value for environment variable %s (%s)",
						  PMIX_CPUS_PER_NODE_ENV, cpus_per_node);
			return SLURM_ERROR;
		}
		if (end_ptr[0] == ',')
			end_ptr++;
		if (end_ptr[0] == 0)
			break;
	}
	*tasks = total_tasks;
	*cpus = total_cpus;
	return 0;
}



int pmix_info_resources_set(char ***env)
{
	char *p = NULL;
	hostlist_t hl;

	// Initialize all memory pointers that would be allocated to NULL
	// So in case of error exit we will know what to xfree
	_pmix_job_nodes_list = NULL;
	_pmix_step_nodes_list = NULL;
	_pmix_this_host = NULL;
	_pmix_job_info.task_map = NULL;


	// Save step host list
	p = getenvp(*env, PMIX_STEP_NODES_ENV);
	if (!p) {
		PMIX_ERROR_NO(ENOENT, "Environment variable %s not found", PMIX_STEP_NODES_ENV);
		goto err_exit;
	}
	_pmix_step_nodes_list = xstrdup(p);

	// Extract our node name
	hl = hostlist_create( _pmix_step_nodes_list );
	p = hostlist_nth(hl, _pmix_job_info.node_id);
	hostlist_destroy(hl);
	_pmix_this_host = xstrdup(p);
	free(p);

	// Determine job-wide node id and job-wide node count
	p = getenvp(*env, PMIX_JOB_NODES_ENV);
	if( p == NULL ){
		// shouldn't happen if we are under SLURM!
		PMIX_ERROR_NO(ENOENT, "No %s environment variable found!", PMIX_JOB_NODES_ENV);
		goto err_exit;
	}
	_pmix_job_nodes_list = xstrdup(p);
	hl = hostlist_create(p);
	_pmix_job_info.nnodes_job = hostlist_count(hl);
	_pmix_job_info.node_id_job = hostlist_find(hl, _pmix_this_host);
	hostlist_destroy(hl);

	if( _get_task_count(env, &_pmix_job_info.ntasks_job, &_pmix_job_info.ncpus_job ) < 0 ){
		_pmix_job_info.ntasks_job  = _pmix_job_info.ntasks;
		_pmix_job_info.ncpus_job  = _pmix_job_info.ntasks;
	}
	xassert( _pmix_job_info.ntasks <= _pmix_job_info.ntasks_job );

	// Get modex type
	_pmix_job_info.direct_modex = false;
	p = getenvp(*env, PMIX_DIRECT_MODEX_ENV);
	if( p != NULL ){
		if( atoi(p) == 1 )
			_pmix_job_info.direct_modex = true;
	}

	// Get and parse task-to-node mapping
	p = getenvp(*env, PMIX_SLURM_MAPPING_ENV);
	if( p == NULL ){
		// Direct modex won't work
		PMIX_ERROR_NO(ENOENT, "No %s environment variable found!", PMIX_SLURM_MAPPING_ENV);
		goto err_exit;
	}
	_pmix_job_info.task_map = unpack_process_mapping_flat(p, _pmix_job_info.nnodes, _pmix_job_info.ntasks, NULL);
	if(_pmix_job_info.task_map == NULL ){
		// Direct modex won't work
		PMIX_ERROR_NO(ENOENT, "Bad process mapping value found in %s env: %s",
					  PMIX_SLURM_MAPPING_ENV, p);
		goto err_exit;
	}

	return SLURM_SUCCESS;
err_exit:
	if( _pmix_job_nodes_list != NULL ){
		xfree(_pmix_job_nodes_list);
	}
	if( _pmix_step_nodes_list != NULL ){
		xfree(_pmix_step_nodes_list);
	}
	if( _pmix_this_host != NULL ){
		xfree(_pmix_this_host);
	}
	if( _pmix_job_info.task_map != NULL ){
		xfree(_pmix_job_info.task_map);
	}
	return SLURM_ERROR;
}

char *pmix_info_nth_child_name(int idx)
{
	hostlist_t hl = hostlist_create(pmix_info_step_hosts());
	int n = pmix_info_nth_child(idx);
	char *p = hostlist_nth(hl, n);
	hostlist_destroy(hl);
	return p;
}

char *pmix_info_nth_host_name(int n)
{
	hostlist_t hl = hostlist_create(pmix_info_step_hosts());
	char *p = hostlist_nth(hl, n);
	char *ret = xstrdup(p);
	free(p);
	hostlist_destroy(hl);
	return ret;
}


