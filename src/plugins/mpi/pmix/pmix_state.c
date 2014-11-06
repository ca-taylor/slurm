/*****************************************************************************\
 **  pmix_state.c - PMIx agent state related code
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

#include "pmix_common.h"
#include "pmix_debug.h"
#include "pmix_info.h"
#include "pmix_state.h"

pmix_state_t pmix_state;

void pmix_state_init()
{
  size_t size, i;
#ifndef NDEBUG
  pmix_state.magic = PMIX_STATE_MAGIC;
#endif
  pmix_state.cli_size = pmix_info_ltasks();
  size = pmix_state.cli_size * sizeof(client_state_t);
  pmix_state.cli_state = xmalloc( size );
  for( i = 0; i < pmix_state.cli_size; i++ ){
    pmix_state.cli_state[i].fd = -1;
    pmix_state.cli_state[i].state = PMIX_CLI_UNCONNECTED;
  }
  pmix_state.coll.state = PMIX_COLL_SYNC;
  pmix_state.coll.local_joined = 0;
  pmix_state.coll.nodes_joined = 0;
  pmix_state.coll.local_contrib = xmalloc(sizeof(uint8_t) * pmix_info_ltasks());
  memset(pmix_state.coll.local_contrib, 0, sizeof(uint8_t) * pmix_info_ltasks());
  pmix_state.coll.nodes_contrib = xmalloc(sizeof(uint8_t) * pmix_info_childs());
  memset(pmix_state.coll.nodes_contrib, 0, sizeof(uint8_t) * pmix_info_childs());
}

// Check events
static int _coll_new_contrib()
{
  switch( pmix_state.coll.state ){
  case PMIX_COLL_SYNC:
    PMIX_DEBUG("Start collective");
    pmix_state.coll.state = PMIX_COLL_GATHER;
  case PMIX_COLL_GATHER:
    PMIX_DEBUG("New contribution");
    return SLURM_SUCCESS;
  case PMIX_COLL_FORWARD:
    // This is not ok. Node shouldn't contribute during forward phase
    PMIX_ERROR("New contribution during FORWARD phase");
    return SLURM_ERROR;
  default:
    PMIX_ERROR("pmix_state.coll.state has incomplete value %d", pmix_state.coll.state);
    xassert( 0 );
    return SLURM_ERROR;
  }
}

// Check events
static int _coll_forward()
{
  switch( pmix_state.coll.state ){
  case PMIX_COLL_SYNC:
    PMIX_ERROR("Inconsistency: can't go to FORWARD from SYNC state");
    return SLURM_ERROR;
  case PMIX_COLL_GATHER:
    pmix_state.coll.state = PMIX_COLL_FORWARD;
    PMIX_DEBUG("Transit to FORWARD state");
    return SLURM_SUCCESS;
  case PMIX_COLL_FORWARD:
    PMIX_ERROR("FORWARD phase was already enabled");
    return SLURM_ERROR;
  default:
    PMIX_ERROR("pmix_state.coll.state has incomplete value %d", pmix_state.coll.state);
    xassert( 0 );
    return SLURM_ERROR;
  }
}

// Check events
static int _coll_sync()
{
  switch( pmix_state.coll.state ){
  case PMIX_COLL_SYNC:
    PMIX_ERROR("SYNC phase is already enabled");
    return SLURM_ERROR;
  case PMIX_COLL_GATHER:
    PMIX_ERROR("Cannot transit from GATHER phase to SYNC phase");
    return SLURM_ERROR;
  case PMIX_COLL_FORWARD:
    PMIX_DEBUG("Go to SYNC state");
    pmix_state.coll.state = PMIX_COLL_SYNC;
    return SLURM_SUCCESS;
  default:
    PMIX_ERROR("pmix_state.coll.state has incomplete value %d", pmix_state.coll.state);
    xassert( 0 );
    return SLURM_ERROR;
  }
}

bool pmix_state_node_contrib_ok(int idx)
{
  // Check state consistence
  if( _coll_new_contrib() ){
    char *p = pmix_info_nth_child_name(idx);
    PMIX_ERROR("%s [%d]: Inconsistent contribution from node %s [%d]",
               pmix_info_this_host(), pmix_info_nodeid(), p, pmix_info_nth_child(idx) );
    xfree(p);
    return false;
  }

  if( pmix_state.coll.nodes_contrib[idx] ){
    char *p = pmix_info_nth_child_name(idx);
    PMIX_ERROR("%s [%d]: Node %s [%d] already contributed to the collective",
               pmix_info_this_host(), pmix_info_nodeid(), p, pmix_info_nth_child(idx) );
    xfree(p);
    return false;
  }

  pmix_state.coll.nodes_contrib[idx] = 1;
  pmix_state.coll.nodes_joined++;
  return true;
}

bool pmix_state_task_contrib_ok(int idx)
{
  // Check state consistence
  if( _coll_new_contrib() ){
    PMIX_ERROR("%s [%d]: Inconsistent contribution from task %d",
               pmix_info_this_host(), pmix_info_nodeid(), pmix_info_task_id(idx) );
    return false;
  }

  if( pmix_state.coll.local_contrib[idx] ){
    PMIX_ERROR("%s [%d]: Task %d already contributed to the collective",
               pmix_info_this_host(), pmix_info_nodeid(), pmix_info_task_id(idx) );
    return false;
  }
  pmix_state.cli_state[idx].state = PMIX_CLI_COLL;
  pmix_state.coll.local_contrib[idx] = 1;
  pmix_state.coll.local_joined++;
  return true;
}

bool pmix_state_coll_local_ok()
{
  return (pmix_state.coll.local_joined == pmix_info_ltasks() ) &&
      (pmix_state.coll.nodes_joined == pmix_info_childs());
}


bool pmix_state_node_contrib_cancel(int idx)
{
  // Check state consistence
  if( pmix_state.coll.state != PMIX_COLL_FORWARD ){
    PMIX_DEBUG("WARNING: trying to cancel contrib for node %s [%d] during wrong phase %d\n",
               pmix_info_nth_child_name(idx), pmix_info_nth_child(idx), pmix_state.coll.state );
    return false;
  }
  // We need to contribute before we cancel!
  xassert(pmix_state.coll.nodes_contrib[idx]);
  pmix_state.coll.nodes_contrib[idx] = 0;
  pmix_state.coll.nodes_joined--;
  xassert(pmix_state.coll.nodes_joined >=0 );
  return true;
}

bool pmix_state_task_contrib_cancel(int idx)
{
  // Check state consistence
  if( pmix_state.coll.state != PMIX_COLL_FORWARD ){
    PMIX_DEBUG("%s [%d]: WARNING: trying to cancel contrib for task [%d] during wrong phase %d\n",
               pmix_info_this_host(), pmix_info_nodeid(), pmix_info_task_id(idx), pmix_state.coll.state );
    return false;
  }
  // We need to contribute before we cancel!
  xassert(pmix_state.coll.local_contrib[idx]);
  pmix_state.coll.local_contrib[idx] = 0;
  pmix_state.coll.local_joined--;
  xassert(pmix_state.coll.local_joined >=0 );
  return true;
}

bool pmix_state_coll_forwad()
{
  if( _coll_forward() ){
    PMIX_ERROR("Cannot transit to FORWARD state!");
    return false;
  }
  return true;
}

bool pmix_state_coll_sync()
{
  if( _coll_sync() ){
    PMIX_ERROR("Cannot transit to SYNC state!");
    return false;
  }
  return true;
}
