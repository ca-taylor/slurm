/*****************************************************************************\
 **  pmix_debug.h - PMIx debug primitives
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
  error("%s [%d] %s:%d [%s] mpi/pmix: ERROR: " format ": %s (%d)", \
		pmix_info_this_host(), pmix_info_nodeid(),    \
		file_base, __LINE__, __FUNCTION__,            \
        ## args, strerror(errno), errno);             \
}

#define PMIX_ERROR_NO(err, format, args...) {                 \
  char file[] = __FILE__;                             \
  char *file_base = strrchr(file, '/');               \
  if( file_base == NULL ){                            \
	file_base = file;                                 \
  }                                                   \
  if( err == 0 ){                                     \
	error("%s [%d] %s:%d [%s] mpi/pmix: ERROR: " format, \
		pmix_info_this_host(), pmix_info_nodeid(),    \
		file_base, __LINE__, __FUNCTION__, ## args);  \
	} else {                                          \
	error("%s [%d] %s:%d [%s] mpi/pmix: ERROR: " format ": %s (%d)", \
		pmix_info_this_host(), pmix_info_nodeid(),    \
		file_base, __LINE__, __FUNCTION__,            \
		## args, strerror(err), err);                 \
  }                                                   \
}
#ifdef NDEBUG
#define pmix_debug_hang(x)
#else
inline static void pmix_debug_hang(int delay)
{
	while (delay) {
		sleep(1);
	}
}
#endif
#endif				// PMIX_DEBUG_H
