/*-------------------------------------------------------------------------------------*/

/*
 *  Create job-specific temporary directory for SLURM jobs.
 *
 *  Copyright (C) 2017 University of Florida 
 *  Produced at The University of Florida 
 *              Gainesville, Florida
 *  Written by Charles A. Taylor <chasman@ufl.edu>.
 *
 *  This is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *
 *  The name and initial code for this plugin came from 
 *
 *      https://github.com/grondo/slurm-spank-plugins/blob/master/tmpdir.c
 *
 *  However, that code produced a step-specific local temporary directory 
 *  rather than a job-specific temporary directory so it was not suitable to 
 *  our purposes and has been rewritten.  The resulting code bears little 
 *  resemblance to the original.  Nonetheless, the copyright from the original
 *  code is included.
 *
 *  Copyright (C) 2007-2008 Lawrence Livermore National Security, LLC.
 *  Produced at Lawrence Livermore National Laboratory.
 *  Written by Mark Grondona <mgrondona@llnl.gov>.
 *  
 *  UCRL-CODE-235358
 */

/*-------------------------------------------------------------------------------------*/
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <stdint.h>
#include <string.h>
#include <fts.h>
#include <sys/types.h>
#include <sys/stat.h> 
#include <slurm/slurm.h>
#include <slurm/spank.h>

/*-------------------------------------------------------------------------------------*/
#define TRUE          1
#define FALSE         0
#define CLOBBER       1
#define NOCLOBBER     0
#define UID_NOBODY   99
#define GID_NOBODY   99
#define BUFLEN     1024
#define KEYLEN       16
#define TMPDIRROOT "/scratch/local"

const char *spank_context_names[] = {
                        "S_CTX_ERROR",
                        "S_CTX_LOCAL",
                        "S_CTX_REMOTE",
                        "S_CTX_ALLOCATOR",
                        "S_CTX_SLURMD",
                        "S_CTX_JOB_SCRIPT"};

/*---------------------------This is Boilerplate---------------------------------------*/
SPANK_PLUGIN (tmpdir, 1);
/*-------------------------------------------------------------------------------------*/

/*
 * _print_context:
 *
 *  Print the string associated with the spank context.  Uses the globally declared 
 *  spank_context_names[] array to do this.
 */

/*-------------------------------------------------------------------------------------*/
void _print_context(const char *caller, spank_context_t context)
{
    if ( context == S_CTX_ERROR ) {
        slurm_error ("%s: Failed to get spank context", caller);
    } 
    else {
       slurm_verbose("%s: spank context = %s", caller, spank_context_names[context]);
    }
}
/*-------------------------------------------------------------------------------------*/

/*
 * _dir_exists:
 *
 *  Use stat() to see if the file pointed to by dir exists and is, in fact, a directory.
 *  If so, we assume it is the local scratch directory (SLURM_TMPDIR) and set the 
 *  ownership to match the uid/gid of the job.  
 *
 *  The group ownership cannot be set correctly at the time of creation because the job
 *  GID information is not available to the SPANK prolog/epilog routines.  
 */

/*-------------------------------------------------------------------------------------*/
int _dir_exists(char *dir, uid_t uid, gid_t gid)
{
  /* 
   * Check to see if the given directory exists.  If so, make sure
   * the ownership matches the uid/gid of the job.
   *
   * We do this because we create the directory in slurm_spank_job_prolog()
   * where the gid information is not available.
   */
    int rc;
    struct stat statbuf;
    
    rc = stat(dir, &statbuf);
    if ( rc ) {  /* stat() call failed */
        slurm_verbose("%s: stat call failed for dir = %s: %s", __func__, dir, strerror(errno));
        return(FALSE);
    }

    if ( ! S_ISDIR(statbuf.st_mode) ) { /* Not a directory */
        slurm_info("%s: Warning - %s is not a directory", __func__, dir);
        return(FALSE);
    }

    if ( (statbuf.st_uid != uid) || (statbuf.st_gid != gid) ) {
        slurm_verbose("%s: %s is owned by UID:GID = %u:%u - Fixing...", 
                    __func__, dir, statbuf.st_uid, statbuf.st_gid);
        if (chown(dir, uid, gid) == -ESPANK_ERROR) {
            slurm_error("%s: chown failed for %s %u.%u: %s", __func__, dir, uid, gid, strerror(errno));
            return(FALSE);
        }
        slurm_verbose("%s: %s ownership changed to UID:GID = %u:%u.", __func__, dir, uid, gid);
    }

    return(TRUE);
}
/*-------------------------------------------------------------------------------------*/

int _mk_tmpdir(char *dir, uid_t uid, gid_t gid)
{
   /* If the tmpdir directory exists and has the correct ownership,
    * just return "true", otherwise, create it, set the permissions
    * and return "true" or report the error.
    */
    int rc      = ESPANK_SUCCESS;
    mode_t mask = 0770;

    slurm_debug2("%s: dir  = %s", __func__, dir);
    slurm_debug2("%s: uid  = %d", __func__, uid);
    slurm_debug2("%s: gid  = %d", __func__, gid);

    if (_dir_exists(dir, uid, gid)) { /* nothing to do */
        slurm_debug2("%s: Directory %s exists.  Returning...", __func__, dir);
        return(ESPANK_SUCCESS);
    }

   /*
    * Note 1: mkdir() and chown() return '-1" on error.
    *
    * Note 2: The SLURM_JOB_GID is not available to the epilog/prolog spank routines.  
    *         Who knows why but it doesn't matter.  This will get called with GID_NOBODY
    *         from the job_prolog() and job_epilog() routines but will get fixed in the
    *         user_init() routine where we also set SLURM_TMPDIR.
    */
    slurm_verbose("%s: Creating directory %s...", __func__, dir);
    if ((rc = mkdir(dir , mask)) == -ESPANK_ERROR) {
        slurm_error("%s: mkdir failed for %s. Error = %d", __func__, dir, strerror(errno));
        return(ESPANK_ERROR);
    }

    if (chown(dir, uid, gid) == -ESPANK_ERROR) {
        slurm_error("%s: chown failed for %s %d.%d: %s", __func__, dir, uid, gid, strerror(errno));
        return(ESPANK_ERROR);
    }

    return(ESPANK_SUCCESS);
}
/*-------------------------------------------------------------------------------------*/

/*
 * _get_job_info:
 *
 * Use spank_get_item() to get the jobid, job uid, and job gid (if available) from SLURM.
 *
 * The S_JOB_GID is not available in S_CTX_JOB_SCRIPT context but get_spank_item() does 
 * not return an error.  So in that context, we just set the GID to "nobody" and don't
 * bother to call spank_get_item().
 */

/*-------------------------------------------------------------------------------------*/
int _get_job_info(spank_t sp, uint32_t *jobid, uid_t *uid, gid_t *gid)
{
    int             rc    = ESPANK_SUCCESS;
    spank_context_t context;

    *jobid = 0;
    if (spank_get_item (sp, S_JOB_ID, jobid) != ESPANK_SUCCESS) {
        slurm_error("%s: Failed to get S_JOB_ID from SLURM", __func__);
        return(ESPANK_ERROR);
    }
    else {
        slurm_verbose("%s: S_JOB_ID = %u", __func__, *jobid);
    }

    *uid = UID_NOBODY;
    if (spank_get_item (sp, S_JOB_UID, uid) != ESPANK_SUCCESS) {
        slurm_error("%s: Failed to get S_JOB_UID from SLURM", __func__);
        return(ESPANK_ERROR);
    }
    else {
        slurm_verbose("%s: S_JOB_UID = %d", __func__, *uid);
    }

   /* 
    * Can't get the job GID in JOB_SCRIPT context (sigh).
    * For our purposes, we'll just set it to "nobody" and
    * fix it in spank_user_init().
    */
    context = spank_context();
    if ( context == S_CTX_JOB_SCRIPT ) {
        *gid = GID_NOBODY;
        return(ESPANK_SUCCESS);
    }

    *gid = GID_NOBODY;
    if (spank_get_item (sp, S_JOB_GID, gid) != ESPANK_SUCCESS) {
        slurm_error("%s: Failed to get S_JOB_GID from SLURM", __func__);
        return(ESPANK_ERROR);
    }
    else {
        slurm_verbose("%s: S_JOB_GID = %d", __func__, *gid);
    }
    return(ESPANK_SUCCESS);
}
/*-------------------------------------------------------------------------------------*/


/*
 *
 * _env_job_info:
 *
 * This routine proved to be unnecessary.  It was not clear at first that spank_get_item()
 * was going to work reliably from S_CTX_JOB_SCRIPT context and that pulling the info 
 * from the corresponding environment variables might be a better way to go. 
 *
 * It proved unnecessary but I'm leaving the code here for posterity.
 *
 */

/*-------------------------------------------------------------------------------------*/
int _env_job_info(uint32_t *jobid, uid_t *uid, gid_t *gid)
{
    char           *strptr;
    int             rc    = ESPANK_SUCCESS;
    spank_context_t context;

    *jobid = NO_VAL;
    strptr = getenv("SLURM_JOB_ID");
    if ( ! strptr ) {
        slurm_error("%s: Failed to get SLURM_JOB_ID from SLURM", __func__);
        return(ESPANK_ERROR);
    }
    else {
        *jobid = (uint32_t) atoi(strptr);
        slurm_verbose("%s: S_JOB_ID = %u", __func__, *jobid);
    }

    *uid = UID_NOBODY;
    strptr = getenv("SLURM_JOB_UID");
    if ( ! strptr ) {
        slurm_error("%s: Failed to get SLURM_JOB_UID from environment", __func__);
        return(ESPANK_ERROR);
    }
    else {
        *uid = (uid_t) atoi(strptr);
        slurm_verbose("%s: S_JOB_UID = %d", __func__, *uid);
    }

   /*
    * SLURM_JOB_GID and SLURM_JOB_GROUP are only available to 
    * PrologSlurmctld and EpilogSlurmctld.  Seems stupid but 
    * that's what the docs say and trying to get it fails so
    * we won't bother.
    */
    context = spank_context();
    if ( context == S_CTX_JOB_SCRIPT ) {
       *gid = GID_NOBODY;
        return(ESPANK_SUCCESS);
    }

    strptr = getenv("SLURM_JOB_GID");
    if ( ! strptr ) {
       *gid = GID_NOBODY;
        slurm_error("%s: Failed to get SLURM_JOB_GID from environment", __func__);
        //return(ESPANK_ERROR);
    }
    else {
       *gid = (uint32_t) atoi(strptr);
        slurm_verbose("%s: S_JOB_GID = %d", __func__, *gid);
    }
    return(ESPANK_SUCCESS);
}
/*-------------------------------------------------------------------------------------*/

/*
 * _rm_tmp_dir:
 *
 * The code for the _rm_tmp_dir() routine was "borrowed" from a post on stackoverflow 
 * and modified slightly for our purposes.  There was no copyright associated with 
 * the post.
 * 
 * Never used the "fts" functions before - pretty handy.
 *
 *   https://stackoverflow.com/questions/2256945/removing-a-non-empty-directory-programmatically-in-c-or-c
 */

/*-------------------------------------------------------------------------------------*/
int _rm_tmp_dir(const char *dir)
{
    int     rc    = ESPANK_SUCCESS;
    FTS    *fts_p = NULL;
    FTSENT *this  = NULL;

/*
 * Cast needed (in C) because fts_open() takes a "char * const *", instead
 * of a "const char * const *", which is only allowed in C++. fts_open()
 * does not modify the argument.
 */
    char *files[] = { (char *) dir, NULL };

/*
 *  FTS_NOCHDIR  - Avoid changing cwd, which could cause unexpected behavior
 *                 in multithreaded programs
 *  FTS_PHYSICAL - Don't follow symlinks. Prevents deletion of files outside
 *                 of the specified directory
 *  FTS_XDEV     - Don't cross filesystem boundaries
 */
    slurm_debug2("%s: Calling fts_open()...", __func__);
    fts_p = fts_open(files, FTS_NOCHDIR | FTS_PHYSICAL | FTS_XDEV, NULL);
                     
    if (!fts_p) {
        slurm_error("%s: fts_open failed: %s", dir, strerror(errno));
        rc = ESPANK_ERROR;
        goto done;
    }

    slurm_debug2("%s: Starting while loop ...", __func__);
    while ((this = fts_read(fts_p))) {
        slurm_debug2("%s: Current file = %s", __func__, this->fts_accpath);
        switch (this->fts_info) {
        case FTS_NS:
        case FTS_DNR:
        case FTS_ERR:
            slurm_error("%s: fts_read error: %s", this->fts_accpath, strerror(this->fts_errno));
            break;
        case FTS_DC:
        case FTS_DOT:
        case FTS_NSOK:
            // Not reached unless FTS_LOGICAL, FTS_SEEDOT, or FTS_NOSTAT were
            // passed to fts_open()
            break;
        case FTS_D:
            // Do nothing. Need depth-first search, so directories are deleted
            // in FTS_DP
            break;
        case FTS_DP:
        case FTS_F:
        case FTS_SL:
        case FTS_SLNONE:
        case FTS_DEFAULT:
            slurm_verbose("%s: Removing %s...",  __func__, this->fts_accpath);
            if (remove(this->fts_accpath) < 0) {
                slurm_error("%s: Failed to remove %s: %s", __func__, this->fts_path, strerror(errno));
                rc = ESPANK_ERROR;
            }
            break;
        }
    }

done:
    if (fts_p) {
        fts_close(fts_p);
    }
    return rc;
}
/*-------------------------------------------------------------------------------------*/

/* Here we create the SLURM_TMPDIR directory on all nodes allocated to the job
 * and stuff the path into the SLURM_TMPDIR environment variable.  Additionally,
 * if TMPDIR is not found in the environment, we set that to point to the SLURM_TMPDIR
 * directory as well.
 *
 * If the user has set "TMPDIR" we do not overwrite it.
 *
 * The SLURM_TMPDIR_KEY environment variables allow us to ensure that the temporary
 * directory is deleted by the job/step that for created it.  In other words, 
 * slurm_spank_user_init() and slurm_spank_exit() are called for each job and job
 * step.  We want to be sure that the temporary directory is not removed prematurely.
 * This mechanism ensures that the job or step that initiated the creation is the same
 * one that initiates the deletion.
 *
 * It is ugly but it works.
 */
int slurm_spank_job_prolog(spank_t sp, int ac, char **argv)
{
    int             strlen, rc;
    uid_t           uid;
    gid_t           gid;
    uint32_t        jobid;
    char           *strptr;
    char            tmpdir1[BUFLEN];
    char            tmpdir2[BUFLEN];
    spank_context_t context;

    rc = ESPANK_SUCCESS;

    context = spank_context();
    _print_context(__func__, context);

    slurm_debug2("%s: Calling _get_job_info()", __func__);
    rc = _get_job_info(sp, &jobid, &uid, &gid);
    if (rc != ESPANK_SUCCESS) {
        slurm_error ("%s: _get_job_info() failed", __func__);
        return(ESPANK_ERROR);
    }

    slurm_debug2("%s: jobid   = %u", __func__, jobid);
    slurm_debug2("%s: uid     = %u", __func__, uid);
    slurm_debug2("%s: gid     = %u", __func__, gid);

    if (jobid >= NO_VAL) {
        slurm_error ("%s: Invalid JobId = %ju", __func__, jobid);
        return(ESPANK_ERROR);
    }

    strlen = snprintf (tmpdir1, sizeof(tmpdir1), "%s/%u", TMPDIRROOT, jobid);
    if ((strlen < 0) || (strlen > BUFLEN - 1)) {
        slurm_error ("%s: SLURM_TMPDIR = %s too large", __func__);
        return (ESPANK_ERROR);
    }

    strptr = tmpdir1;
    slurm_debug2("%s: Calling _mk_tmpdir(). tmpdir = %s", __func__, strptr);
    rc = _mk_tmpdir(strptr, uid, gid);
    if ( rc != ESPANK_SUCCESS ) {
        slurm_error ("%s: _mk_tmpdir() failed", __func__);
        return(ESPANK_ERROR);
    }

    slurm_info("%s: Created local temporary directory = %s", __func__, strptr);

    slurm_verbose("%s: Setting SLURM_TMPDIR = %s", __func__, strptr);
    rc = setenv("SLURM_TMPDIR", strptr, CLOBBER);
    if (rc != ESPANK_SUCCESS) {
        slurm_error ("%s: setenv() failed for SLURM_TMPDIR = %s, rc = %s) failed.", 
                      __func__,  spank_strerror(rc));
        return (ESPANK_ERROR);
    }

   /*
    * Here, if the user has not set TMPDIR, wet set it to the value of SLURM_TMPDIR
    * Why?  Just seems like a reasonable thing to do.
    */
    slurm_verbose("%s: Checking environment for TMPDIR", __func__);
    strptr = getenv("TMPDIR");
    if ( strptr ) {
        slurm_verbose("%s: TMPDIR already set: Value = %s", __func__, tmpdir2);
    }
    else {
        slurm_verbose("%s: getenv() failed. rc = %s", __func__, spank_strerror(rc));
        slurm_verbose("%s: TMPDIR not set in user's environment", __func__);
    
        strptr = tmpdir1;
        slurm_verbose("%s: setting TMPDIR to %s", __func__, strptr);
        rc = setenv("TMPDIR", strptr, CLOBBER);
        if (rc != ESPANK_SUCCESS) {
            slurm_error ("%s: setenv() failed for TMPDIR = %s, rc = %s) failed.", __func__,  strptr, spank_strerror(rc));
            return (ESPANK_ERROR);
        }
    }
    return (ESPANK_SUCCESS);
}
/*-------------------------------------------------------------------------------------*/

/*
 *  Remove the job-specific directory.  This needs to happen on each node but not before the job has
 *  actually completed.  Thus, we don't want it removed prematurely due to a call at the completion
 *  of a job step within the batch job i.e. srun invoked inside the job script.
 */
int slurm_spank_job_epilog(spank_t sp, int ac, char **argv)
{
    int             strlen, rc;
    uid_t           uid;  
    gid_t           gid;  
    uint32_t        jobid;
    char           *strptr;
    char            tmpdir1[BUFLEN];
    char            tmpdir2[BUFLEN];
    spank_context_t context;

    rc = ESPANK_SUCCESS;

    context = spank_context();
    _print_context(__func__, context);

    slurm_debug2("%s: Calling _get_job_info()", __func__);
    rc = _get_job_info(sp, &jobid, &uid, &gid);
    if (rc != ESPANK_SUCCESS) {
        slurm_error ("%s: _get_job_info() failed", __func__);
        return(ESPANK_ERROR);
    }

    slurm_debug2("%s: jobid   = %u", __func__, jobid);
    slurm_debug2("%s: uid     = %u", __func__, uid);
    slurm_debug2("%s: gid     = %u", __func__, gid);

    if (jobid >= NO_VAL) {
        slurm_error ("%s: Invalid JobId = %ju", __func__, jobid);
        return(ESPANK_ERROR);
    }

   /*
    * For completeness, initialize tmpdir1, tmpdir2
    */
    strncpy(tmpdir1, "NOT_SET", sizeof(tmpdir1));
    strncpy(tmpdir2, "NOT_SET", sizeof(tmpdir2));

   /*
    * Build the path string from the base path and job id.
    */
    strlen = snprintf (tmpdir2, sizeof(tmpdir2), "%s/%u", TMPDIRROOT, jobid);
    if ((strlen < 0) || (strlen > BUFLEN - 1)) {
        slurm_error ("%s: Error creating SLURM_TMPDIR path string", __func__);
        return (ESPANK_ERROR);
    }

   /*
    * Does SLURM_TMPDIR exist?  If so, remove it.  If not, issue a warning.
    */
    strptr = tmpdir2;
    if (! _dir_exists(strptr, uid, gid)) {
        slurm_info("%s: Warning - SLURM temporary directory %s not found.", __func__, strptr);
        return(ESPANK_SUCCESS);
    }

    slurm_verbose("%s: Removing SLURM_TMPDIR = %s", __func__, strptr);
    rc = _rm_tmp_dir(strptr);
    if ( rc != ESPANK_SUCCESS ) {
        slurm_error ("%s: Failed to remove %s", __func__, strptr);
        return (ESPANK_ERROR);
    }
    slurm_info("%s: Removed local temproray directory = %s", __func__, strptr);
    return (ESPANK_SUCCESS);
}
/*-------------------------------------------------------------------------------------*/

/*
 * This routine is necessary because,
 *   a) We can't modify the jobs environment from S_CTX_JOB_SCRIPT context
 *   b) We can't get the job GID from S_CTX_JOB_SCRIPT context.
 *
 *   Thus, we use this routine to 
 *     a) make sure that the directory was, indeed, created 
 *     b) fix the GID part of the ownership by calling _dir_exists()
 *     c) set SLURM_TMPDIR in the job's environment (local to each node).
 *     d) set TMPDIR = SLURM_TMPDIR if TMPDIR is not already set.
 */
int slurm_spank_user_init(spank_t sp, int ac, char **argv)
{
    int             strlen, rc;
    uid_t           uid;
    gid_t           gid;
    uint32_t        jobid;
    char           *strptr;
    char            tmpdir1[BUFLEN];
    char            tmpdir2[BUFLEN];
    spank_context_t context;

    rc = ESPANK_SUCCESS;

    context = spank_context();
    _print_context(__func__, context);

   /* 
    * If not remote context, do nothing
    */
    if ( context != S_CTX_REMOTE ) {
        return(ESPANK_SUCCESS);
    }

    slurm_debug2("%s: Calling _get_job_info()", __func__);
    rc = _get_job_info(sp, &jobid, &uid, &gid);
    if (rc != ESPANK_SUCCESS) {
        slurm_error ("%s: _get_job_info() failed", __func__);
        return(ESPANK_ERROR);
    }

    slurm_debug2("%s: jobid   = %u", __func__, jobid);
    slurm_debug2("%s: uid     = %u", __func__, uid);
    slurm_debug2("%s: gid     = %u", __func__, gid);

    if (jobid >= NO_VAL) {
        slurm_error ("%s: Invalid JobId = %ju", __func__, jobid);
        return(ESPANK_ERROR);
    }

    strlen = snprintf (tmpdir1, sizeof(tmpdir1), "%s/%u", TMPDIRROOT, jobid);
    if ((strlen < 0) || (strlen > BUFLEN - 1)) {
        slurm_error ("%s: SLURM_TMPDIR = %s too large", __func__);
        return (ESPANK_ERROR);
    }

    strptr = tmpdir1;
    if ( ! _dir_exists(strptr, uid, gid) ) { /* Should probably make this a hard error. */
        slurm_info("%s: Warning - %s does not exist. SLURM_TMPDIR will not be set", __func__, strptr);
        return (ESPANK_SUCCESS);
    }

    slurm_verbose("%s: Setting SLURM_TMPDIR = %s", __func__, strptr);
    rc = spank_setenv(sp, "SLURM_TMPDIR", strptr, CLOBBER);
    if (rc != ESPANK_SUCCESS) {
        slurm_error ("%s: spank_setenv() failed for SLURM_TMPDIR = %s, rc = %s) failed.", 
                     __func__,  spank_strerror(rc));
        return (ESPANK_ERROR);
    }

   /*
    * Here, if the user has not set TMPDIR, we set it to the value of SLURM_TMPDIR
    * Why?  Just seems like a reasonable thing to do since many apps will look for
    * TMPDIR in the environment and use it if set.  
    *
    * Note: Do we want to do the same for TMP?
    */
    rc = spank_getenv(sp, "TMPDIR", tmpdir2, sizeof(tmpdir2));
    if ( rc == ESPANK_SUCCESS ) { /* Nothing to do */
        slurm_verbose("%s: TMPDIR already set: Value = %s", __func__, tmpdir2);
        return (ESPANK_SUCCESS);
    }

    slurm_verbose("%s: spank_getenv() failed. rc = %s", __func__, spank_strerror(rc));
    slurm_verbose("%s: TMPDIR not set in user's environment", __func__);
    
    strptr = tmpdir1;
    slurm_verbose("%s: setting TMPDIR to %s", __func__, strptr);
    rc = spank_setenv(sp, "TMPDIR", strptr, CLOBBER);
    if (rc != ESPANK_SUCCESS) {
        slurm_error ("%s: spank_setenv() failed for TMPDIR = %s, rc = %s) failed.", 
                     __func__,  strptr, spank_strerror(rc));
        return (ESPANK_ERROR);
    }
    return (ESPANK_SUCCESS);
}
