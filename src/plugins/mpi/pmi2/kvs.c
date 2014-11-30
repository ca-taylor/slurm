/*****************************************************************************\
 **  kvs.c - KVS manipulation functions
 *****************************************************************************
 *  Copyright (C) 2011-2012 National University of Defense Technology.
 *  Written by Hongjia Cao <hjcao@nudt.edu.cn>.
 *  All rights reserved.
 *  Portions copyright (C) 2014 Institute of Semiconductor Physics
 *                     Siberian Branch of Russian Academy of Science
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

#include <stdlib.h>
#include <unistd.h>

#include "kvs.h"
#include "setup.h"
#include "tree.h"
#include "pmi.h"

#define MAX_RETRIES 5

/* for fence */
int tasks_to_wait = 0;
int children_to_wait = 0;
int kvs_seq = 1; /* starting from 1 */
int waiting_kvs_resp = 0;


/* bucket of key-value pairs */
typedef struct kvs_bucket {
	char **pairs;
	uint32_t count;
	uint32_t size;
} kvs_bucket_t;

static kvs_bucket_t *kvs_hash = NULL;
static uint32_t hash_size = 0;

typedef struct {
	char *buf;
	int payload;
	int size;
} temp_kvs_buf_t;
static List temp_kvs_buffers;
static int max_msg_size = MAX_PACK_MEM_LEN;
static int buf_offset = 0;

static int no_dup_keys = 0;

#define TASKS_PER_BUCKET 8
#define TEMP_KVS_SIZE_INC 2048

#define KEY_INDEX(i) (i * 2)
#define VAL_INDEX(i) (i * 2 + 1)
#define HASH(key) ( _hash(key) % hash_size)


inline static uint32_t
_hash(char *key)
{
	int len, i;
	uint32_t hash = 0;
	uint8_t shift;

	len = strlen(key);
	for (i = 0; i < len; i ++) {
		shift = (uint8_t)(hash >> 24);
		hash = (hash << 8) | (uint32_t)(shift ^ (uint8_t)key[i]);
	}
	return hash;
}


inline static void
_temp_kvs_buf_free(void *buf)
{
	temp_kvs_buf_t *elem = buf;
	xfree(elem->buf);
	xfree(buf);
}

inline static void
_temp_kvs_buf_new()
{
	temp_kvs_buf_t *elem = xmalloc(sizeof(*elem));
	elem->buf = xmalloc(TEMP_KVS_SIZE_INC);
	/* Reserve the space for the header (will be packed later) */
	elem->payload = buf_offset;
	elem->size = TEMP_KVS_SIZE_INC;
	list_push(temp_kvs_buffers, elem);
}

static inline int
_pack_header(Buf buf, uint32_t seq, uint32_t size)
{
	uint16_t cmd;
	uint32_t nodeid, num_children;
	/* put the tree cmd here to simplify message sending */
	if (in_stepd()) {
		cmd = TREE_CMD_KVS_FENCE;
	} else {
		cmd = TREE_CMD_KVS_FENCE_RESP;
	}

	pack16(cmd, buf);
	pack32(size, buf);
	pack32(seq, buf);
	if (in_stepd()) {
		nodeid = job_info.nodeid;
		/* XXX: TBC */
		num_children = tree_info.num_children + 1;
		pack32((uint32_t)nodeid, buf); /* from_nodeid */
		packstr(tree_info.this_node, buf); /* from_node */
		pack32((uint32_t)num_children, buf); /* num_children */
		pack32(kvs_seq, buf);
	} else {
		pack32(kvs_seq, buf);
	}
	return SLURM_SUCCESS;
}

inline static temp_kvs_buf_t*
_temp_kvs_buf_inc(temp_kvs_buf_t * elem, uint32_t size)
{
	/* Switching to the new buffer if necessary */
	if ((elem->payload + size) >= max_msg_size) {
		list_push(temp_kvs_buffers, elem);
		_temp_kvs_buf_new();
		elem = list_pop(temp_kvs_buffers);
	}
	/* Increasing the buffer if necessary */
	if (elem->payload + size > elem->size) {
		while (elem->payload + size > elem->size) {
			elem->size += TEMP_KVS_SIZE_INC;
		}
		xrealloc(elem->buf, elem->size);
	}
	return elem;
}

inline static void
_temp_kvs_buf_add_size(Buf buf, uint32_t size)
{
	temp_kvs_buf_t *elem = list_pop(temp_kvs_buffers);
	char *data = NULL;
	data = (char *) get_buf_data(buf);
	data += get_buf_offset(buf);

	if (elem->payload + size > elem->size) {
		elem = _temp_kvs_buf_inc(elem, size);
	}
	memcpy(elem->buf + elem->payload, data, size);
	elem->payload += size;
	list_push(temp_kvs_buffers, elem);
}

inline static void
_temp_kvs_buf_add(Buf buf)
{
	_temp_kvs_buf_add_size(buf, remaining_buf(buf));
}

static inline bool
_temp_kvs_try_merge(temp_kvs_buf_t * elem, temp_kvs_buf_t * elem2)
{
	int32_t size = elem2->payload - buf_offset;
	if ((elem->payload + size) < max_msg_size) {
		/* merging buffers payload */
		char *data = elem2->buf + buf_offset;
		if (elem->size < elem->payload + size) {
			xrealloc(elem->buf, elem->payload + size);
			elem->size = elem->payload + size;
		}
		memcpy(elem->buf + elem->payload, data, size);
		elem->payload += size;
		return true;
	}
	return false;
}

inline static void
_temp_kvs_buf_finalize()
{
	temp_kvs_buf_t *elem;
	List tmp;
	int frame_seq = 0, frame_cnt = 0;

	frame_seq = list_count(temp_kvs_buffers);
	/* - switching from stack order to the queue order
	 * - merging small messages together */
	tmp = list_create(_temp_kvs_buf_free);
	while ((elem = list_pop(temp_kvs_buffers))) {
		/* if the message is bigger than 90%
		 * of possible size - leave it alone */
		uint32_t treshold = (int) (max_msg_size * 0.9);
		if (elem->payload < treshold) {
			/* trying to merge it with other elements */
			ListIterator iter =
			    list_iterator_create(temp_kvs_buffers);
			temp_kvs_buf_t *elem2;
			while ((elem2 = list_next(iter))
			       && elem->payload < treshold) {
				if (_temp_kvs_try_merge(elem, elem2)) {
					list_delete_item(iter);
				}
			}
			list_iterator_destroy(iter);
		}
		list_push(tmp, elem);
	}

	/* Setup message headers in the frames */
	frame_cnt = list_count(tmp);
	debug3("Reduce number of messages from %d to %d",
	      frame_seq, frame_cnt);
	frame_seq = 0;
	while ((elem = list_dequeue(tmp))) {
		Buf buf = create_buf(elem->buf, elem->size);
		_pack_header(buf, frame_seq, frame_cnt);
		list_enqueue(temp_kvs_buffers, elem);
		frame_seq++;
	}
	list_destroy(tmp);
}

static inline int
_temp_kvs_reset(void)
{
	list_flush(temp_kvs_buffers);
	_temp_kvs_buf_new();

	tasks_to_wait = 0;
	children_to_wait = 0;

	return SLURM_SUCCESS;
}


extern int
temp_kvs_init()
{
	temp_kvs_buffers = list_create(_temp_kvs_buf_free);
	/* Estimate the header offset */
	Buf buf = init_buf(1024);
	_pack_header(buf, 0, 1);
	buf_offset = get_buf_offset(buf);
	free_buf(buf);
	/* Reset the buffers */
	_temp_kvs_reset();
	return SLURM_SUCCESS;
}

extern int
temp_kvs_merge(Buf buf)
{
	_temp_kvs_buf_add(buf);
	return SLURM_SUCCESS;
}

extern int
temp_kvs_add(char *key, char *val)
{
	Buf buf;
	uint32_t size;

	if (key == NULL || val == NULL)
		return SLURM_SUCCESS;
	buf =
	    init_buf(PMI2_MAX_KEYLEN + PMI2_MAX_VALLEN +
		     2 * sizeof(uint32_t));
	packstr(key, buf);
	packstr(val, buf);
	size = get_buf_offset(buf);
	set_buf_offset(buf, 0);
	_temp_kvs_buf_add_size(buf, size);
	return SLURM_SUCCESS;
}

static int
_send_reliable(char *buf, uint32_t size)
{
	int rc = SLURM_ERROR;
	unsigned int delay = 1, retry = 0;
	while (1) {
		if (retry == 1) {
			verbose("%s: failed to send temp kvs"
				", rc=%d, retrying. try #%d",
			      tree_info.this_node, rc, retry);
		}
		if (! in_stepd()) {	/* srun */
			rc = tree_msg_to_stepds(job_info.step_nodelist,
						size, buf);
		} else if (tree_info.parent_node != NULL) {
			/* non-first-level stepds */
			rc = tree_msg_to_stepds(tree_info.parent_node,
						size, buf);
		} else {	/* first level stepds */
			rc = tree_msg_to_srun(size, buf);
		}
		if (SLURM_SUCCESS == rc)
			break;
		retry++;
		if (MAX_RETRIES <= retry)
			break;
		/* wait, in case parent stepd / srun not ready */
		sleep(delay);
		delay *= 2;
	}
	return rc;
}

extern int
temp_kvs_send(void)
{
	int rc = SLURM_ERROR;
	ListIterator iter;
	temp_kvs_buf_t *elem;
	/* for logging only */
	uint32_t frame_seq = 0, frame_cnt;

	_temp_kvs_buf_finalize();

	/* expecting new kvs after now */
	kvs_seq++;

	/* Iterate over all database parts and send them one by one */
	iter = list_iterator_create(temp_kvs_buffers);
	frame_cnt = list_count(temp_kvs_buffers);
	while ((elem = list_next(iter))) {
		rc = _send_reliable(elem->buf, elem->payload);
		if (SLURM_SUCCESS != rc) {
			/* Were unable to send DB part */
			error("%s: completely failed to send temp kvs"
				"[%d/%d], rc=%d", tree_info.this_node,
				frame_seq, frame_cnt, rc);
			break;
		}
		frame_seq++;
	}
	list_iterator_destroy(iter);

	_temp_kvs_reset();	/* clear old temp kvs */
	return rc;
}

/**************************************************************/

extern int
kvs_init(void)
{
	debug3("mpi/pmi2: in kvs_init");

	hash_size = ((job_info.ntasks + TASKS_PER_BUCKET - 1) / TASKS_PER_BUCKET);

	kvs_hash = xmalloc(hash_size * sizeof(kvs_bucket_t));

	if (getenv(PMI2_KVS_NO_DUP_KEYS_ENV))
		no_dup_keys = 1;

	return SLURM_SUCCESS;
}

/*
 * returned value is not dup-ed
 */
extern char *
kvs_get(char *key)
{
	kvs_bucket_t *bucket;
	char *val = NULL;
	int i;

	debug3("mpi/pmi2: in kvs_get, key=%s", key);

	bucket = &kvs_hash[HASH(key)];
	if (bucket->count > 0) {
		for (i = 0; i < bucket->count; i ++) {
			if (! strcmp(key, bucket->pairs[KEY_INDEX(i)])) {
				val = bucket->pairs[VAL_INDEX(i)];
				break;
			}
		}
	}

	debug3("mpi/pmi2: out kvs_get, val=%s", val);

	return val;
}

extern int
kvs_put(char *key, char *val)
{
	kvs_bucket_t *bucket;
	int i;

	debug3("mpi/pmi2: in kvs_put");

	bucket = &kvs_hash[HASH(key)];

	if (! no_dup_keys) {
		for (i = 0; i < bucket->count; i ++) {
			if (! strcmp(key, bucket->pairs[KEY_INDEX(i)])) {
				/* replace the k-v pair */
				xfree(bucket->pairs[VAL_INDEX(i)]);
				bucket->pairs[VAL_INDEX(i)] = xstrdup(val);
				debug("mpi/pmi2: put kvs %s=%s", key, val);
				return SLURM_SUCCESS;
			}
		}
	}
	if (bucket->count * 2 >= bucket->size) {
		bucket->size += (TASKS_PER_BUCKET * 2);
		xrealloc(bucket->pairs, bucket->size * sizeof(char *));
	}
	/* add the k-v pair */
	i = bucket->count;
	bucket->pairs[KEY_INDEX(i)] = xstrdup(key);
	bucket->pairs[VAL_INDEX(i)] = xstrdup(val);
	bucket->count++;

	debug3("mpi/pmi2: put kvs %s=%s", key, val);
	return SLURM_SUCCESS;
}

extern int
kvs_clear(void)
{
	kvs_bucket_t *bucket;
	int i, j;

	for (i = 0; i < hash_size; i++) {
		bucket = &kvs_hash[i];
		for (j = 0; j < bucket->count; j++) {
			xfree(bucket->pairs[KEY_INDEX(j)]);
			xfree(bucket->pairs[VAL_INDEX(j)]);
		}
	}
	xfree(kvs_hash);

	return SLURM_SUCCESS;
}
