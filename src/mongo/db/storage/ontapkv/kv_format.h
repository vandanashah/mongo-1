#include "kv.h"

#ifndef _KV_FORMAT_H_
#define _KV_FORMAT_H_

typedef enum kvreq_type {
	KV_GET_ONE = 1,
	KV_PUT_ONE,
	KV_DEL_ONE,
	KV_GET_BULK,
	KV_DEL_BULK,
	KV_LAST_REQ_TYPE = KV_DEL_BULK
} kvreq_type_t;

typedef enum kvresp_code {
	KV_NO_ERROR = 0,
	KV_NO_CONTAINER,
	KV_KEY_NOT_FOUND,
	KV_NO_SPACE
} kvresp_code_t;

/*
 * Request format
 */ 
#define KV_MAGIC 0x4B56 /* KV */

typedef struct kvreq_header_s {
	int32_t kvreq_magic:16;
	int32_t kvreq_flags:16;
	kvreq_type_t kvreq_type;
	uint32_t kvreq_container_id;
} __attribute__((__packed__)) kvreq_header_t;

typedef struct kvreq_struct_s {
	kvreq_header_t kvreq_header;
	void *kvreq_data;
} kvreq_struct_t;

/*
 * hint of 0 means no hint
 */
typedef struct kvreq_get_one_s {
	kv_storage_hint_t kvreq_get_hint;
	int32_t kvreq_get_keylen;
	/* Key follows */
//	void *kvreq_get_key;
} __attribute__((__packed__)) kvreq_get_one_t;

typedef struct kvreq_put_one_s {
	int32_t kvreq_put_keylen;
	int32_t kvreq_put_datalen;
	kv_txn_id kvreq_put_txnid;
	/* This is followed by key and data values */
} __attribute__((__packed__)) kvreq_put_one_t;

/*
 * The total length is easily derivable for fixed-size record keys.
 * May need fixing when variable length keys are supported
 */ 
typedef struct kvreq_get_bulk_s {
	int32_t kvreq_getb_numkeys;
	kvreq_get_one_t *kvreq_getb_keys;
} kvreq_get_bulk_t;

/*
 * Response format
 */ 
typedef struct kvresp_get_one_s {
	kvresp_code_t kvresp_get;
	kv_storage_hint_t kvresp_get_hint;
	int kvresp_get_datalen;
	/* data followes */
	//void *kvresp_get_data;
} __attribute__((__packed__)) kvresp_get_one_t;

typedef struct kvresp_put_one_s {
	int64_t	     kvresp_record_id;
	kvresp_code_t kvresp_put;
	kv_storage_hint_t kvresp_put_hint;
} __attribute__((__packed__)) kvresp_put_one_t;

/*
 * May need fixing
 */ 
typedef struct kvresp_get_bulk_s {
	kvresp_get_one_t *kvresp_getb;
} kvresp_get_bulk_t;

#endif
