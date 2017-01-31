#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/storage/capped_callback.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/concurrency/synchronization.h"
#include "mongo/util/concurrency/rwlock.h"
#include "mongo/util/fail_point_service.h"
#include <iostream>
#include "mongo/db/storage/ontapkv/kv_format.h"


/*
 * To decide:
 * Data cache or metadata cache only:
 * May be both.
 * How to implement
 * Define a KVCacheEntry that could potentially contain both data
 * and metadata.
 */

#pragma once

namespace mongo {
const int CACHE_SIZE = 1024;
const int h_bits = 10; //cache size is 2^10
 
class DataContext {
public:
	int getContext() {
		return ctx;
	}

	void setContext(int ctx) {
		this->ctx = ctx;
	}

private:
	int ctx;
};

class StorageUberContext {
public:
	int size() {return sizeof(hint); }
	
	kv_storage_hint_t getHint() {
		return hint;
	}

	DataContext getContext() {
		return ctx;
	}
	
	StorageUberContext getMetadata() {
		StorageUberContext storage_ctx;
		storage_ctx.hint = getHint();
		storage_ctx.ctx = getContext();
		return storage_ctx;
	}

	void setHint(kv_storage_hint_t hint) {
		this->hint = hint;
	}

	void setDataContext(kv_storage_hint_t hint) {
		DataContext data_ctx;
		data_ctx.setContext(sizeof(hint));
		this->ctx = data_ctx;
	}
private:
	kv_storage_hint_t hint;
	DataContext ctx;	
};

class OntapKVCacheEntry {
public:
	OntapKVCacheEntry(int32_t cont_id, RecordId id,
			  kv_storage_hint_t ctx, void *data,
			  int len,	
			  OntapKVCacheEntry *next) {
		this->_container = cont_id;
		this->_id = id;
		this->setMetadata(ctx);
		this->setData(data, len);
		this->data_len = len;
		this->next = next;
	}
	
	bool hasData(void) { return _data != NULL;}

	StorageUberContext getMetadata(void) {
		return ctx;
	}

	void setMetadata(kv_storage_hint_t hint) {
		StorageUberContext sto_ctx;
		sto_ctx.setHint(hint);
		sto_ctx.setDataContext(hint);
		this->ctx = sto_ctx;
	}
		
	int32_t getContainerId() {
		return _container;
	}

	RecordId getRecordId() {
		return _id;
	}
	
	void *getData() {
		return _data;
	}

	OntapKVCacheEntry *getNext() {
		return next;
	}

	void setContainerId(int32_t cont_id) {
		this->_container = cont_id;
	}

	void setRecordId(RecordId id) {
		this->_id = id;
	}

	void setData(void *data, int size) {
		char *buf_data = (char *)malloc(size * sizeof(char *));
		//SharedBuffer buf_data = SharedBuffer::allocate(size);
		memcpy(buf_data, data, size);
		this->_data = buf_data;
	}

	void setDataSize(int len) {
		this->data_len = len;
	}

	void setNext(OntapKVCacheEntry *next) {
		this->next = next;
	}
	
	int getDataSize() {
		return data_len;
	}

	int64_t getKey() {
		return (((int64_t) this->_container) + (this->_id).repr());
	}

private:
	int32_t _container;
	RecordId _id;
	void *_data;
	int data_len;
	StorageUberContext ctx;
        OntapKVCacheEntry *next;
};

/*
 * Lookup may return:
 * none. Nothing in cache.
 * mdonly. Metadata found in cache.
 * data. Lookup got everything.
 * context mismatch. Invalidate entry.
 */ 
#define KVCACHE_FOUND 0
#define KVCACHE_NOT_FOUND -1
#define KVCACHE_MD_ONLY 1

class OntapKVCacheMgr {
public:
	OntapKVCacheMgr() : OntapKVCacheLock("kvcachelock") {
		std::cout<<"CacheMgr: Starting\n";
		cache = new OntapKVCacheEntry*[CACHE_SIZE];
		for (int i = 0; i < CACHE_SIZE; i++) {
			cache[i] = NULL;
		}
		std::cout<<"CacheMgr: Starting end\n";
	}

	~OntapKVCacheMgr() { std::cout<<"CacheMgr: Bye Bye\n"; }

	int64_t generateKey(int32_t cont_id, RecordId id) {
		return (((int64_t) cont_id) + id.repr());
	}

	int generateHashKey(int64_t key) {
		return ((key * 11400714819323198549ul) >> (64 - h_bits));
	}

	int lookup(OperationContext *txn, int32_t container,
		   const RecordId& id, kv_storage_hint_t *hint,
		   RecordData *out);
	bool insert(OperationContext *txn, int32_t container,
		const char *data, kv_storage_hint_t hint,
		int len, RecordId id);
	bool update(OperationContext *txn, int32_t container, const char *data, 
		kv_storage_hint_t hint, int len,
		RecordId id);
	bool invalidate(OperationContext *txn, int32_t container, RecordId id);
private:
	OntapKVCacheEntry **cache;
	RWLock OntapKVCacheLock;
};

}//
