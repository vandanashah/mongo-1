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
#include "mongo/db/storage/ontapkv/ontapkv_histogram.h"


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

	long long getSize() {
		return (sizeof(int32_t) + sizeof(RecordId) + data_len + sizeof(StorageUberContext) + sizeof(OntapKVCacheEntry *)); 
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
	OntapKVCacheMgr(const Histogram::Options& opts) : OntapKVCacheLock("kvcachelock"),
			    lookup_hist( opts ),
			    write_hist( opts ),
			    update_hist( opts ),
			    cache_size(0),
			    min_lookup_latency(0),
			    max_lookup_latency(0),
			    total_lookup_latency(0),
			    numLookups(0),
			    min_write_latency(0),
			    max_write_latency(0),
			    total_write_latency(0),
			    numWrites(0),
			    min_update_latency(0),
			    max_update_latency(0),
			    total_update_latency(0),
			    numUpdates(0) {
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

	long long getCacheSize() {
		return (cache_size + sizeof(RWLock) + (3 * sizeof(Histogram)) + (3 * sizeof(int64_t)) + (10 * sizeof(long long)));
	}

	long long getMinLookupLatency() {
		return min_lookup_latency;
	}

	long long getMaxLookupLatency() {
		return max_lookup_latency;
	}

	long long getAvgLookupLatency() {
		return (total_lookup_latency/numLookups);
	}

	long long getMinWriteLatency() {
		return min_write_latency;
	}

	long long getMaxWriteLatency() {
		return max_write_latency;
	}

	long long getAvgWriteLatency() {
		return (total_write_latency/numWrites);
	}

	long long getMinUpdateLatency() {
		return min_update_latency;
	}

	long long getMaxUpdateLatency() {
		return max_update_latency;
	}

	long long getAvgUpdateLatency() {
		return (total_update_latency/numUpdates);
	}

	void setLookupLatency(long long latency) {
		if (this->max_lookup_latency == 0) {
			this->min_lookup_latency = latency;
			this->max_lookup_latency = latency;
		} else {
			this->min_lookup_latency = std::min(this->min_lookup_latency, latency);
			this->max_lookup_latency = std::max(this->max_lookup_latency, latency);
		}
		numLookups++;
		total_lookup_latency += latency;
	}
		
	void setWriteLatency(long long latency) {
		if (this->max_write_latency == 0) {
			this->min_write_latency = latency;
			this->max_write_latency = latency;
		} else {
			this->min_write_latency = std::min(this->min_write_latency, latency);
			this->max_write_latency = std::max(this->max_write_latency, latency);
		}
		numWrites++;
		total_write_latency += latency;
	}
		
	void setUpdateLatency(long long latency) {
		if (this->max_update_latency == 0) {
			this->min_update_latency = latency;
			this->max_update_latency = latency;
		} else {
			this->min_update_latency = std::min(this->min_update_latency, latency);
			this->max_update_latency = std::max(this->max_update_latency, latency);
		}
		numUpdates++;
		total_update_latency += latency;
	}

	Histogram& getLookupHistogram() {
		return lookup_hist;
	}

	Histogram& getWriteHistogram() {
		return write_hist;
	}

	Histogram& getUpdateHistogram() {
		return update_hist;
	}

	void setLookupHistogram(long long latency) {
		this->lookup_hist.insert(latency);
	}

	void setWriteHistogram(long long latency) {
		this->write_hist.insert(latency);
	}

	void setUpdateHistogram(long long latency) {
		this->update_hist.insert(latency);
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
	Histogram lookup_hist;
	Histogram write_hist;
	Histogram update_hist;
	/* cache size in bytes */
	long long cache_size;
	/* Latencies are in microseconds */ 
	long long min_lookup_latency;
	long long max_lookup_latency;
	long long total_lookup_latency;
	int64_t numLookups;
	long long min_write_latency;
	long long max_write_latency;
	long long total_write_latency;
	int64_t numWrites;
	long long min_update_latency;
	long long max_update_latency;
	long long total_update_latency;
	int64_t numUpdates; 
};

}//
