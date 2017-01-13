#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/storage/capped_callback.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/concurrency/synchronization.h"
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
class StorageContext {
public:
	int size() {return sizeof(_bytes); }
private:
	char _bytes[16];
};

class OntapKVCacheEntry {
public:
	bool hasData(void) { return _data != NULL;}
	kv_storage_hint_t getMetadata(void) {return hint;}

private:
	std::string _container;
	RecordId _id; // Most likely not needed
	void *_data;
	kv_storage_hint_t hint;
};

/*
 * Lookup may return:
 * none. Nothing in cache.
 * mdonly. Metadata found in cache.
 * data. Lookup got everything.
 */ 
#define KVCACHE_FOUND 0
#define KVCACHE_NOT_FOUND -1
#define KVCACHE_MD_ONLY 1

class OntapKVCacheMgr {
public:
//	OntapKVCacheMgr() { std::cout<<"CacheMgr: Hello \n"; }
	~OntapKVCacheMgr() { std::cout<<"CacheMgr: Bye Bye\n"; }
	int lookup(OperationContext *txn, std::string container,
		   const RecordId&, kv_storage_hint_t *hint,
		   RecordData *out);
	bool insert(OperationContext *txn, std::string container,
		const char *data,
		int len, RecordId id);
	bool update(OperationContext *txn, std::string container, const char *data, int len,
		RecordId id);
	bool invalidate(OperationContext *txn, std::string container, RecordId id);
};

}//
