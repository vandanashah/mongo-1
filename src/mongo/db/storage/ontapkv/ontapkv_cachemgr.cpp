#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/storage/capped_callback.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/concurrency/synchronization.h"
#include "mongo/util/fail_point_service.h"
#include <iostream>

#include "mongo/db/storage/ontapkv/ontapkv_cachemgr.h"

/*
 * To decide:
 * Data cache or metadata cache only:
 * May be both.
 * How to implement
 * Define a KVCacheEntry that could potentially contain both data
 * and metadata.
 */

namespace mongo {
int OntapKVCacheMgr::lookup(OperationContext *txn, std::string container,
		   const RecordId&, kv_storage_hint_t *hint,
		   RecordData *out) {
	return KVCACHE_NOT_FOUND;
}

bool OntapKVCacheMgr::insert(OperationContext *txn, std::string container,
		const char *data,
		int len, RecordId id) {
	return true;
}

bool OntapKVCacheMgr::update(OperationContext *txn, std::string container, const char *data, int len,
		RecordId id) {
	return true;
}

bool OntapKVCacheMgr::invalidate(OperationContext *txn, std::string container, RecordId id) {
	return true;
}

}//namespace mongo
