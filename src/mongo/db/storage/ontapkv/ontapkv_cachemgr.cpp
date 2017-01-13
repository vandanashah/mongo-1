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
int OntapKVCacheMgr::lookup(OperationContext *txn, int32_t container,
		   const RecordId& id, StorageContext *cxt,
		   RecordData *out) {
	int64_t key = generateKey(container, id);
	int index = generateHashKey(key);
	invariant(index < CACHE_SIZE);

	if (cache[index] == NULL) {
		return KVCACHE_NOT_FOUND;
	} else {
		OntapKVCacheEntry *entry = cache[index];
		while ((entry != NULL) && (entry->getKey() != key)) {
			entry = entry->getNext();
		}
		if (entry == NULL) {
			return KVCACHE_NOT_FOUND;
		} else {
			/*
			 * Case 1: Both data and metadata is found
			 * Case 2: Only metadata found
			 * Case 3: Data context doesn't match. Invalidate entry
			 */	
			char *data = (char *) entry->getData();
			StorageUberContext metadata = entry->getMetadata();
			DataContext data_ctx = metadata.getContext();
			StorageContext storage_ctx = metadata.getHint();
			if (storage_ctx.size() != data_ctx.getContext()) {
				// Invalidate
				invalidate(txn, container, id);
				return KVCACHE_NOT_FOUND;
			} else {
				// Copy metadata into cxt pointer
				memcpy(cxt, &storage_ctx, storage_ctx.size());
				if (data == NULL) {
					return KVCACHE_MD_ONLY;
				} else {
					int size = entry->getDataSize();
					SharedBuffer buf_data = SharedBuffer::allocate(size);
					memcpy(buf_data.get(), data, size);
					*out = RecordData(buf_data, size);
					return KVCACHE_FOUND;
				}
			}	
		}
	}
}

bool OntapKVCacheMgr::insert(OperationContext *txn, int32_t container,
		const char *data, StorageContext cxt,
		int len, RecordId id) {
	int64_t key = generateKey(container, id);
	int index = generateHashKey(key);
	invariant(index < CACHE_SIZE);

	if (cache[index] == NULL) {
		cache[index] = new OntapKVCacheEntry(container, id, cxt, (void *) data, len, NULL);
	} else {
		OntapKVCacheEntry *entry = cache[index];
		while ((entry->getNext() != NULL) && (entry->getKey() != key)) {
			entry = entry->getNext();
		}
		if (entry->getKey() != key) {
			// Not found in hash but last entry in coalesced chain is reached. Insert 
			entry->setNext(new OntapKVCacheEntry(container, id, cxt, (void *) data, len, NULL));
		} else {
			// Entry already exists. Update
			return update(txn, container, data, cxt, len, id);
		}
	}
	return true;
}

bool OntapKVCacheMgr::update(OperationContext *txn, int32_t container, const char *data,
		StorageContext cxt, int len, RecordId id) {
	int64_t key = generateKey(container, id);
	int index = generateHashKey(key);
	invariant(index < CACHE_SIZE);

	if (cache[index] == NULL) {
		// Should have called insert
		return false;
	} else {
		OntapKVCacheEntry *entry = cache[index];
		while ((entry->getNext() != NULL) && (entry->getKey() != key)) {
			entry = entry->getNext();
		}
		if (entry->getKey() != key) {
			// Should have called insert
			return false;
		} else {
			// Entry already exists. Update
			char *olddata = (char *) entry->getData();
			free(olddata);
			entry->setData((void *)data, len);
			entry->setDataSize(len);
			entry->setMetadata(cxt);
		} 
	}
	return true;
}

bool OntapKVCacheMgr::invalidate(OperationContext *txn, int32_t container, RecordId id) {
	int64_t key = generateKey(container, id);
	int index = generateHashKey(key);
	invariant(index < CACHE_SIZE);

	if (cache[index] != NULL) {
		OntapKVCacheEntry *prev_entry = NULL;
		OntapKVCacheEntry *entry = cache[index];
		while ((entry->getNext() != NULL) && (entry->getKey() != key)) {
			prev_entry = entry;
			entry = entry->getNext();
		}
		if (entry->getKey() == key) {
			OntapKVCacheEntry *next_entry = entry->getNext();
			delete entry;
			if (prev_entry == NULL) {
				// Very first entry
				cache[index] = next_entry;
			} else {
				prev_entry->setNext(next_entry);
			}
		}
			
        }
	return true;
}

}//namespace mongo
