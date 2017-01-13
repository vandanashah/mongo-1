
#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/storage/capped_callback.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/concurrency/synchronization.h"
#include "mongo/util/fail_point_service.h"

#include "mongo/db/storage/ontapkv/ontapkv_iomgr.h"
#pragma once

namespace mongo {

#if 0
class OntapKVIOMgrIPC;
class OntapKVIterator_mock : public OntapKVIterator {
public:
	OntapKVIterator_mock(OperationContext *txn,
			OntapKVIOMgr_mock *iomgr,
			bool forward);
	~OntapKVIterator_mock() {}

    boost::optional<Record> next();
    boost::optional<Record> seekExact(const RecordId& id) ;

private:
	OntapKVIOMgr_mock *_iomgr;
    	int64_t _curr;
};

#endif
class OntapKVIOMgrIPC : public OntapKVIOMgr {
public:
	OntapKVIOMgrIPC() { std::cout << "IOMgrIPC Constructor\n";}
	/*
	 * Write record persistently.
	 * Input: Txn, container id, data and len
	 * Output: RecordId, StorageContext
	 */

	StatusWith<RecordId> writeRecord(OperationContext *txn,
			std::string contid,
			const char *data,
		        int len, kv_storage_hint_t *storageHint);
	StatusWith<RecordId> updateRecord(OperationContext *txn,
			std::string contid,
			const RecordId &id,
			const char *data,
		        int len, kv_storage_hint_t *storageHint);
	bool readRecord(OperationContext *txn,
		       std::string contid,
		       const RecordId &id,
		       kv_storage_hint_t *storageHint,
		       RecordData *out);
	void deleteRecord(OperationContext *txn,
		       	  std::string contid,
			  const RecordId &id);
	RecordData dataFor(
		OperationContext* txn,
		const RecordId& id); 
	std::unique_ptr<SeekableRecordCursor> getIterator(
			OperationContext *txn,
//			OntapKVIOMgr *mgr,
			bool forward); 
private:
#if 0
	/* Map from RecordId->repr to RecordData
	 * is out KV store*/
	typedef	std::map<int64_t, RecordData> Records; 
	Records _kvmap;
	AtomicInt64 _nextIdNum;

	RecordId getNextRecordId();
	RecordId _fromKey(int64_t k); 
	int64_t _makeKey(const RecordId& id); 
	void _increaseDataSize(
		OperationContext *txn, int64_t amount); 
	void _changeNumRecords(OperationContext *txn, int64_t diff); 
#endif

	friend class OntapKVIterator_mock;
};

}//mongo
