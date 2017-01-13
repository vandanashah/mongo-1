#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/storage/capped_callback.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/concurrency/synchronization.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/db/storage/ontapkv/kv_format.h"

#pragma once

namespace mongo {

class OntapKVIterator : public SeekableRecordCursor {
public:
	OntapKVIterator(OperationContext *txn,
//			OntapKVIOMgr *iomgr,
			bool forward) :
	_txn(txn),
	_forward(forward) {}
	~OntapKVIterator() {}

    boost::optional<Record> next() = 0;
    boost::optional<Record> seekExact(const RecordId& id) = 0;

    void save() final {}
    bool restore() final {
        return true;
    }
    void detachFromOperationContext() final {}
    void reattachToOperationContext(OperationContext* txn) final {}

protected:
	OperationContext *_txn;
	const bool _forward;
};

class OntapKVIOMgr {
public:
	OntapKVIOMgr() { std::cout<<"IOMgr: Hello \n"; }
	virtual ~OntapKVIOMgr() { std::cout<<"IOMgr: Bye Bye\n"; }
	/*
	 * Write record persistently.
	 * Input: Txn, container id, data and len
	 * Output: RecordId, StorageContext
	 */

	virtual StatusWith<RecordId> writeRecord(OperationContext *txn,
			std::string contid,
			const char *data,
		        int len, kv_storage_hint_t *hint) = 0;
	virtual StatusWith<RecordId> updateRecord(OperationContext *txn,
			std::string contid,
			const RecordId &id,
			const char *data,
		        int len, kv_storage_hint_t *hint) = 0;
	virtual bool readRecord(OperationContext *txn,
		       std::string contid,
		       const RecordId &id,
		       kv_storage_hint_t *hint,
		       RecordData *out) = 0;
	virtual void deleteRecord(OperationContext *txn,
			std::string contid,
			  const RecordId &id) = 0;
	virtual std::unique_ptr<SeekableRecordCursor> getIterator(
			OperationContext *txn,
//			OntapKVIOMgr *mgr,
			bool forward) = 0;
};

}//mongo
