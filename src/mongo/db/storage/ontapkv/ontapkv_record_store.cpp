#include "mongo/base/checked_cast.h"
#include "mongo/bson/util/builder.h"
#include "mongo/db/concurrency/locker.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/oplog_hack.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/fail_point.h"
//#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/time_support.h"

#include "mongo/db/storage/ontapkv/ontapkv_record_store.h"
#include "mongo/db/storage/ontapkv/ontapkv_container_mgr.h"
#include "mongo/db/storage/ontapkv/ontapkv_cachemgr.h"
//#include "mongo/db/storage/ontapkv/ontapkv_iomgr.h"
#include "mongo/db/storage/ontapkv/ontapkv_iomgr_mock.h"
#include "mongo/db/storage/ontapkv/ontapkv_iomgr_ipc.h"
#include "mongo/db/storage/ontapkv/kv_format.h"
#include <climits>
namespace mongo {

OntapKVRecordStore::OntapKVRecordStore(OperationContext* txn,
                  StringData ns,
                  StringData uri,
                  std::string engineName,
                  bool isCapped,
                  bool isEphemeral,
		  OntapKVCacheMgr *cachemgr,
		  int64_t rsID,
                  int64_t cappedMaxSize,
                  int64_t cappedMaxDocs) :
	RecordStore(ns),
	_uri(uri.toString()),
	_engineName(engineName),
	_isCapped(isCapped),
	_isEphemeral(isEphemeral) {
	_nextIdNum.store((rsID << 32) | 1);
	_shuttingDown = false;
	contMgr = new OntapKVContainerMgr();
	//ioMgr = new OntapKVIOMgr_mock(rsID);
	cacheMgr = cachemgr;
	ioMgr = new OntapKVIOMgrIPC(rsID);
}

OntapKVRecordStore::~OntapKVRecordStore() {
	std::cout<< "Bye bye from OntapKVRecordStore\n";
}

/*
 * Not needed
RecordId OntapKVRecordStore::getNextRecordId()
{
	RecordId out = RecordId(_nextIdNum.fetchAndAdd(1));
	invariant(out.isNormal());
	return out;
}
 */ 
StatusWith<RecordId> 
OntapKVRecordStore::insertRecord(OperationContext* txn,
				const char* data,
				int len,
				bool enforceQuota)
{
	kv_storage_hint_t hint;

	std::string containerId = contMgr->getContainerId(_ns);
	/* ZERO copy ?? */
	StatusWith<RecordId> s = ioMgr->writeRecord(txn, containerId,
						    data, len,
							&hint);
	if (!s.isOK()) {
		/* FIXME */
		return Status(ErrorCodes::BadValue, "Update failed");
	}

	cacheMgr->insert(txn, std::stoi("1234"), data, hint,
				len, s.getValue());
	
	_changeNumRecords(txn, 1);
	_increaseDataSize(txn, len);
	return s;
}

#if 0
RecordId OntapKVRecordStore::_fromKey(int64_t k) {
	return RecordId(k);
}
#endif

RecordData OntapKVRecordStore::dataFor(
		OperationContext* txn,
		const RecordId& id) const {
	RecordData rd;
	findRecord(txn, id, &rd);
	return rd;
}

bool OntapKVRecordStore::findRecord(OperationContext* txn,
					const RecordId& id,
					RecordData* out) const {
	kv_storage_hint_t hint;
	RecordData rd;

	std::string containerId = contMgr->getContainerId(_ns);

	/* Copy data or give out reference ?*/
	int result = cacheMgr->lookup(txn, std::stoi("1234"), id,
			&hint, out);
	if (result == KVCACHE_FOUND) {
		invariant(out != NULL);
		return true;
	}
	if (result == KVCACHE_NOT_FOUND) {
		bzero(&hint, sizeof(hint));
	}
	return ioMgr->readRecord(txn, containerId, id, &hint, out);
}

void OntapKVRecordStore::_increaseDataSize(
			OperationContext *txn, int64_t amount) {
	 if (_dataSize.fetchAndAdd(amount) < 0) { 
		 _dataSize.store(std::max(amount, int64_t(0)));
	 }
}

void OntapKVRecordStore::_changeNumRecords(OperationContext *txn, int64_t diff) {
	 if (_numRecords.fetchAndAdd(diff) < 0) {
		 _numRecords.store(std::max(diff, int64_t(0)));
	 }
}

const char* OntapKVRecordStore::name() const {
	return _engineName.c_str();
}

long long OntapKVRecordStore::dataSize(OperationContext* txn) const {
	return _dataSize.load();
}

long long OntapKVRecordStore::numRecords(OperationContext* txn) const {
	return _numRecords.load();
}

bool OntapKVRecordStore::isCapped() const {
	return _isCapped;
}

int64_t OntapKVRecordStore::storageSize(OperationContext* txn,
                                           BSONObjBuilder* extraInfo,
                                           int infoLevel) const {
        return dataSize(txn);
}


void OntapKVRecordStore::deleteRecord(
				OperationContext* txn,
				const RecordId &id) {
	
	std::string containerId = contMgr->getContainerId(_ns);
	ioMgr->deleteRecord(txn, containerId, id);
	cacheMgr->invalidate(txn, std::stoi("1234"), id);
	_changeNumRecords(txn, -1);
#define LENGTH_BLAH 400
	int length = LENGTH_BLAH;
	_increaseDataSize(txn, -length);
}

StatusWith<RecordId> 
OntapKVRecordStore::insertRecord(OperationContext* txn,
				const DocWriter* doc,
				bool enforceQuota) {
    const int len = doc->documentSize();

    std::unique_ptr<char[]> buf(new char[len]);
    doc->writeDocument(buf.get());

    return insertRecord(txn, buf.get(), len, enforceQuota);
}

StatusWith<RecordId> 
OntapKVRecordStore::updateRecord(OperationContext* txn,
				const RecordId& oldLocation,
				const char* data,
				int len,
				bool enforceQuota,
				UpdateNotifier* notifier) {

	kv_storage_hint_t hint;

	std::string containerId = contMgr->getContainerId(_ns);
	/* ZERO copy ?? */
	StatusWith<RecordId> s = ioMgr->updateRecord(txn, containerId,
						    oldLocation, data, len,
							&hint);
	if (!s.isOK()) {
		/* FIXME */
		return Status(ErrorCodes::BadValue, "Update failed");
	}

	//cxt.setBytes((char *)"Context");
	/* For now assume this will work if record already exists */
	cacheMgr->insert(txn, std::stoi("1234"), data,
				hint, len, s.getValue());
	
	/* FIXME: adjust delta
	_increaseDataSize(txn, len - oldLen); */
	return s;
}

bool OntapKVRecordStore::updateWithDamagesSupported() const { 
	return false;
}

StatusWith<RecordData> 
OntapKVRecordStore::updateWithDamages(
    OperationContext* txn,
    const RecordId& id,
    const RecordData& oldRec,
    const char* damageSource,
    const mutablebson::DamageVector& damages) {
    MONGO_UNREACHABLE;
}

std::unique_ptr<SeekableRecordCursor>
OntapKVRecordStore::getCursor(
			OperationContext* txn,
			bool forward) const {
	return ioMgr->getIterator(txn, forward);
}

Status OntapKVRecordStore::truncate(OperationContext* txn) {

	invariant(0);
	std::cout << "TBD\n";
	return Status::OK();
}

void OntapKVRecordStore::temp_cappedTruncateAfter(OperationContext* txn,
		RecordId end,
		bool inclusive) {
	invariant(0);
	std::cout << "cappedTruncateAfter TBD\n";
}

Status OntapKVRecordStore::validate(OperationContext* txn,
                                       bool full,
                                       bool scanData,
                                       ValidateAdaptor* adaptor,
                                       ValidateResults* results,
                                       BSONObjBuilder* output) {
	std::cout << "validate TBD\n";
	return Status::OK();
}

void OntapKVRecordStore::appendCustomStats(OperationContext* txn,
                                              BSONObjBuilder* result,
                                              double scale) const {

	std::cout << "appendCustomStats TBD\n";
}
void OntapKVRecordStore::updateStatsAfterRepair(OperationContext* txn,
                                        long long numRecords,
                                        long long dataSize) {
	_numRecords.store(numRecords);
	_dataSize.store(dataSize);
}
} //namespace mongo
