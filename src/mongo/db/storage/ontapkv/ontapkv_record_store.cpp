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
#include <climits>
namespace mongo {

#if 0
const int64_t firstRecordId = 1;
class OntapKVIterator final : public SeekableRecordCursor {
public:
	OntapKVIterator(OperationContext *txn,
			const OntapKVRecordStore *rs,
			bool forward);
	~OntapKVIterator() {}

    boost::optional<Record> next() final;
    boost::optional<Record> seekExact(const RecordId& id) final;

    void save() final {}
    bool restore() final {
        return true;
    }
    void detachFromOperationContext() final {}
    void reattachToOperationContext(OperationContext* txn) final {}

private:
	OperationContext *_txn;
	const OntapKVRecordStore * const _rs;
	const bool _forward;
	int64_t _curr;
};

OntapKVIterator::OntapKVIterator(OperationContext *txn,
			const OntapKVRecordStore *rs,
			bool forward) :
	_txn(txn),
	_rs(rs),
	_forward(forward),
	_curr(0) {
	ioMgr->createIterator();	
	}


boost::optional<Record>
OntapKVIterator::seekExact(const RecordId& id) {
	RecordData rd;
	if (findRecord(txn, id, &rd) == false) {
		return {};
	}
	return {{id, rd}};
}

boost::optional<Record> OntapKVIterator::next() {
	_rs->ioMgr->getNext();
}
/*
 * keep the id from where to move.
 * On call to next,
 * If it is forward going (lower to higher) use lower_bound iterator
 * else use upper bound iterator
*/
boost::optional<Record> OntapKVIterator::next() {
	std::map<int64_t, RecordData>::const_iterator iter;	
	int64_t searchKey;
	switch (_forward) {
		case true:
		if (_curr == LLONG_MAX) {
			return boost::none;
		}
		searchKey = _curr == 0 ? firstRecordId : _curr;
		iter = _rs->_kvmap.lower_bound(searchKey);
		if (iter != _rs->_kvmap.end()) {
			Record r = {_rs->_fromKey(iter->first),
				RecordData(iter->second.data(), iter->second.size())};
			if (++iter == _rs->_kvmap.end()) {
				_curr = LLONG_MAX;
			} else {
				_curr = iter->first;
			}
			return r;
		} else {
			_curr = LLONG_MAX;
			return boost::none;
		}
		break;
		case false:
		if (_curr == LLONG_MIN) {
			return boost::none;
		}
		searchKey = _curr == 0 ? _rs->_nextIdNum.load() : _curr;
		iter = _rs->_kvmap.upper_bound(searchKey);
		if (iter != _rs->_kvmap.begin()) {
			Record r = {_rs->_fromKey(iter->first),
				RecordData(iter->second.data(), iter->second.size())};
			if (--iter == _rs->_kvmap.begin()) {
				_curr = LLONG_MIN;
			} else {
				_curr = iter->first;
			}
			return r;
		} else {
			_curr = LLONG_MIN;
			return boost::none;
		}
		break;

		default:
		invariant (0);
	}
	return boost::none;
}
#endif

OntapKVRecordStore::OntapKVRecordStore(OperationContext* txn,
                  StringData ns,
                  StringData uri,
                  std::string engineName,
                  bool isCapped,
                  bool isEphemeral,
                  int64_t cappedMaxSize,
                  int64_t cappedMaxDocs) :
	RecordStore(ns),
	_uri(uri.toString()),
	_engineName(engineName),
	_isCapped(isCapped),
	_isEphemeral(isEphemeral) {
	_nextIdNum.store(1);
	_shuttingDown = false;
	contMgr = new OntapKVContainerMgr();
	ioMgr = new OntapKVIOMgr_mock();
	cacheMgr = new OntapKVCacheMgr();
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
	void *storageContext;

	std::string containerId = contMgr->getContainerId(_ns);
	/* ZERO copy ?? */
	StatusWith<RecordId> s = ioMgr->writeRecord(txn, containerId,
						    data, len,
							storageContext);
	if (!s.isOK()) {
		/* FIXME */
		return Status(ErrorCodes::BadValue, "Update failed");
	}

	cacheMgr->insert(txn, containerId, data,
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
	StorageContext cxt;
	RecordData rd;

	std::string containerId = contMgr->getContainerId(_ns);

	/* Copy data or give out reference ?*/
	int result = cacheMgr->lookup(txn, containerId, id,
			&cxt, out);
	if (result == KVCACHE_FOUND) {
		invariant(out != NULL);
		return true;
	}
	if (result == KVCACHE_NOT_FOUND) {
		bzero(&cxt, sizeof(cxt));
	}
	return ioMgr->readRecord(txn, containerId, id, &cxt, out);
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
	cacheMgr->invalidate(txn, containerId, id);
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

	void *storageContext;

	std::string containerId = contMgr->getContainerId(_ns);
	/* ZERO copy ?? */
	StatusWith<RecordId> s = ioMgr->updateRecord(txn, containerId,
						    oldLocation, data, len,
							storageContext);
	if (!s.isOK()) {
		/* FIXME */
		return Status(ErrorCodes::BadValue, "Update failed");
	}

	/* For now assume this will work if record already exists */
	cacheMgr->insert(txn, containerId, data,
				len, s.getValue());
	
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
