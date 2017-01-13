
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
#include "mongo/db/storage/ontapkv/ontapkv_iomgr_mock.h"
#include <climits>
namespace mongo {

const int64_t firstRecordId = 1;
OntapKVIterator_mock::OntapKVIterator_mock(OperationContext *txn,
			OntapKVIOMgr_mock *iomgr,
			bool forward) : 
	OntapKVIterator(txn, forward),
	_iomgr(iomgr),
	_curr(0) {}

boost::optional<Record>
OntapKVIterator_mock::seekExact(const RecordId& id) {
	int64_t key = _iomgr->_makeKey(id);
	RecordData rd;
	std::map<int64_t, RecordData>::const_iterator it = _iomgr->_kvmap.find(key);

	if (it == _iomgr->_kvmap.end()) {
		return {};
	}
	return {{_iomgr->_fromKey(it->first), 
		_iomgr->dataFor(_txn, _iomgr->_fromKey(it->first))}};
}

/*
 * keep the id from where to move.
 * On call to next,
 * If it is forward going (lower to higher) use lower_bound iterator
 * else use upper bound iterator
*/
boost::optional<Record> OntapKVIterator_mock::next() {
	std::map<int64_t, RecordData>::const_iterator iter;	
	int64_t searchKey;
	//switch (_forward) {
	if (_forward) {
		//case true:
		if (_curr == LLONG_MAX) {
			return boost::none;
		}
		searchKey = _curr == 0 ? firstRecordId : _curr;
		iter = _iomgr->_kvmap.lower_bound(searchKey);
		if (iter != _iomgr->_kvmap.end()) {
			Record r = {_iomgr->_fromKey(iter->first),
				RecordData(iter->second.data(), iter->second.size())};
			if (++iter == _iomgr->_kvmap.end()) {
				_curr = LLONG_MAX;
			} else {
				_curr = iter->first;
			}
			return r;
		} else {
			_curr = LLONG_MAX;
			return boost::none;
		}
		//break;
	} else { 
		//case false:
		if (_curr == LLONG_MIN) {
			return boost::none;
		}
		searchKey = _curr == 0 ? _iomgr->_nextIdNum.load() : _curr;
		iter = _iomgr->_kvmap.upper_bound(searchKey);
		if (iter != _iomgr->_kvmap.begin()) {
			Record r = {_iomgr->_fromKey(iter->first),
				RecordData(iter->second.data(), iter->second.size())};
			if (--iter == _iomgr->_kvmap.begin()) {
				_curr = LLONG_MIN;
			} else {
				_curr = iter->first;
			}
			return r;
		} else {
			_curr = LLONG_MIN;
			return boost::none;
		}
	//	break;

	//	default:
	//	invariant (0);
	}
	return boost::none;
}

#if 0
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
}

OntapKVRecordStore::~OntapKVRecordStore() {
	std::cout<< "Bye bye from OntapKVRecordStore\n";
}
#endif

OntapKVIOMgr_mock::OntapKVIOMgr_mock(int64_t rsid) :
	OntapKVIOMgr() {
		std::cout << "IOMgr begins \n";
		_nextIdNum.store((rsid << 32) | 1);
	}


RecordId OntapKVIOMgr_mock::getNextRecordId()
{
	RecordId out = RecordId(_nextIdNum.fetchAndAdd(1));
	invariant(out.isNormal());
	return out;
}

StatusWith<RecordId> 
OntapKVIOMgr_mock::writeRecord(OperationContext* txn,
				std::string contid,
				const char* data,
				int len,
				void *storageContext)
{
	RecordId id = getNextRecordId();
	//_changeNumRecords(txn, 1);
	//_increaseDataSize(txn, len);
	std::cout <<"Insert in RecStore:"<<
		"AT id:"<<id.repr()<<"\n";
	/* Make sure to Copy */
	RecordData rd = RecordData(data, len);
	RecordData rd1 = rd.getOwned();
	_kvmap[id.repr()] = rd1;
	return StatusWith<RecordId>(id);
}

RecordId OntapKVIOMgr_mock::_fromKey(int64_t k) {
	return RecordId(k);
}

int64_t OntapKVIOMgr_mock::_makeKey(const RecordId& id) {
	    return id.repr();
}

RecordData OntapKVIOMgr_mock::dataFor(
		OperationContext* txn,
		const RecordId& id) {
	RecordData rd;
	std::string nocontainer;
	void *sto_ctx;
	readRecord(txn, nocontainer, id, sto_ctx, &rd);
	return rd;
}

bool OntapKVIOMgr_mock::readRecord(OperationContext* txn,
				    std::string contid,
					const RecordId& id,
					void *storageContext,
					RecordData* out) {
	int64_t key = _makeKey(id);
	Records::const_iterator it = _kvmap.find(key);

	if (it == _kvmap.end()) {
		std::cout <<"readRecord in RecStore:"<<
		"AT id:"<<"EMPTY REC\n";
		return false;
	}
	std::cout <<"readRecord in RecStore:"<<
		"AT id:"<<id.repr()<<"\n";
	SharedBuffer data = SharedBuffer::allocate(it->second.size());
	memcpy(data.get(), it->second.data(), it->second.size());
	*out = RecordData(data, it->second.size());
	return true;
}

#if 0
void OntapKVIOMgr_mock::_increaseDataSize(
			OperationContext *txn, int64_t amount) {
	 if (_dataSize.fetchAndAdd(amount) < 0) { 
		 _dataSize.store(std::max(amount, int64_t(0)));
	 }
}

void OntapKVIOMgr_mock::_changeNumRecords(OperationContext *txn, int64_t diff) {
	 if (_numRecords.fetchAndAdd(diff) < 0) {
		 _numRecords.store(std::max(diff, int64_t(0)));
	 }
}

const char* OntapKVIOMgr_mock::name() const {
	return _engineName.c_str();
}

long long OntapKVIOMgr_mock::dataSize(OperationContext* txn) const {
	return _dataSize.load();
}

long long OntapKVIOMgr_mock::numRecords(OperationContext* txn) const {
	return _numRecords.load();
}

bool OntapKVIOMgr_mock::isCapped() const {
	return _isCapped;
}

int64_t OntapKVIOMgr_mock::storageSize(OperationContext* txn,
                                           BSONObjBuilder* extraInfo,
                                           int infoLevel) const {
        return dataSize(txn);
}

#endif

void OntapKVIOMgr_mock::deleteRecord(
				OperationContext* txn,
				std::string contid,
				const RecordId &id) {
	
	int64_t key = _makeKey(id);
	RecordData rd = _kvmap[key];

	if (rd.size () == 0) {
		return; // Empty record
	}

	/* FIXME Free up the RecordData */
//	delete rd;

	_kvmap.erase(key);
//	_changeNumRecords(txn, -1);
//	_increaseDataSize(txn, -length);
}

#if 0
StatusWith<RecordId> 
OntapKVIOMgr_mock::insertRecord(OperationContext* txn,
				const DocWriter* doc,
				bool enforceQuota) {
    const int len = doc->documentSize();

    std::unique_ptr<char[]> buf(new char[len]);
    doc->writeDocument(buf.get());

    return insertRecord(txn, buf.get(), len, enforceQuota);
}
#endif

StatusWith<RecordId> 
OntapKVIOMgr_mock::updateRecord(OperationContext* txn,
				std::string contid,
				const RecordId& oldLocation,
				const char* data,
				int len,
		        	void *storageContext) {
	Records::const_iterator it = _kvmap.find(_makeKey(oldLocation));

	/* New record */
	if (it == _kvmap.end()) {
		return writeRecord(txn, contid, data, len, storageContext);
	}
	//const char *oldData = it->second.data();
	//_increaseDataSize(txn, len - oldLen);
	std::cout <<"Update in iomgr aT id:"<<
		oldLocation.repr()<<"\n";
	RecordData rd = RecordData(data, len);
	RecordData rd1 = rd.getOwned();
	_kvmap[oldLocation.repr()] = rd1;
	// Free old 
	//delete oldData;
	return StatusWith<RecordId>(oldLocation);
}

#if 0
bool OntapKVIOMgr_mock::updateWithDamagesSupported() const { 
	return false;
}

StatusWith<RecordData> 
OntapKVIOMgr_mock::updateWithDamages(
    OperationContext* txn,
    const RecordId& id,
    const RecordData& oldRec,
    const char* damageSource,
    const mutablebson::DamageVector& damages) {
    MONGO_UNREACHABLE;
}

std::unique_ptr<SeekableRecordCursor>
OntapKVIOMgr_mock::getCursor(
			OperationContext* txn,
			bool forward) const {
        return stdx::make_unique<OntapKVIterator>(txn, this, forward);
}

Status OntapKVIOMgr_mock::truncate(OperationContext* txn) {

	std::cout << "TBD\n";
	return Status::OK();
}
void OntapKVIOMgr_mock::temp_cappedTruncateAfter(OperationContext* txn,
		RecordId end,
		bool inclusive) {
	std::cout << "cappedTruncateAfter TBD\n";
}

Status OntapKVIOMgr_mock::validate(OperationContext* txn,
                                       bool full,
                                       bool scanData,
                                       ValidateAdaptor* adaptor,
                                       ValidateResults* results,
                                       BSONObjBuilder* output) {
	std::cout << "validate TBD\n";
	return Status::OK();
}

void OntapKVIOMgr_mock::appendCustomStats(OperationContext* txn,
                                              BSONObjBuilder* result,
                                              double scale) const {

	std::cout << "appendCustomStats TBD\n";
}
void OntapKVIOMgr_mock::updateStatsAfterRepair(OperationContext* txn,
                                        long long numRecords,
                                        long long dataSize) {
	_numRecords.store(numRecords);
	_dataSize.store(dataSize);
}
#endif

std::unique_ptr<SeekableRecordCursor> OntapKVIOMgr_mock::getIterator(
			OperationContext *txn,
			bool forward) {
        return stdx::make_unique<OntapKVIterator_mock>(txn, this, forward);
}
} //namespace mongo
