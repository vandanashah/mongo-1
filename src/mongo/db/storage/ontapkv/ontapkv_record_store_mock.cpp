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
#include <climits>
namespace mongo {

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
	_curr(0) {}
//	_curr = _forward ? firstRecordId : _rs->_nextIdNum.load();

boost::optional<Record>
OntapKVIterator::seekExact(const RecordId& id) {
	int64_t key = _rs->_makeKey(id);
	RecordData rd;
	std::map<int64_t, RecordData>::const_iterator it = _rs->_kvmap.find(key);

	if (it == _rs->_kvmap.end()) {
		return {};
	}
	return {{_rs->_fromKey(it->first), 
		_rs->dataFor(_txn, _rs->_fromKey(it->first))}};
//	return Record(_rs->_fromKey(it->first), RecordData(it->second.data(), it->second.size()));
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
# if 0
boost::optional<Record> OntapKVIterator::next() {
	std::map<int64_t, RecordData>::const_iterator iter;	
	if (_forward) {
		iter = _rs->_kvmap.lower_bound(_curr);
	} else {
		iter = _rs->_kvmap.upper_bound(_curr);
	}
	if (iter == _rs->_kvmap.end()) {
		return boost::none;//Record(0, RecordData());
	}
	Record r = {_rs->_fromKey(iter->first),
			RecordData(iter->second.data(), iter->second.size())};
	iter++;
	if (_forward && iter == _rs->kvmap.end()) {
	}
		_curr 
	if (_forward && ++iter)
	_curr = iter++->first;
	return r;
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
}

OntapKVRecordStore::~OntapKVRecordStore() {
	std::cout<< "Bye bye from OntapKVRecordStore\n";
}

RecordId OntapKVRecordStore::getNextRecordId()
{
	RecordId out = RecordId(_nextIdNum.fetchAndAdd(1));
	invariant(out.isNormal());
	return out;
}

StatusWith<RecordId> 
OntapKVRecordStore::insertRecord(OperationContext* txn,
				const char* data,
				int len,
				bool enforceQuota)
{
	RecordId id = getNextRecordId();
	_changeNumRecords(txn, 1);
	_increaseDataSize(txn, len);
	std::cout <<"Insert in RecStore:"<<
		_ns<<"AT id:"<<id.repr()<<"\n";
	/* Make sure to Copy */
	RecordData rd = RecordData(data, len);
	RecordData rd1 = rd.getOwned();
	_kvmap[id.repr()] = rd1;
	return StatusWith<RecordId>(id);
}

RecordId OntapKVRecordStore::_fromKey(int64_t k) {
	return RecordId(k);
}

int64_t OntapKVRecordStore::_makeKey(const RecordId& id) {
	    return id.repr();
}

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
	int64_t key = _makeKey(id);
//	RecordData rd;
	Records::const_iterator it = _kvmap.find(key);

	if (it == _kvmap.end()) {
		std::cout <<"findRecord in RecStore:"<<
		_ns<<"AT id:"<<"EMPTY REC\n";
		return false;
	}
	std::cout <<"findRecord in RecStore:"<<
		_ns<<"AT id:"<<id.repr()<<"\n";
	SharedBuffer data = SharedBuffer::allocate(it->second.size());
	memcpy(data.get(), it->second.data(), it->second.size());
//	rd = RecordData(it->second.data(), it->second.size());
//	rd.getOwned();
	*out = RecordData(data, it->second.size());
	return true;
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
	
	int64_t key = _makeKey(id);
	RecordData rd = _kvmap[key];

	if (rd.size () == 0) {
		return; // Empty record
	}

	int length = rd.size();
	/* FIXME Free up the RecordData */
//	delete rd;

	_kvmap.erase(key);
	_changeNumRecords(txn, -1);
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
	Records::const_iterator it = _kvmap.find(_makeKey(oldLocation));

	/* New record */
	if (it == _kvmap.end()) {
		return insertRecord(txn, data, len, enforceQuota);
	}
	int oldLen = it->second.size();
	//const char *oldData = it->second.data();
	_increaseDataSize(txn, len - oldLen);
	std::cout <<"Update in RecStore:"<<
		_ns<<"AT id:"<<oldLocation.repr()<<"\n";
	RecordData rd = RecordData(data, len);
	RecordData rd1 = rd.getOwned();
	_kvmap[oldLocation.repr()] = rd1;
	// Free old 
	//delete oldData;
	return StatusWith<RecordId>(oldLocation);
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
        return stdx::make_unique<OntapKVIterator>(txn, this, forward);
}

Status OntapKVRecordStore::truncate(OperationContext* txn) {

	std::cout << "TBD\n";
	return Status::OK();
}
void OntapKVRecordStore::temp_cappedTruncateAfter(OperationContext* txn,
		RecordId end,
		bool inclusive) {
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
