#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/base/checked_cast.h"
#include "mongo/db/json.h"
#include "mongo/db/catalog/index_catalog_entry.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/key_string.h"

#include "mongo/db/storage/storage_options.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/hex.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/db/storage/ontapkv/ontapkv_index.h"

namespace mongo {
namespace {
bool hasFieldNames(const BSONObj& obj) {
    BSONForEach(e, obj) {
        if (e.fieldName()[0])
            return true;
    }
    return false;
}

BSONObj stripFieldNames(const BSONObj& query) {
    if (!hasFieldNames(query))
        return query;

    BSONObjBuilder bb;
    BSONForEach(e, query) {
        bb.appendAs(e, StringData());
    }
    return bb.obj();
}
} //namespace

class OntapKVBulkBuilderInterface : public SortedDataBuilderInterface {
    MONGO_DISALLOW_COPYING(OntapKVBulkBuilderInterface);

public:
    OntapKVBulkBuilderInterface() {}

    virtual Status addKey(const BSONObj& key, const RecordId& loc) {
	invariant(0);
	return Status::OK();

    }
};

SortedDataBuilderInterface*
OntapKVIndex::getBulkBuilder(OperationContext* txn,
				bool dupsAllowed) {
	return new OntapKVBulkBuilderInterface();
}

class OntapKVIndexCursor final : public SortedDataInterface::Cursor {
public:
	OntapKVIndexCursor(OperationContext *txn,
			const OntapKVIndex *index,
			bool isForward) 
		: _txn(txn),
		_index(index),
		_forward(isForward),
		_curr(),
		_endPosition(),
		endPositionSet(false),
		_eof(false) {}

	~OntapKVIndexCursor() {}

	void setEndPosition(const BSONObj& key, bool inclusive) {
		if (key.isEmpty()) {
			_eof = true;
			return;
		} else {
			const BSONObj finalkey = stripFieldNames(key);
    			std::multimap<const BSONObj, RecordId>::const_iterator it ;
    			std::multimap<const BSONObj, RecordId>::const_iterator itend ;
    			std::multimap<const BSONObj, RecordId>::const_iterator itr ;
    			std::multimap<const BSONObj, RecordId>::const_iterator prev_itr ;
			int elem_count = 0;
			it = _index->_kvindex.find(finalkey);
			itend = _index->_kvindex.end();
			if (it == itend) {
				// No exact match. point to the key which is greater than the key
				// passed to the function. In case of all the key is exhausted, point
				// to the last key.
				for (itr = _index->_kvindex.begin(); itr != _index->_kvindex.end(); ++itr) {
					int cmp = finalkey.woCompare(itr->first, _index->_ordering, true);
					if (cmp <= 0) {
						break;
					}
					prev_itr = itr;
					++elem_count;
				}
				if (elem_count == 0) {
					// endkey is lesser than kvindex.begin()
					_eof = true;
				} else {
					_eof = false;
					_curr = prev_itr->first;
					_endPosition = prev_itr->first;
					endPositionSet = true;
				}
			} else {
				_eof = false;
				_curr = it->first;
				_endPosition = it->first;
				endPositionSet = true;
			}
		}
	}
#if 0
	{
    		std::multimap<const BSONObj, RecordId>::const_iterator it ;
		kk
		it = _index->_kvindex.find(key);
		if (_forward) {
			_curr = _index->_kvindex.end()->first;
		} else {
			_curr = _index->_kvindex.begin()->first;
		}
	}
#endif

        boost::optional<IndexKeyEntry> next(RequestedInfo parts = kKeyAndLoc) { 
		if (_eof) {
			return boost::none;
		}
    	std::multimap<const BSONObj, RecordId>::const_iterator it;
    	std::multimap<const BSONObj, RecordId>::const_iterator itend;
	if (_forward) {
		it = _index->_kvindex.lower_bound(_curr);
		itend = _index->_kvindex.find(_endPosition);

		if (it != itend) {
			++it;
			IndexKeyEntry ent(it->first, it->second);
			if (it != itend) {
				_curr = it->first;
			} else {
				_eof = true;
			}
			return ent;
		} else {
			_eof = true;
			BSONObj tmp;
			_curr = tmp;
			return boost::none;
		}
	} else {
		it = _index->_kvindex.upper_bound(_curr);
		itend = _index->_kvindex.find(_endPosition);
		if (it != itend) {
			++it;
			IndexKeyEntry ent(it->first, it->second);
			if (it != itend) {
				_curr = it->first;
			} else {
				_eof = true;
			}
			return ent;
		} else {
			_eof = true;
			BSONObj tmp;
			_curr = tmp;
			return boost::none;
		}
	}

	}

        boost::optional<IndexKeyEntry> seek(const BSONObj& key,
                                            bool inclusive,
                             RequestedInfo parts = kKeyAndLoc) {
			
    		std::multimap<const BSONObj, RecordId>::const_iterator it ;
    		std::multimap<const BSONObj, RecordId>::const_iterator itr ;
		const BSONObj finalkey = stripFieldNames(key);
		StringBuilder sb_str;
		sb_str << "Seeking key:";
		sb_str << finalkey;
		std::cout << sb_str.str() << '\n';
		it = _index->_kvindex.lower_bound(finalkey);
		if (endPositionSet) {
			int cmp = finalkey.woCompare(_endPosition, _index->_ordering, true);  
		
			if (_forward ? (cmp <= 0) : (cmp >= 0)) {
				_curr = it->first;
				return IndexKeyEntry(it->first, it->second);
			}
		} else {
			if (it != _index->_kvindex.end()) {
				_curr = it->first;
				return IndexKeyEntry(it->first, it->second);
			}
		}
                                
		return boost::none;
	}

        boost::optional<IndexKeyEntry> seek(const IndexSeekPoint& seekPoint,
                                                    RequestedInfo parts = kKeyAndLoc) {

		BSONObj key = IndexEntryComparison::makeQueryObject(seekPoint, _forward);
		return seek(key, true, parts);
	}

    void save() final {}
    void restore() final {}
    void detachFromOperationContext() final {}
    void reattachToOperationContext(OperationContext* txn) final {}

private:
	OperationContext *_txn;
	const OntapKVIndex *_index;
	const bool _forward;
	BSONObj _curr;
	BSONObj _endPosition;
	bool endPositionSet;
	bool _eof;
};

std::unique_ptr<SortedDataInterface::Cursor>
OntapKVIndex::newCursor(OperationContext* txn, bool isForward) const {
        return stdx::make_unique<OntapKVIndexCursor>(txn, this, isForward);
}

OntapKVIndex::OntapKVIndex(OperationContext* ctx,
			const std::string& uri,
			const IndexDescriptor* desc) 
    : _ordering(Ordering::make(desc->keyPattern())),
      _uri(uri),
      _collectionNamespace(desc->parentNS()),
      _indexName(desc->indexName()) {

}

Status OntapKVIndex::initAsEmpty(OperationContext* txn) {
	return Status::OK();
}

Status OntapKVIndex::insert(OperationContext* txn,
                          const BSONObj& key,
                          const RecordId& id,
                          bool dupsAllowed) {
	const BSONObj finalkey = stripFieldNames(key);
	if (!dupsAllowed) {
		indexMap::const_iterator it = _kvindex.find(finalkey);
		if (it != _kvindex.end()) {
			StringBuilder sb;
			sb << "DUP key in (col, finalkey):";
			sb << _collectionNamespace;
			sb << key;
			return Status(ErrorCodes::DuplicateKey, sb.str());
		}
	}
	StringBuilder sb_str;
	sb_str << "Inserting key:";
	sb_str << finalkey;
	std::cout << sb_str.str() << '\n';
	_kvindex.insert(std::pair<const BSONObj,RecordId>(finalkey, id));
	return Status::OK(); 
}
void OntapKVIndex::unindex(OperationContext* txn,
                         const BSONObj& key,
                         const RecordId& id,
                         bool dupsAllowed) {
	std::cout <<" Someone unindexing\n";
	StringBuilder sb_str;
	sb_str << "deleting key:";
	sb_str << key;
	std::cout << sb_str.str() << '\n';
	indexMap::iterator it = _kvindex.find(stripFieldNames(key));
	_kvindex.erase(it);
}

void OntapKVIndex::fullValidate(OperationContext* txn,
                              bool full,
                              long long* numKeysOut,
                              BSONObjBuilder* output) const {

	*numKeysOut = _kvindex.size();
}

bool OntapKVIndex::appendCustomStats(OperationContext* txn,
                                   BSONObjBuilder* output,
                                   double scale) const {
	BSONObjBuilder md;

	md.append("dummy", 0);
	return true;
}

Status OntapKVIndex::dupKeyCheck(OperationContext* txn,
				const BSONObj& key,
				const RecordId& id) {
	return Status::OK();
}

bool OntapKVIndex::isEmpty(OperationContext* txn) {
	return _kvindex.empty();
}

Status OntapKVIndex::touch(OperationContext* txn) const {
	return Status::OK();
}

long long OntapKVIndex::getSpaceUsedBytes(OperationContext* txn) const {
#define DEFAULT_KEY_SIZE (64)
	return (_kvindex.size() * (sizeof(int64_t) + DEFAULT_KEY_SIZE));
}

} //namespace mongo
