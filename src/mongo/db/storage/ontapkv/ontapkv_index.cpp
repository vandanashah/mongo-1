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

#if 0
class OntapKVIndexCursor final : public Cursor {
public:
	OntapKVIndexCursor(OperationContext *txn,
			bool isForward) 
		: _txn(txn),
		_isForward(isForward)
	{}
	~OntapKVCursor() {}

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
	const bool _isForward;
	int64_t _curr;
};
#endif

std::unique_ptr<SortedDataInterface::Cursor>
OntapKVIndex::newCursor(OperationContext* txn, bool isForward) const {
	invariant(0);
        return {};
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
	if (!dupsAllowed) {
		indexMap::const_iterator it = _kvindex.find(key);
		if (it != _kvindex.end()) {
			StringBuilder sb;
			sb << "DUP key in (col, key):";
			sb << _collectionNamespace;
			sb << key;
			return Status(ErrorCodes::DuplicateKey, sb.str());
		}
	}
	_kvindex.insert(std::pair<const BSONObj,RecordId>(key, id));
	return Status::OK(); 
}
void OntapKVIndex::unindex(OperationContext* txn,
                         const BSONObj& key,
                         const RecordId& id,
                         bool dupsAllowed) {
	std::cout <<" Someone unindexing\n";
	invariant(0);
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
