//define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/base/checked_cast.h"
#include "mongo/base/init.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/concurrency/ticketholder.h"
//#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/stacktrace.h"

#include "mongo/db/storage/ontapkv/ontapkv_recovery_unit.h"

namespace mongo {

OntapKVRecoveryUnit::OntapKVRecoveryUnit()
    : _inUnitOfWork(false),
      _active(false),
      _everStartedWrite(false) {
//	      std::cout << "New recovery unit\n";
}


OntapKVRecoveryUnit::~OntapKVRecoveryUnit() {
    invariant(!_inUnitOfWork);
//    std::cout << "Dying recovery unit\n";
    _abort();
}


void OntapKVRecoveryUnit::beginUnitOfWork(OperationContext* opCtx) {
   invariant(!_areWriteUnitOfWorksBanned);
    invariant(!_inUnitOfWork);
    _inUnitOfWork = true;
    _everStartedWrite = true;

}

void OntapKVRecoveryUnit::commitUnitOfWork() {
	 invariant(_inUnitOfWork);
    _inUnitOfWork = false;
    _commit();
}
    
void OntapKVRecoveryUnit::abortUnitOfWork() {
	invariant(_inUnitOfWork);
    _inUnitOfWork = false;
	_abort();
}

bool OntapKVRecoveryUnit::waitUntilDurable() {
	invariant(!_inUnitOfWork);
	return false;
}

void OntapKVRecoveryUnit::abandonSnapshot() {
	return;	
}

SnapshotId OntapKVRecoveryUnit::getSnapshotId() const {
	return SnapshotId();
}

void OntapKVRecoveryUnit::registerChange(Change* change) {
    invariant(_inUnitOfWork);
    //_changes.push_back(change);
}

void* OntapKVRecoveryUnit::writingPtr(void* data, size_t len) {
    // This API should not be used for anything other than the MMAP V1 storage engine
    MONGO_UNREACHABLE;
}

void OntapKVRecoveryUnit::_commit() {
	std::cout<< "recovery unit: committing\n";
}

void OntapKVRecoveryUnit::_abort() {
	//std::cout<< "recovery unit: aborting\n";
}

} //namespace mongo
