
/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include <queue>
#include <string>


#include "mongo/bson/ordering.h"
#include "mongo/db/storage/kv/kv_engine.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/elapsed_tracker.h"
#include "mongo/db/storage/ontapkv/ontapkv_kv_engine.h"
#include "mongo/db/storage/ontapkv/ontapkv_recovery_unit.h"
#include "mongo/db/storage/ontapkv/ontapkv_record_store.h"
#include "mongo/db/storage/ontapkv/ontapkv_index.h"
#include "mongo/db/storage/ontapkv/ontapkv_histogram.h"
//#include "mongo/db/storage/ontapkv/ontapkv_cachemgr.h"

namespace mongo {

OntapKVEngine::OntapKVEngine(const std::string& canonicalName,
                       const std::string& path,
                       const std::string& extraOpenOptions,
                       size_t cacheSizeGB,
                       bool durable,
                       bool ephemeral,
                       bool repair)
		 : _durable(durable),
		   _ephemeral(ephemeral) {

	invariant(repair == false);
	/* Use the cacheSize for kvcache*/
	std::cout <<" OntapKVEngine with name: , path:, cacheSize:"
			<<canonicalName
			<<path
			<<cacheSizeGB
			<<"\n";
	Histogram::Options opts;
	opts.numBuckets = 11;
        opts.bucketSize = 10;
	cacheMgr = new OntapKVCacheMgr(opts);
	_nextrsID.store(1);
}

OntapKVEngine::~OntapKVEngine() {
	std::cout<<"Bye Bye from OntapKVEngine\n";
}

int64_t OntapKVEngine::nextRecordStoreID() {
	return _nextrsID.fetchAndAdd(1);
}

RecoveryUnit* OntapKVEngine::newRecoveryUnit() {
	/* Instantiate the recovery unit class here please */
	return new OntapKVRecoveryUnit();

}
Status OntapKVEngine::createRecordStore(OperationContext* opCtx,
                                     StringData ns,
                                     StringData ident,
                                     const CollectionOptions& options) {

	OntapKVRecordStore *rs; 
	OntapKVStores::const_iterator it = _recordStores.find(ns.toString());
	if (it != _recordStores.end()) {
		invariant(0);
		std::cout<<"RecordStore for "<<ns.toString()<<"EXISTS\n";
		return Status::OK();
	}
	int64_t rsID = nextRecordStoreID();
	rs = new OntapKVRecordStore(opCtx, ns, ident, "OntapKVEngine", false, false, cacheMgr, rsID); 
	invariant(rs);
	_recordStores[ns.toString()] = rs;
	return Status::OK();
}

RecordStore *OntapKVEngine::getRecordStore(OperationContext *opCtx,
					StringData ns,
					StringData ident,
					const CollectionOptions& options) {

	OntapKVRecordStore *rs;
	OntapKVStores::const_iterator it = _recordStores.find(ns.toString());
	if (it == _recordStores.end()) {
		std::cout<<"No recordStore for "<<ns.toString()<<"\n";
		return NULL;
	}
	rs = it->second;
	return rs;
}

SortedDataInterface* 
OntapKVEngine::getSortedDataInterface(OperationContext* opCtx,
				StringData ident,
				const IndexDescriptor* desc) {
	return new OntapKVIndex(opCtx, ident.toString(), desc);
}

Status OntapKVEngine::createSortedDataInterface(OperationContext* opCtx,
                                             StringData ident,
                                             const IndexDescriptor* desc) {

	return Status::OK();
}

int64_t OntapKVEngine::getIdentSize(OperationContext* opCtx, StringData ident) {
	return 0;
}

Status OntapKVEngine::repairIdent(OperationContext* opCtx, StringData ident) {
	return Status::OK();
}

Status OntapKVEngine::dropIdent(OperationContext* opCtx, StringData ident) {
	return Status::OK();
}

bool OntapKVEngine::supportsDocLocking() const {
	return true;
}

bool OntapKVEngine::supportsDirectoryPerDB() const {
	return false;
}

bool OntapKVEngine::hasIdent(OperationContext* opCtx, StringData ident) const {
	return false;
}

std::vector<std::string> OntapKVEngine::getAllIdents(OperationContext* opCtx) const {
	std::vector<std::string> duh_vector;
	return duh_vector;
}

void OntapKVEngine::setJournalListener(JournalListener* jl) { }

void OntapKVEngine::cleanShutdown() {}
} // mongo
