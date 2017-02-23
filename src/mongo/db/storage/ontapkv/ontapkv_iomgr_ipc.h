
#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/storage/capped_callback.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/concurrency/synchronization.h"
#include "mongo/util/concurrency/rwlock.h"
#include "mongo/util/fail_point_service.h"

#include "mongo/db/storage/ontapkv/ontapkv_cachemgr.h"
#include "mongo/db/storage/ontapkv/ontapkv_iomgr.h"
#pragma once

namespace mongo {
#define MAX_CONN 20

class IPCConnection {
public:
        IPCConnection(const char *addr, int port) :
                         _addr(addr), _port(port) {
                getConnection();
        }
        ~IPCConnection() { }
        void getConnection();
        void sendRequest(const char *buf, int len);
        int recvResponse(char *buf, int len);
        int getSock() {
                return _sock;
        }
private:
        const char *_addr;
        int _port;
        int _sock;

};

class IPCConnectionCache {
public:
        IPCConnectionCache(const char *addr, int port) : IPCConnectionCacheLock("IPC Connection Cache Lock") {
                conn = new IPCConnection*[MAX_CONN];
                for (int i = 0; i < MAX_CONN; i++) {
                        conn[i] = new IPCConnection(addr, port);
                        in_use[i] = 0;
                }
        }
        ~IPCConnectionCache() {
                for (int i = 0; i < MAX_CONN; i++) {
			std::cout << "ConnectionCache destructor" << std::endl;
                        close(conn[i]->getSock());
                }
        }
        IPCConnection *getConnection(int *conn_no) {
		int i;
		IPCConnectionCacheLock.lock();
                for (i = 0; i < MAX_CONN; i++) {
                        if (in_use[i] == 0) {
                                break;
                        }
                }
                invariant(i < MAX_CONN);
                in_use[i] = 1;
                *conn_no = i;
		IPCConnectionCacheLock.unlock();
                return conn[i];
        }

        void dropConnection(int i) {
		IPCConnectionCacheLock.lock();
                in_use[i] = 0;
		IPCConnectionCacheLock.unlock();
        }
private:
        IPCConnection **conn;
        int in_use[MAX_CONN];
	RWLock IPCConnectionCacheLock;

};

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
       OntapKVIOMgrIPC(int64_t rsid, OntapKVCacheMgr *cachemgr, IPCConnectionCache *_conn_cache) : _rsid(rsid<<32) {
		 std::cout << "IOMgrIPC Constructor\n";
		_nextIdNum.store(_rsid | 0);
		cacheMgr = cachemgr;
		conn_cache = _conn_cache;
       }
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
//                     OntapKVIOMgr *mgr,
                       bool forward);

private:
#if 0
       /* Map from RecordId->repr to RecordData
        * is out KV store*/
       typedef std::map<int64_t, RecordData> Records;
       Records _kvmap;
       AtomicInt64 _nextIdNum;

       RecordId getNextRecordId();
       RecordId _fromKey(int64_t k);
       int64_t _makeKey(const RecordId& id);
       void _increaseDataSize(
               OperationContext *txn, int64_t amount);
       void _changeNumRecords(OperationContext *txn, int64_t diff);
#endif

       int64_t getRsid() { return _rsid; }
       OntapKVCacheMgr *getCacheMgr() { return cacheMgr; }
       AtomicInt64 _nextIdNum;
       int64_t  _rsid;
       OntapKVCacheMgr *cacheMgr;
       IPCConnectionCache *conn_cache;

       int64_t getNextRecordId();
       friend class OntapKVIteratorIPC;
};
}// mongo
