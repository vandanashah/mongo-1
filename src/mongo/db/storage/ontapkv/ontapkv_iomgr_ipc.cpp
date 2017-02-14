
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
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/time_support.h"

#include "mongo/db/storage/ontapkv/ontapkv_record_store.h"
#include "mongo/db/storage/ontapkv/ontapkv_iomgr_ipc.h"
#include "mongo/db/storage/ontapkv/kv_format.h"
#include <climits>
#include <sys/socket.h>
#include <uuid/uuid.h>
#include <arpa/inet.h>

namespace mongo {
#define SERVER "0.0.0.0"
//#define SERVER "10.140.44.215"
#define PORT 1919

/*
 * IPC client implementation
 * Keep a connection cache. For now a single connection
 * Or simply create a new connection on each request
 * Tear down when done
 * Need to implement an iterator on the backend with this or even
 * the other client
 */

class IPCConnection {
public:
	IPCConnection(const char *addr, int port) :
			 _addr(addr), _port(port) {
		getConnection();
	}
	~IPCConnection() { close(_sock);}
	void getConnection();
	void sendRequest(const char *buf, int len) {
		int result = send(_sock, buf, len, 0);
		invariant(result == len);
	}
	int recvResponse(char *buf, int len) {
		/* Header first */
		int result = recv(_sock, buf, len, 0);
		invariant(result == len);
		std::cout<<"GOT response"; return 0;
	}
private:
	const char *_addr;
	int _port;
	int _sock;

};

void IPCConnection::getConnection()
{
	struct sockaddr_in serverAddr;
	socklen_t addr_size;

	_sock = socket(PF_INET, SOCK_STREAM, 0);
	serverAddr.sin_family = AF_INET;
//	serverAddr.sin_addr.s_addr = inet_addr("10.140.44.201");
//	serverAddr.sin_port = htons(1919);
	serverAddr.sin_port = htons(_port);
	serverAddr.sin_addr.s_addr = inet_addr(_addr);
	memset(serverAddr.sin_zero, '\0', sizeof serverAddr.sin_zero);

	addr_size = sizeof(serverAddr);
	connect(_sock, (struct sockaddr *) &serverAddr, addr_size);
}

/*
 Need APIs to generate a given request.
 Constructors is one option. And let it return a req structure
 which is both header and payload and len
*/
class Request {
public:
	Request(kvreq_type_t req_type) : 
		_reqId(req_type),
		_buf(NULL),
		_len(0) {}

	~Request() { 
		if (_buf) {
			free(_buf);
		}
	}

	void preparePutOne(
		std::string contid,
		const char* data,
		int len,
		const RecordId &id);
	void prepareGetOne(std::string contid,
			const RecordId& id,
			kv_storage_hint_t *storageHint);
	void prepareDelOne(std::string contid,
			const RecordId &id) {}
	char *getBuf() { return _buf;}
	int getLen() { return _len;}
	int getRecordIdStr(const RecordId &id, std::string &idbuf) {
		if (id.isNull()) {
			return 0;
		}
		idbuf = std::to_string(id.repr());
		int idlen = idbuf.size();
		return idlen;
	}

private:
	kvreq_type_t _reqId;
	char *_buf;
	int _len;
};

class Response {
public:
	Response() : _buf(NULL), _len(0) {}

	~Response() { 
		if (_buf) {
			free(_buf);
		}
	}
	bool parsePutResponse(const char *buf,
				kv_storage_hint_t *hint,
				int64_t *recId);
	bool parseGetResponse(const char *buf,
				kv_storage_hint_t *hint,
				int *len); 
	char *getBuf() { return _buf;}
	int getLen() { return _len;}
private:
	char *_buf;
	int _len;
};

bool Response::parsePutResponse(const char *buf,
				kv_storage_hint_t *hint,
				int64_t *recId) {
	kvresp_put_one_t resp = *(kvresp_put_one_t *)buf;
	if (resp.kvresp_put != KV_NO_ERROR) {
		std::cout << "Put failed with"<<resp.kvresp_put<<"\n";
		return false;
	}
	*hint = resp.kvresp_put_hint;
//	*recId = resp.kvresp_record_id;	
	return true;
}

bool Response::parseGetResponse(const char *buf, kv_storage_hint_t *hint, int *len) {
	kvresp_get_one_t resp = *(kvresp_get_one_t *)buf;
	if (resp.kvresp_get != KV_NO_ERROR) {
		std::cout << "Get failed with"<<resp.kvresp_get<<"\n";
		return false;
	}
	*len = resp.kvresp_get_datalen;
//	*hint = resp.kvresp_get_hint;
	_len = *len;
	_buf = (char *)malloc(_len);
	return true;
}

void Request::preparePutOne(std::string contid,
			const char* data,
			int len,
			const RecordId &id) {
	kvreq_header_t kv_hdr;
	kvreq_put_one_t kv_put;
	std::string idBuf;

	kv_put.kvreq_put_keylen = getRecordIdStr(id, idBuf);
	_len = sizeof(kvreq_header_t) +
		sizeof(kvreq_put_one_t) + 
		+ kv_put.kvreq_put_keylen +len;
		
	_buf = (char *)malloc(_len);
	/* prepare header */
	kv_hdr.kvreq_magic = 0x4B56;
	kv_hdr.kvreq_flags = 0;
	kv_hdr.kvreq_type = _reqId;
	/* FIXME */
	kv_hdr.kvreq_container_id = -1;

	/* prepare key,data */
	kv_put.kvreq_put_datalen = len;
	kv_put.kvreq_put_txn_id = 0;

	memcpy(_buf, &kv_hdr, sizeof(kv_hdr));
	memcpy(_buf + sizeof(kv_hdr), &kv_put, sizeof(kv_put));

	/* Save line length */
	char *tmpbuf = _buf + sizeof(kv_hdr) + sizeof(kv_put);
	if (!id.isNull()) {
		memcpy(tmpbuf, (void *)idBuf.c_str(),
			kv_put.kvreq_put_keylen);
	}
	tmpbuf += kv_put.kvreq_put_keylen;
	memcpy(tmpbuf, data, len);
}

void Request::prepareGetOne(std::string contid,
			const RecordId& id,
			kv_storage_hint_t *storageHint) {
	kvreq_header_t kv_hdr;
	kvreq_get_one_t kv_get;
	std::string idBuf;

	bzero(&kv_get, sizeof(kvreq_get_one_t));
	kv_get.kvreq_get_keylen = getRecordIdStr(id, idBuf);
	_len = sizeof(kvreq_header_t) +
		sizeof(kvreq_get_one_t) +
		kv_get.kvreq_get_keylen;
		
	_buf = (char *)malloc(_len);
	/* prepare header */
	kv_hdr.kvreq_magic = 0x4B56;
	kv_hdr.kvreq_flags = 0;
	kv_hdr.kvreq_type = _reqId;
	/* FIXME */
	kv_hdr.kvreq_container_id = -1;
	if (storageHint) {
		kv_storage_hint_t *hint = (kv_storage_hint_t *)storageHint;
		kv_get.kvreq_get_hint = *hint;
	}
	memcpy(_buf, &kv_hdr, sizeof(kv_hdr));
	memcpy(_buf+sizeof(kv_hdr), &kv_get, sizeof(kv_get));
	memcpy(_buf+sizeof(kv_hdr) + sizeof(kv_get), idBuf.c_str(), kv_get.kvreq_get_keylen); 
}

int64_t OntapKVIOMgrIPC::getNextRecordId()
{
        return _nextIdNum.addAndFetch(1);
}

StatusWith<RecordId> 
OntapKVIOMgrIPC::writeRecord(OperationContext* txn,
				std::string contid,
				const char* data,
				int len,
				kv_storage_hint_t *storageHint) {

	IPCConnection conn(SERVER, PORT);
	Request req(KV_PUT_ONE);
	kvresp_put_one_t respHeader;
	int64_t recId;
	Response resp;
	
	
	recId = getNextRecordId();
	Timer t;
	req.preparePutOne(contid, data, len, RecordId(recId));
	conn.sendRequest(req.getBuf(), req.getLen());
	conn.recvResponse((char *)&respHeader, sizeof(respHeader));
	if (!resp.parsePutResponse((char *)&respHeader, storageHint, &recId)) {
		return Status(ErrorCodes::BadValue, "Put  failed");
	}
	std::cout << "Write record including protocol latency " << t.micros() << "us" << std::endl;
	return StatusWith<RecordId>(RecordId(recId));
}

bool OntapKVIOMgrIPC::readRecord(OperationContext* txn,
			 std::string contid,
			 const RecordId& id,
			 kv_storage_hint_t *storageHint,
			 RecordData* out) {
	IPCConnection conn(SERVER, PORT);
	Request req(KV_GET_ONE);
	Response resp;
	kvresp_get_one_t getResp;
	bool result;
	int len;
	
	Timer t;
	req.prepareGetOne(contid, id, storageHint);
	conn.sendRequest(req.getBuf(), req.getLen());

	/* header */
	conn.recvResponse((char *)&getResp, sizeof(getResp));
	result = resp.parseGetResponse((char *)&getResp, storageHint, &len);
	if (result == false) {
		return false;
	}
	conn.recvResponse(resp.getBuf(), resp.getLen());
	std::cout << "Read record including protocol latency " << t.micros() << "us" << std::endl;
	RecordData rd = RecordData(resp.getBuf(), resp.getLen());
	*out = rd.getOwned();
	/* FIXME: handle change in context */
	return true;
}

StatusWith<RecordId> 
OntapKVIOMgrIPC::updateRecord(OperationContext* txn,
				std::string contid,
				const RecordId& oldLocation,
				const char* data,
				int len,
		        	kv_storage_hint_t *storageHint) {
	IPCConnection conn(SERVER, PORT);
	Request req(KV_PUT_ONE);
	kvresp_put_one_t respHeader;
	int64_t recId;
	Response resp;
	
	req.preparePutOne(contid, data, len, oldLocation);
	conn.sendRequest(req.getBuf(), req.getLen());
	conn.recvResponse((char *)&respHeader, sizeof(respHeader));
	if (!resp.parsePutResponse((char *)&respHeader, storageHint, &recId)) {
		return Status(ErrorCodes::BadValue, "Put  failed");
	}
	return StatusWith<RecordId>(RecordId(oldLocation));
}

void OntapKVIOMgrIPC::deleteRecord(
				OperationContext* txn,
				std::string contid,
				const RecordId &id) {
	/*
	send delete to the other side
	*/
	IPCConnection conn(SERVER, PORT);
	Request req(KV_DEL_ONE);
	
	std::cout << "DEL on id:" <<id.repr()<<"\n";
	req.prepareDelOne(contid, id);
//	conn.sendRequest(req.getBuf(), req.getLen());
}

class OntapKVIteratorIPC : public OntapKVIterator {
public:
	OntapKVIteratorIPC(OperationContext *txn,
			OntapKVIOMgrIPC *iomgr,
			bool forward) :
	OntapKVIterator(txn, forward),
	_iomgr(iomgr) {
		if (_forward) {
			_curr = _iomgr->getRsid() + 1;
		} else {
			_curr = _iomgr->_nextIdNum.load();
		}
	}

	~OntapKVIteratorIPC() {}

    boost::optional<Record> next() {
	RecordData rd;

	invariant(checkRange() == true);
	if (_forward && _curr > _iomgr->_nextIdNum.load()) {
		return {};
	} else if (!_forward && _curr == _iomgr->getRsid()) {
		return {};
	}
	RecordId id(_curr);
	kv_storage_hint_t hint;
	Timer t;

        /* Copy data or give out reference ?*/
        int lookup_result = _iomgr->getCacheMgr()->lookup(_txn, std::stoi("1234"), id,
							  &hint, &rd);
        long long tm = t.micros();
        _iomgr->getCacheMgr()->setLookupLatency(tm);
        _iomgr->getCacheMgr()->setLookupHistogram(tm);
        if (lookup_result == KVCACHE_FOUND) {
		_curr = _forward ? _curr+1 : _curr-1;
                return {{id, rd}};
        }
        if (lookup_result == KVCACHE_NOT_FOUND) {
                bzero(&hint, sizeof(hint));
        }
	bool result = _iomgr->readRecord(_txn,
			    "dummy",
			     id,
			     &hint,
				&rd);
	_curr = _forward ? _curr+1 : _curr-1;
	if (result) {
		return {{id, rd}};
	}
	return {};
	}
    boost::optional<Record> seekExact(const RecordId& id) {
	RecordData rd;

	invariant(checkRange() == true);

	kv_storage_hint_t hint;
	Timer t;

        /* Copy data or give out reference ?*/
        int lookup_result = _iomgr->getCacheMgr()->lookup(_txn, std::stoi("1234"), id,
							  &hint, &rd);
        long long tm = t.micros();
        _iomgr->getCacheMgr()->setLookupLatency(tm);
        _iomgr->getCacheMgr()->setLookupHistogram(tm);
        if (lookup_result == KVCACHE_FOUND) {
		_curr = id.repr();
                return {{id, rd}};
        }
        if (lookup_result == KVCACHE_NOT_FOUND) {
                bzero(&hint, sizeof(hint));
        }
	bool result = _iomgr->readRecord(_txn,
			    "dummy",
			     id,
			     &hint,
				&rd); 
	if (result) {
		_curr = id.repr();
		return {{id, rd}};
	}
	return {};
	}

private:
	bool checkRange() {
		return ((_iomgr->getRsid() & _curr) ? true : false);
	}
	OntapKVIOMgrIPC *_iomgr;
    	int64_t _curr;
};
std::unique_ptr<SeekableRecordCursor> OntapKVIOMgrIPC::getIterator(
			OperationContext *txn,
			bool forward) {
        return stdx::make_unique<OntapKVIteratorIPC>(txn, this, forward);
}
}//namespace mongo
