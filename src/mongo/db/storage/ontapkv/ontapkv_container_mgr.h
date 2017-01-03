#pragma once

namespace mongo {
class OntapKVContainerMgr {
public:
	OntapKVContainerMgr() {}
	~OntapKVContainerMgr() {}
	std::string getContainerId(StringData ns) {
		return ns.toString();
	}
};

}//mongo
