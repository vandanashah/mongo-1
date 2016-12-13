/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "mongo/base/status.h"
#include "mongo/db/storage/ontapkv/ontapkv_global_options.h"
#include "mongo/util/log.h"
#include "mongo/util/options_parser/constraints.h"

namespace mongo {

OntapKVGlobalOptions ontapKVGlobalOptions;

Status OntapKVGlobalOptions::add(moe::OptionSection* options) {
    moe::OptionSection ontapKVOptions("OntapKV options");

    // OntapKV storage engine options
    ontapKVOptions.addOptionChaining("storage.ontapKV.engineConfig.cacheSizeGB",
                                        "ontapKVCacheSizeGB",
                                        moe::Int,
                                        "maximum amount of memory to allocate for cache; "
                                        "defaults to 1/2 of physical RAM").validRange(1, 10000);
    ontapKVOptions.addOptionChaining(
                          "storage.ontapKV.engineConfig.statisticsLogDelaySecs",
                          "ontapKVStatisticsLogDelaySecs",
                          moe::Int,
                          "seconds to wait between each write to a statistics file in the dbpath; "
                          "0 means do not log statistics")
        .validRange(0, 100000)
        .setDefault(moe::Value(0));
    ontapKVOptions.addOptionChaining("storage.ontapKV.engineConfig.journalCompressor",
                                        "ontapKVJournalCompressor",
                                        moe::String,
                                        "use a compressor for log records [none|snappy|zlib]")
        .format("(:?none)|(:?snappy)|(:?zlib)", "(none/snappy/zlib)")
        .setDefault(moe::Value(std::string("snappy")));
    ontapKVOptions.addOptionChaining("storage.ontapKV.engineConfig.directoryForIndexes",
                                        "ontapKVDirectoryForIndexes",
                                        moe::Switch,
                                        "Put indexes and data in different directories");
    ontapKVOptions.addOptionChaining("storage.ontapKV.engineConfig.configString",
                                        "ontapKVEngineConfigString",
                                        moe::String,
                                        "OntapKV storage engine custom "
                                        "configuration settings").hidden();

    // OntapKV collection options
    ontapKVOptions.addOptionChaining("storage.ontapKV.collectionConfig.blockCompressor",
                                        "ontapKVCollectionBlockCompressor",
                                        moe::String,
                                        "block compression algorithm for collection data "
                                        "[none|snappy|zlib]")
        .format("(:?none)|(:?snappy)|(:?zlib)", "(none/snappy/zlib)")
        .setDefault(moe::Value(std::string("snappy")));
    ontapKVOptions.addOptionChaining("storage.ontapKV.collectionConfig.configString",
                                        "ontapKVCollectionConfigString",
                                        moe::String,
                                        "OntapKV custom collection configuration settings")
        .hidden();


    // OntapKV index options
    ontapKVOptions.addOptionChaining("storage.ontapKV.indexConfig.prefixCompression",
                                        "ontapKVIndexPrefixCompression",
                                        moe::Bool,
                                        "use prefix compression on row-store leaf pages")
        .setDefault(moe::Value(true));
    ontapKVOptions.addOptionChaining("storage.ontapKV.indexConfig.configString",
                                        "ontapKVIndexConfigString",
                                        moe::String,
                                        "OntapKV custom index configuration settings").hidden();

    return options->addSection(ontapKVOptions);
}

Status OntapKVGlobalOptions::store(const moe::Environment& params,
                                      const std::vector<std::string>& args) {
    // OntapKV storage engine options
    if (params.count("storage.ontapKV.engineConfig.cacheSizeGB")) {
        ontapKVGlobalOptions.cacheSizeGB =
            params["storage.ontapKV.engineConfig.cacheSizeGB"].as<int>();
    }
    if (params.count("storage.syncPeriodSecs")) {
        ontapKVGlobalOptions.checkpointDelaySecs =
            static_cast<size_t>(params["storage.syncPeriodSecs"].as<double>());
    }
    if (params.count("storage.ontapKV.engineConfig.statisticsLogDelaySecs")) {
        ontapKVGlobalOptions.statisticsLogDelaySecs =
            params["storage.ontapKV.engineConfig.statisticsLogDelaySecs"].as<int>();
    }
    if (params.count("storage.ontapKV.engineConfig.journalCompressor")) {
        ontapKVGlobalOptions.journalCompressor =
            params["storage.ontapKV.engineConfig.journalCompressor"].as<std::string>();
    }
    if (params.count("storage.ontapKV.engineConfig.directoryForIndexes")) {
        ontapKVGlobalOptions.directoryForIndexes =
            params["storage.ontapKV.engineConfig.directoryForIndexes"].as<bool>();
    }
    if (params.count("storage.ontapKV.engineConfig.configString")) {
        ontapKVGlobalOptions.engineConfig =
            params["storage.ontapKV.engineConfig.configString"].as<std::string>();
        log() << "Engine custom option: " << ontapKVGlobalOptions.engineConfig;
    }

    // OntapKV collection options
    if (params.count("storage.ontapKV.collectionConfig.blockCompressor")) {
        ontapKVGlobalOptions.collectionBlockCompressor =
            params["storage.ontapKV.collectionConfig.blockCompressor"].as<std::string>();
    }
    if (params.count("storage.ontapKV.collectionConfig.configString")) {
        ontapKVGlobalOptions.collectionConfig =
            params["storage.ontapKV.collectionConfig.configString"].as<std::string>();
        log() << "Collection custom option: " << ontapKVGlobalOptions.collectionConfig;
    }

    // OntapKV index options
    if (params.count("storage.ontapKV.indexConfig.prefixCompression")) {
        ontapKVGlobalOptions.useIndexPrefixCompression =
            params["storage.ontapKV.indexConfig.prefixCompression"].as<bool>();
    }
    if (params.count("storage.ontapKV.indexConfig.configString")) {
        ontapKVGlobalOptions.indexConfig =
            params["storage.ontapKV.indexConfig.configString"].as<std::string>();
        log() << "Index custom option: " << ontapKVGlobalOptions.indexConfig;
    }

    return Status::OK();
}

}  // namespace mongo
