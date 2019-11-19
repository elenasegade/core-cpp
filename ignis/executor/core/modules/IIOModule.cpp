
#include "IIOModule.h"
#include <boost/filesystem.hpp>
#include <fstream>
#include "executor/core/selector/ISelector.h"
#include "executor/core/exception/IInvalidArgument.h"

using namespace ignis::executor::core::modules;

IIOModule::IIOModule(std::shared_ptr<IExecutorData> &executor_data) : IModule(executor_data), impl(executor_data) {}

IIOModule::~IIOModule() {}

void IIOModule::openPartitionObjectFile(const std::string &path, int64_t first, int64_t partitions) {
    IGNIS_TRY()
        IGNIS_LOG(info) << "IO: reading header";
        auto header_name = impl.partitionFileName(path + ".header", first);
        auto header_file = impl.openFileRead(header_name + ".header");

        std::string header(100, 0);
        header_file.read(const_cast<char *>(header.c_str()), 100);
        header.resize(header_file.gcount());

        auto type = typeFromHeader(header);
        if (type) {
            typeFromHeader(header)->openPartitionObjectFile(impl, path, first, partitions);
        } else {
            impl.openPartitionObjectFileUnknown(path, first, partitions);
        }
    IGNIS_CATCH()
}

void IIOModule::openPartitionObjectFileFunction(const rpc::ISource &function, const std::string &path,
                                                int64_t first, int64_t partitions) {
    auto lib = executor_data->loadLibrary(function);
    if (lib->args.size() != 1) {
        throw exception::ICompatibilyException("openPartitionObjectFile", lib->info());
    }
    lib->args.begin()->second->openPartitionObjectFile(impl, path, first, partitions);

}

void IIOModule::saveAsPartitionObjectFile(const std::string &path, int8_t compression, int64_t first) {
    typeFromPartition()->saveAsPartitionObjectFile(impl, path, compression, first);
}

void IIOModule::saveAsTextFile(const std::string &path, int64_t first) {
    typeFromPartition()->saveAsTextFile(impl, path, first);
}

void IIOModule::saveAsJsonFile(const std::string &path, int64_t first) {
    typeFromPartition()->saveAsJsonFile(impl, path, first);
}

