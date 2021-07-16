
#include "ICommImpl.h"
#include "ignis/executor/core/protocol/IObjectProtocol.h"

using namespace ignis::executor::core::modules::impl;

ICommImpl::ICommImpl(std::shared_ptr<IExecutorData> &executorData) : IBaseImpl(executorData) {
    const_cast<MPI::Intracomm &>(executor_data->mpi().native()).Set_errhandler(MPI::ERRORS_THROW_EXCEPTIONS);
}

ICommImpl::~ICommImpl() {}

std::string ICommImpl::openGroup() {
    IGNIS_TRY()
    IGNIS_LOG(info) << "Comm: creating group server";
    char port_name[MPI_MAX_PORT_NAME];
    MPI::Open_port(MPI::INFO_NULL, port_name);
    auto handle = std::shared_ptr<std::string>(new std::string(port_name), [](const std::string *group_name) {
        MPI::Close_port(group_name->c_str());//Close port on context clear
        delete group_name;
    });
    IGNIS_LOG(info) << "Comm: group created on " << port_name;
    executor_data->setVariable<std::shared_ptr<std::string>>("server", handle);
    return port_name;
    IGNIS_CATCH()
}

void ICommImpl::closeGroup() {
    IGNIS_TRY()
    IGNIS_LOG(info) << "Comm: closing group server";
    executor_data->removeVariable("server");
    IGNIS_CATCH()
}

void ICommImpl::joinToGroup(const std::string &id, bool leader) {
    IGNIS_TRY()
    executor_data->setMpiGroup(joinToGroupImpl(id, leader, id));
    IGNIS_CATCH()
}

void ICommImpl::joinToGroupName(const std::string &id, bool leader, const std::string &name) {
    IGNIS_TRY()
    groups[name] = joinToGroupImpl(id, leader, name);
    IGNIS_CATCH()
}

bool ICommImpl::hasGroup(const std::string &name) {
    IGNIS_TRY()
    return groups.find(name) != groups.end();
    IGNIS_CATCH()
}

void ICommImpl::destroyGroup(const std::string &name) {
    IGNIS_TRY()
    if (name.empty()) {
        MPI::Intracomm comm = executor_data->mpi().native();
        if (comm != MPI::COMM_WORLD) {
            comm.Free();
            executor_data->setMpiGroup(MPI::COMM_WORLD);
        }
        return;
    }

    auto entry = groups.find(name);
    if (entry != groups.end()) {
        entry->second.Free();
        groups.erase(entry);
    }
    IGNIS_CATCH()
}

void ICommImpl::destroyGroups() {
    IGNIS_TRY()
    for (auto &elem : groups) { elem.second.Free(); }
    groups.clear();
    MPI::Intracomm comm = executor_data->mpi().native();
    if (comm != MPI::COMM_WORLD) {
        comm.Free();
        executor_data->setMpiGroup(MPI::COMM_WORLD);
    }
    IGNIS_CATCH()
}

int8_t ICommImpl::getProtocol() { return protocol::IObjectProtocol::CPP_PROTOCOL; }

void ICommImpl::setPartitionsVoid(const std::vector<std::string> &partitions) {
    IGNIS_TRY()
    auto group = executor_data->getPartitionTools().newPartitionGroup<storage::IVoidPartition::VOID_TYPE>();
    for (int64_t i = 0; i < partitions.size(); i++) {
        auto &bytes = partitions[i];
        auto buffer = std::make_shared<transport::IMemoryBuffer>((uint8_t *) bytes.c_str(), bytes.size());
        auto part = executor_data->getPartitionTools().newVoidPartition(bytes.size());
        part->read((std::shared_ptr<transport::ITransport> &) buffer);
        group->add(part);
    }
    IGNIS_CATCH()
}

void ICommImpl::newEmptyPartitionsVoid(int64_t n) {
    IGNIS_TRY()
    auto part_group = executor_data->getPartitionTools().newPartitionGroup<storage::IVoidPartition::VOID_TYPE>();
    for (int64_t i = 0; i < n; i++) { part_group->add(executor_data->getPartitionTools().newVoidPartition()); }
    executor_data->setPartitions<storage::IVoidPartition::VOID_TYPE>(part_group);
    IGNIS_CATCH()
}

void ICommImpl::driverScatterVoid(const std::string &group, int64_t partitions) {
    IGNIS_TRY()
    auto comm = getGroup(group);
    auto part_group = executor_data->getPartitionTools().newPartitionGroup<storage::IVoidPartition::VOID_TYPE>();
    executor_data->mpi().driverScatterVoid(comm, *part_group, partitions);
    executor_data->setPartitions<storage::IVoidPartition::VOID_TYPE>(part_group);
    IGNIS_CATCH()
}

int32_t ICommImpl::enableMultithreading(const std::string &group) {
    return 1;//TODO
}

void ICommImpl::recvVoid(const std::string &group, int64_t partition, int64_t source, int64_t thread) {
    IGNIS_TRY()
    auto part_group = executor_data->getPartitions<storage::IVoidPartition::VOID_TYPE>(true);
    auto comm = getGroup(group);
    int tag = comm.Get_rank();
    executor_data->mpi().recvVoid(comm, (storage::IVoidPartition &) *(*part_group)[partition], source, tag);

    IGNIS_CATCH()
}

MPI::Intracomm ICommImpl::joinToGroupImpl(const std::string &id, bool leader, const std::string &name) {
    bool root = executor_data->hasVariable("server");
    MPI::Intracomm comm = executor_data->mpi().native();
    MPI::Intercomm intercomm;
    MPI::Intercomm comm1;
    if(leader){
        const char * port = root ?  id.c_str() : nullptr;
        intercomm = comm.Accept(port, MPI::INFO_NULL, 0);
        comm1 = intercomm.Merge(0);
        intercomm.Free();
        if(comm != MPI::COMM_WORLD){
            comm.Free();
        }
        comm = comm1;
    }else{
        const char * port = comm.Get_rank() == 0 ?  id.c_str() : nullptr;
        intercomm = comm.Connect(port, MPI::INFO_NULL, 0);
        comm1 = intercomm.Merge(0);
        intercomm.Free();
        if(comm != MPI::COMM_WORLD){
            comm.Free();
        }
        comm = comm1;
    }
    comm.Set_errhandler(MPI::ERRORS_THROW_EXCEPTIONS);
    return comm;
}

MPI::Intracomm &ICommImpl::getGroup(const std::string &id) {
    auto it = groups.find(id);
    if (it == groups.end()) { throw ignis::executor::core::exception::ILogicError("Group " + id + " not found"); }
    return it->second;
}
