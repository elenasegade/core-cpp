
#ifndef IGNIS_IRAWMEMORYOBJECT_H
#define IGNIS_IRAWMEMORYOBJECT_H

#include <memory>
#include <thrift/transport/TBufferTransports.h>
#include "IRawObject.h"


namespace ignis {
    namespace executor {
        namespace core {
            namespace storage {
                class IRawMemoryObject : public IRawObject {
                public:
                    IRawMemoryObject(int8_t compression, uint32_t sz = 100 * 1024 * 1024);

                    std::shared_ptr<api::IReadIterator<Any>> readIterator() override;

                    std::shared_ptr<api::IWriteIterator<Any>> writeIterator() override;

                    void read(std::shared_ptr<apache::thrift::transport::TTransport> trans) override;

                    void write(std::shared_ptr<apache::thrift::transport::TTransport> trans, int8_t compression) override;

                private:

                    inline bool fastWrite(std::shared_ptr<apache::thrift::transport::TTransport> transport) override;

                    std::shared_ptr<apache::thrift::transport::TMemoryBuffer> raw_memory;
                };
            }
        }
    }
}

#endif
