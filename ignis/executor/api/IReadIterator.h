
#ifndef IGNIS_IREADITERATOR_H
#define IGNIS_IREADITERATOR_H

#include <memory>
#include "executor/core/exception/ILogicError.h"

namespace ignis {
    namespace executor {
        namespace api {
            template<typename Tp>
            class IReadIterator {
            public:
                typedef Tp value_type;
                typedef IReadIterator<Tp> _IReadIterator_type;

                virtual Tp &next() { throw core::exception::ILogicError("not implemented"); }

                virtual std::shared_ptr<Tp> nextShared() { throw core::exception::ILogicError("not implemented"); }

                virtual bool hasNext() { throw core::exception::ILogicError("not implemented"); }
            };
        }
    }
}


#endif
