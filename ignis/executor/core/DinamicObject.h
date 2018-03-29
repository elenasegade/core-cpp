
#ifndef EXECUTORCPP_DINAMICOBJECT_H
#define EXECUTORCPP_DINAMICOBJECT_H

#include <string>
#include <memory>
#include "../api/ILoadClass.h"

namespace ignis {
    namespace executor {
        namespace core {

            template<typename T>
            class DinamicObject;

            class ObjectLoader {
            private:
                template<typename>
                friend
                class DinamicObject;

                static void
                open(DinamicObject<api::ILoadClass<void>> &obj, std::string &name, const std::type_info &info);

                static void close(void *dl);
            };

            template<typename T>
            class DinamicObject {
            public:
                friend ObjectLoader;

                DinamicObject(std::string &name) {
                    ObjectLoader::open((DinamicObject<api::ILoadClass<void>> &) *this, name, typeid(T));
                }

                T *operator->() const {
                    return handle->object_ptr;
                }

                T *get() const {
                    return handle->object_ptr;
                }

            private:

                class Handle {
                public:
                    Handle(T *object_ptr, void (*destructor)(T *), void *dl) : object_ptr(object_ptr),
                                                                               destructor(destructor), dl(dl) {}

                    ~Handle() {
                        try {
                            (*destructor)(object_ptr);
                        } catch (...) {}
                        ObjectLoader::close(dl);
                    }

                    void *dl;

                    T *object_ptr;

                    void (*destructor)(T *);

                };

                std::shared_ptr<Handle> handle;
            };

        }
    }
}
#endif
