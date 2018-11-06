
#include "IDinamicObject.h"
#include <dlfcn.h>
#include <boost/filesystem.hpp>
#include "../../exceptions/IInvalidArgument.h"
#include "../../data/RTTInfo.h"

using namespace ignis::executor::core;

ObjectLoader::ObjectLoader(const std::string &name) {
    int sep = name.find(':');

    if (sep < -1) {
        throw exceptions::IInvalidArgument(name + " is not a valid c++ class");
    }
    std::string path = name.substr(0, sep);
    std::string class_name = name.substr(sep + 1, name.size());

    if(!boost::filesystem::exists(path)){
        throw exceptions::IInvalidArgument(path + " was not found");
    }

    library = dlopen(path.c_str(), RTLD_NOW | RTLD_GLOBAL | RTLD_DEEPBIND);

    if (!library) {
        throw exceptions::IInvalidArgument(path + " could not be loaded");
    }

    auto constructor = (void *(*)()) dlsym(library, (class_name + "_constructor").c_str());
    destructor = (void (*)(void *)) dlsym(library, (class_name + "_destructor").c_str());

    if (!constructor || !destructor) {
        throw exceptions::IInvalidArgument(
                class_name + " must be register, use: ignis_register_class(name, class) in you library");
    }

    object = (*constructor)();
}

ObjectLoader::~ObjectLoader() {
    try {
      //  (*destructor)(object);
    } catch (...) {}
    //dlclose(library);
}

void *ObjectLoader::getObject() {
    return object;
}
