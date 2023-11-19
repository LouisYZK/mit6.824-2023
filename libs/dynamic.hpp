#pragma once
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <iostream>
#include <unordered_map>
#include "nameof.hpp"
#include "spdlog/spdlog.h"

template <class BaseClass>
class AppCreator {
public:
    AppCreator() = default;
    AppCreator(const AppCreator& rhs) = delete;
    AppCreator& operator=(const AppCreator& rhs) = delete;

    using regist_func_t = std::function<std::shared_ptr<BaseClass>()>;
    static std::shared_ptr<AppCreator> instance() {
        static auto ptr = []() {
            return std::make_shared<AppCreator>();
        }();
        return ptr;
    }
    
    void regist_obj(const std::string& ttype, regist_func_t reg_func) {
        auto itr = func_map_.find(ttype);
        if (itr == func_map_.end()) {
            func_map_[ttype] = std::move(reg_func);
            spdlog::info("Regist Class [{}]", ttype);
        }
    }

    std::shared_ptr<BaseClass> create(const std::string& ttype) {
        auto itr = func_map_.find(ttype);
        if (itr != func_map_.end()) {
            return itr->second();
        }
        spdlog::error("Class [{}] not found", ttype);
        return nullptr;
    }

private:
    std::unordered_map<std::string, regist_func_t> func_map_;
};

template <class BaseClass, class DerivedClass>
class AppFactory {
public:
    AppFactory() {
        reg.ping();
    }
    static std::shared_ptr<DerivedClass> create_obj() {
        return std::make_shared<DerivedClass>();
    }

    struct Register {
        Register() {
            std::string obj_class_name {NAMEOF_TYPE(DerivedClass)};
            AppCreator<BaseClass>::instance()->regist_obj(obj_class_name, AppFactory::create_obj);
        }
        inline void ping() {return; }
    };
private:
    static Register reg;
};

template <class BaseClass, class DerivedClass>
typename AppFactory<BaseClass, DerivedClass>::Register AppFactory<BaseClass, DerivedClass>::reg;



using KeyValue = std::pair<std::string, std::string>;
class MapReduceApp {
public:
    virtual std::vector<KeyValue> map(const std::string& filename, const std::string& contents) = 0;
    virtual std::string reduce(const std::string& key, const std::vector<std::string>& values) = 0;
};
