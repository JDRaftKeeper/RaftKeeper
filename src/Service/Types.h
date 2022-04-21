#pragma once

#include <memory>

namespace DB {

template<typename T>
using ptr = std::shared_ptr<T>;

}
