/**
 * Copyright 2016-2023 ClickHouse, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <Common/PODArray.h>

namespace RK
{

/// Used for left padding of PODArray when empty
const char empty_pod_array[empty_pod_array_size]{};

template class PODArray<UInt8, 4096, Allocator<false>, 15, 16>;
template class PODArray<UInt16, 4096, Allocator<false>, 15, 16>;
template class PODArray<UInt32, 4096, Allocator<false>, 15, 16>;
template class PODArray<UInt64, 4096, Allocator<false>, 15, 16>;

template class PODArray<Int8, 4096, Allocator<false>, 15, 16>;
template class PODArray<Int16, 4096, Allocator<false>, 15, 16>;
template class PODArray<Int32, 4096, Allocator<false>, 15, 16>;
template class PODArray<Int64, 4096, Allocator<false>, 15, 16>;

}
