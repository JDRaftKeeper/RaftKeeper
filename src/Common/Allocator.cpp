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
#include "Allocator.h"

/** Keep definition of this constant in cpp file; otherwise its value
  * is inlined into allocator code making it impossible to override it
  * in third-party code.
  *
  * Note: extern may seem redundant, but is actually needed due to bug in GCC.
  * See also: https://gcc.gnu.org/legacy-ml/gcc-help/2017-12/msg00021.html
  */
#ifdef NDEBUG
    __attribute__((__weak__)) extern const size_t MMAP_THRESHOLD = 64 * (1ULL << 20);
#else
    /**
      * In debug build, use small mmap threshold to reproduce more memory
      * stomping bugs. Along with ASLR it will hopefully detect more issues than
      * ASan. The program may fail due to the limit on number of memory mappings.
      *
      * Not too small to avoid too quick exhaust of memory mappings.
      */
    __attribute__((__weak__)) extern const size_t MMAP_THRESHOLD = 16384;
#endif

template class Allocator<false, false>;
template class Allocator<true, false>;
template class Allocator<false, true>;
template class Allocator<true, true>;
