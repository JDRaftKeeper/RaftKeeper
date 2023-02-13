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
//#pragma once
//
//#if !defined(ARCADIA_BUILD)
//#    include <miniselect/floyd_rivest_select.h>  // Y_IGNORE
//#else
//#    include <algorithm>
//#endif
//
//template <class RandomIt>
//void nth_element(RandomIt first, RandomIt nth, RandomIt last)
//{
//#if !defined(ARCADIA_BUILD)
//    ::miniselect::floyd_rivest_select(first, nth, last);
//#else
//    ::std::nth_element(first, nth, last);
//#endif
//}
//
//template <class RandomIt>
//void partial_sort(RandomIt first, RandomIt middle, RandomIt last)
//{
//#if !defined(ARCADIA_BUILD)
//    ::miniselect::floyd_rivest_partial_sort(first, middle, last);
//#else
//    ::std::partial_sort(first, middle, last);
//#endif
//}
//
//template <class RandomIt, class Compare>
//void partial_sort(RandomIt first, RandomIt middle, RandomIt last, Compare compare)
//{
//#if !defined(ARCADIA_BUILD)
//    ::miniselect::floyd_rivest_partial_sort(first, middle, last, compare);
//#else
//    ::std::partial_sort(first, middle, last, compare);
//#endif
//}
