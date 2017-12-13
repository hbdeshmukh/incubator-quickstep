/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 **/

#ifndef QUICKSTEP_UTILITY_VECTOR_UTIL_HPP_
#define QUICKSTEP_UTILITY_VECTOR_UTIL_HPP_

#include <array>
#include <vector>

#include "glog/logging.h"

namespace quickstep {

/** \addtogroup Utility
 *  @{
 */

/**
 * @brief Adds a new element \p item to a vector \p list if the element does not exist.
 *
 * @param item The element to be inserted to the vector.
 * @param vec The vector to be appended to.
 * @return True if the element is inserted.
 */
template <class Type>
bool AppendToVectorIfNotPresent(const Type &item, std::vector<Type> *vec) {
  for (const Type &exist_item : *vec) {
    if (exist_item == item) {
      return false;
    }
  }
  vec->push_back(item);
  return true;
}

/**
 * @brief Get a permutation of the given vector with the given rank.
 *
 * @note In order to be efficient, we only allow vectors until certain length.
 *       If this condition is not met, we return an empty vector.
 *
 * @param elements The vector
 * @param permutation_rank The rank of the desired permutation.
 * @return A copy of the permuted vector.
 */
template <class Type>
std::vector<Type> GetPermutationOfVector(
    const std::vector<Type> &elements, std::size_t permutation_rank) {
  std::vector<Type> result;
  std::array<std::array<int, 4>, 24> permutations_of_4 = {{
                                                              {0, 1, 2, 3},
                                                              {0, 1, 3, 2},
                                                              {0, 2, 1, 3},
                                                              {0, 2, 3, 1},
                                                              {0, 3, 1, 2},
                                                              {0, 3, 2, 1},
                                                              {1, 0, 2, 3},
                                                              {1, 0, 3, 2},
                                                              {1, 2, 0, 3},
                                                              {1, 2, 3, 0},
                                                              {1, 3, 0, 2},
                                                              {1, 3, 2, 0},
                                                              {2, 0, 1, 3},
                                                              {2, 0, 3, 1},
                                                              {2, 1, 0, 3},
                                                              {2, 1, 3, 0},
                                                              {2, 3, 0, 1},
                                                              {2, 3, 1, 0},
                                                              {3, 0, 1, 2},
                                                              {3, 0, 2, 1},
                                                              {3, 1, 0, 2},
                                                              {3, 1, 2, 0},
                                                              {3, 2, 0, 1},
                                                              {3, 2, 1, 0},
                                                          }};

  std::array<std::array<int, 3>, 6> permutations_of_3 = {{
                                                                    {0, 1, 2},
                                                                    {0, 2, 1},
                                                                    {1, 0, 2},
                                                                    {1, 2, 0},
                                                                    {2, 0, 1},
                                                                    {2, 1, 0},
                                                                }};

  switch (elements.size()) {
    case 3: {
      DCHECK_LT(permutation_rank, permutations_of_3.size());
      for (int permuted_index : permutations_of_3[permutation_rank]) {
        result.push_back(elements[permuted_index]);
      }
      break;
    }
    case 4: {
      DCHECK_LT(permutation_rank, permutations_of_4.size());
      for (int permuted_index : permutations_of_4[permutation_rank]) {
        result.push_back(elements[permuted_index]);
      }
      break;
    }
    default: {
      break;
    }
  }
  return result;
}

/** @} */

}  // namespace quickstep

#endif /* QUICKSTEP_UTILITY_VECTOR_UTIL_HPP_ */
