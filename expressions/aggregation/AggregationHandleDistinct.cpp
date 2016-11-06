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

#include "expressions/aggregation/AggregationHandleDistinct.hpp"

#include <cstddef>
#include <memory>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "storage/PackedPayloadAggregationStateHashTable.hpp"

#include "types/TypedValue.hpp"

#include "glog/logging.h"

namespace quickstep {

class ColumnVector;

ColumnVector* AggregationHandleDistinct::finalizeHashTable(
    const AggregationStateHashTableBase &hash_table,
    std::vector<std::vector<TypedValue>> *group_by_keys,
    int index) const {
  DCHECK(group_by_keys->empty());

  const auto keys_retriever = [&group_by_keys](std::vector<TypedValue> &group_by_key,
                                               const bool &dumb_placeholder) -> void {
    group_by_keys->emplace_back(std::move(group_by_key));
  };
  static_cast<const PackedPayloadSeparateChainingAggregationStateHashTable &>(
      hash_table).forEach(&keys_retriever);

  return nullptr;
}

}  // namespace quickstep
