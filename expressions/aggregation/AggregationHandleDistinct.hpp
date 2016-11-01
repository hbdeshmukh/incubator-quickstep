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

#ifndef QUICKSTEP_EXPRESSIONS_AGGREGATION_AGGREGATION_HANDLE_DISTINCT_HPP_
#define QUICKSTEP_EXPRESSIONS_AGGREGATION_AGGREGATION_HANDLE_DISTINCT_HPP_

#include <cstddef>
#include <memory>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "expressions/aggregation/AggregationConcreteHandle.hpp"
#include "expressions/aggregation/AggregationID.hpp"
#include "storage/HashTableBase.hpp"
#include "types/TypedValue.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {

class AggregationState;
class ColumnVector;
class StorageManager;
class Type;
class ValueAccessor;

/** \addtogroup Expressions
 *  @{
 */

class AggregationHandleDistinct : public AggregationConcreteHandle {
 public:
  /**
   * @brief Constructor.
   **/
  AggregationHandleDistinct()
      : AggregationConcreteHandle(AggregationID::kDistinct) {}

  std::vector<const Type *> getArgumentTypes() const override {
    return {};
  }

  const Type* getResultType() const override {
    LOG(FATAL)
        << "AggregationHandleDistinct does not support getResultType().";
  }

  AggregationState* createInitialState() const override {
    LOG(FATAL)
        << "AggregationHandleDistinct does not support createInitialState().";
  }

  AggregationState* accumulateNullary(
      const std::size_t num_tuples) const override {
    LOG(FATAL)
        << "AggregationHandleDistinct does not support accumulateNullary().";
  }

  AggregationState* accumulate(
      ValueAccessor *accessor,
      ColumnVectorsValueAccessor *aux_accessor,
      const std::vector<attribute_id> &argument_ids) const override {
    LOG(FATAL) << "AggregationHandleDistinct does not support "
                  "accumulate().";
  }

  void mergeStates(const AggregationState &source,
                   AggregationState *destination) const override {
    LOG(FATAL) << "AggregationHandleDistinct does not support mergeStates().";
  }

  TypedValue finalize(const AggregationState &state) const override {
    LOG(FATAL) << "AggregationHandleDistinct does not support finalize().";
  }

  ColumnVector* finalizeHashTable(
      const AggregationStateHashTableBase &hash_table,
      std::vector<std::vector<TypedValue>> *group_by_keys,
      int index) const override;

 private:
  DISALLOW_COPY_AND_ASSIGN(AggregationHandleDistinct);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_EXPRESSIONS_AGGREGATION_AGGREGATION_HANDLE_DISTINCT_HPP_
