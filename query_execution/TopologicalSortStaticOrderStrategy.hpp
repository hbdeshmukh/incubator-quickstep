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

#ifndef QUICKSTEP_QUERY_EXECUTION_TOPOLOGICAL_SORT_STATIC_ORDER_STRATEGY_STRATEGY_HPP_
#define QUICKSTEP_QUERY_EXECUTION_TOPOLOGICAL_SORT_STATIC_ORDER_STRATEGY_STRATEGY_HPP_

#include <cstddef>
#include <vector>

#include "query_execution/IntraQuerySchedulingStrategy.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup QueryExecution
 *  @{
 */

class TopologicalSortStaticOrderStrategy : public IntraQuerySchedulingStrategy {
 public:
  /**
   * @brief Constructor.
   *
   * @param query_dag The query plan DAG.
   **/
  explicit TopologicalSortStaticOrderStrategy(
      const std::vector<std::size_t> &topologically_ordered_operators)
      : topological_order_operators_(topologically_ordered_operators),
        current_index_(0) {}

  ~TopologicalSortStaticOrderStrategy() override {
  }

  int getNextOperator() override {
    if (current_index_ < topological_order_operators_.size()) {
      return topological_order_operators_[current_index_];
    }
    return -1;
  }

  void informCompletionOfOperator(std::size_t operator_index) override {
    ++current_index_;
  }

 private:
  const std::vector<std::size_t> topological_order_operators_;

  std::size_t current_index_;

  DISALLOW_COPY_AND_ASSIGN(TopologicalSortStaticOrderStrategy);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_EXECUTION_TOPOLOGICAL_SORT_STATIC_ORDER_STRATEGY_STRATEGY_HPP_
