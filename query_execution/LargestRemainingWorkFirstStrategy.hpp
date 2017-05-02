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

#ifndef QUICKSTEP_QUERY_EXECUTION_LARGEST_REMAINING_WORK_FIRST_STRATEGY_HPP_
#define QUICKSTEP_QUERY_EXECUTION_LARGEST_REMAINING_WORK_FIRST_STRATEGY_HPP_

#include <cstddef>
#include <utility>
#include <vector>

#include "query_execution/IntraQuerySchedulingStrategy.hpp"
#include "query_execution/WorkOrdersContainer.hpp"
#include "relational_operators/RelationalOperator.hpp"
#include "utility/DAG.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {

/** \addtogroup QueryExecution
 *  @{
 */

class LargestRemainingWorkFirstStrategy : public IntraQuerySchedulingStrategy {
 public:
  /**
   * @brief Constructor.
   **/
  LargestRemainingWorkFirstStrategy(DAG<RelationalOperator, bool> *query_dag,
                                     WorkOrdersContainer *workorders_container);

  ~LargestRemainingWorkFirstStrategy() override {
  }

  int getNextOperator() override {
    return getHighestWaitingOperator().first;
  }

  void informCompletionOfOperator(std::size_t operator_index) override {
    active_operators_.erase(
        std::remove(
            active_operators_.begin(), active_operators_.end(), operator_index),
        active_operators_.end());
  }

 private:
  std::pair<int, int> getHighestWaitingOperator() {
    int max_work = INT_MIN;
    int max_work_op_index = -1;
    for (auto op_id : waiting_operators_) {
      const int curr_op_pending_work =
          workorders_container_->getNumTotalWorkOrders(op_id);
      if (curr_op_pending_work > max_work && curr_op_pending_work > 0) {
        max_work = curr_op_pending_work;
        max_work_op_index = op_id;
      }
    }
    if (max_work == INT_MIN) {
      max_work = -1;
    }
    return std::make_pair(max_work_op_index, max_work);
  }

  void refillOperators();

  DAG<RelationalOperator, bool> *query_dag_;

  WorkOrdersContainer *workorders_container_;

  std::size_t next_active_op_index_;

  std::vector<std::size_t> active_operators_;
  std::vector<std::size_t> waiting_operators_;

  DISALLOW_COPY_AND_ASSIGN(LargestRemainingWorkFirstStrategy);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_EXECUTION_LARGEST_REMAINING_WORK_FIRST_STRATEGY_HPP_
