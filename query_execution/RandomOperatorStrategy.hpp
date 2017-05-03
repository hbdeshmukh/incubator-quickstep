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

#ifndef QUICKSTEP_QUERY_EXECUTION_RANDOM_OPERATOR_STRATEGY_HPP_
#define QUICKSTEP_QUERY_EXECUTION_RANDOM_OPERATOR_STRATEGY_HPP_

#include <cstddef>
#include <random>
#include <vector>

#include "query_execution/IntraQuerySchedulingStrategy.hpp"
#include "query_execution/QueryExecutionState.hpp"
#include "query_execution/WorkOrdersContainer.hpp"
#include "utility/DAG.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup QueryExecution
 *  @{
 */

class RandomOperatorStrategy : public IntraQuerySchedulingStrategy {
 public:
  /**
   * @brief Constructor.
   **/
  RandomOperatorStrategy(DAG<RelationalOperator, bool> *query_dag,
                         WorkOrdersContainer *workorders_container,
                         const QueryExecutionState &query_exec_state)
      : query_dag_(query_dag),
        workorders_container_(workorders_container),
        query_exec_state_(query_exec_state),
        mt_(std::random_device()()) {}

  ~RandomOperatorStrategy() override {
  }

  int getNextOperator() override {
    return getRandomOperatorWithAvailableWork();
  }

  void informCompletionOfOperator(std::size_t operator_index) override {
  }

 private:
  int getRandomOperatorWithAvailableWork() {
    std::vector<std::size_t> operators_with_pending_work;
    for (std::size_t node_id = 0; node_id < query_dag_->size(); ++node_id) {
      if (!query_exec_state_.hasExecutionFinished(node_id) &&
          workorders_container_->getNumTotalWorkOrders(node_id) > 0) {
        operators_with_pending_work.emplace_back(node_id);
      }
    }
    if (operators_with_pending_work.size() == 1) {
      // Short circuit to avoid the random number generation cost.
      return operators_with_pending_work[0];
    } else if (operators_with_pending_work.size() > 1) {
      std::uniform_int_distribution<std::size_t> dist(
          0, operators_with_pending_work.size() - 1);
      return static_cast<int>(operators_with_pending_work[dist(mt_)]);
    } else {
      return -1;
    }
  }

  DAG<RelationalOperator, bool> *query_dag_;

  WorkOrdersContainer *workorders_container_;

  const QueryExecutionState &query_exec_state_;

  std::mt19937_64 mt_;

  DISALLOW_COPY_AND_ASSIGN(RandomOperatorStrategy);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_EXECUTION_RANDOM_OPERATOR_STRATEGY_HPP_
