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

#include "query_execution/LargestRemainingWorkFirstStrategy.hpp"

#include <cstddef>
#include <utility>
#include <vector>

#include "gflags/gflags.h"

namespace quickstep {

DEFINE_int32(max_num_concurrent_operators_lrwf,
             1,
             "The maximum number of concurrent operators that the strategy can "
             "accommodate.");

LargestRemainingWorkFirstStrategy::LargestRemainingWorkFirstStrategy(
    DAG<RelationalOperator, bool> *query_dag,
    WorkOrdersContainer *workorders_container)
    : query_dag_(query_dag),
      workorders_container_(workorders_container),
      next_active_op_index_(0) {
  for (std::size_t index = 0; index < query_dag_->size(); ++index) {
    waiting_operators_.emplace_back(index);
  }
  refillOperators();
}

void LargestRemainingWorkFirstStrategy::refillOperators() {
  const std::size_t original_active_operators_count = active_operators_.size();
  std::size_t num_operators_checked = 0;
  DCHECK_LT(active_operators_.size(),
            static_cast<std::size_t>(FLAGS_max_num_concurrent_operators_lrwf));
  while (num_operators_checked < waiting_operators_.size() &&
         active_operators_.size() <
             static_cast<std::size_t>(FLAGS_max_num_concurrent_operators_lrwf)) {
    // Get a new candidate operator.
    std::pair<std::size_t, int> next_candidate_for_active_ops(0Lu, 0);
    next_candidate_for_active_ops = getHighestWaitingOperator();
    if (next_candidate_for_active_ops.second >= 0) {
      // There's a candidate. Remove it from the waiting list and insert it
      // in the active list.
      std::size_t next_op = next_candidate_for_active_ops.first;
      waiting_operators_.erase(
          std::remove(
              waiting_operators_.begin(), waiting_operators_.end(), next_op),
          waiting_operators_.end());
      active_operators_.emplace_back(next_op);
      ++num_operators_checked;
    } else {
      break;
    }
  }
  if (original_active_operators_count != active_operators_.size()) {
    // This means, we added new operators to the active operators' list.
    // Reset the next active operator index to 0.
    next_active_op_index_ = 0;
  }
}

}  // namespace quickstep
