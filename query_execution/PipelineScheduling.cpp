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

#include <algorithm>
#include <cstddef>
#include <queue>
#include <vector>

#include "query_execution/PipelineScheduling.hpp"
#include "query_execution/DAGAnalyzer.hpp"
#include "query_execution/QueryExecutionState.hpp"
#include "query_execution/WorkOrdersContainer.hpp"

#include "glog/logging.h"

namespace quickstep {

PipelineScheduling::PipelineScheduling(DAG<RelationalOperator, bool> *query_dag,
                                       const DAGAnalyzer *dag_analyzer,
                                       WorkOrdersContainer *workorders_container,
                                       const QueryExecutionState &query_exec_state)
    : query_dag_(query_dag),
      dag_analyzer_(dag_analyzer),
      container_(workorders_container),
      query_exec_state_(query_exec_state) {
  DCHECK(!dag_analyzer_->getPipelineSequence().empty());
  for (auto pid : dag_analyzer->getPipelineSequence()) {
    pipelines_not_started_.push(pid);
  }
  running_pipelines_.emplace_back(pipelines_not_started_.front());
  pipelines_not_started_.pop();
}

int PipelineScheduling::getNextOperator() {
  for (std::size_t active_pipeline_id : running_pipelines_) {
    auto all_operators_in_active_pipeline = dag_analyzer_->getAllOperatorsInPipeline(active_pipeline_id);
    for (auto it = all_operators_in_active_pipeline.rbegin(); it != all_operators_in_active_pipeline.rend(); ++it) {
      const std::size_t curr_operator_id = *it;
      if (!query_exec_state_.hasExecutionFinished(curr_operator_id)
          && container_->getNumTotalWorkOrders(curr_operator_id) > 0) {
        return curr_operator_id;
      }
    }
  }
  // Current running pipelines don't have any available work.
  return -1;
}

void PipelineScheduling::informCompletionOfOperator(std::size_t operator_index) {
  const std::size_t pipeline_for_operator = dag_analyzer_->getPipelineIDForOperator(operator_index);
  if (dag_analyzer_->getAllOperatorsInPipeline(pipeline_for_operator).size() == 1u
      || isPipelineExecutionOver(pipeline_for_operator)) {
    // Remove the finished pipeline from the list of running pipelines.
    auto it = std::find(running_pipelines_.begin(), running_pipelines_.end(), pipeline_for_operator);
    running_pipelines_.erase(it);
    if (!pipelines_not_started_.empty()) {
      running_pipelines_.emplace_back(pipelines_not_started_.front());
      pipelines_not_started_.pop();
    }
  }
}

bool PipelineScheduling::isPipelineExecutionOver(std::size_t pipeline_id) const {
  for (std::size_t op_id : dag_analyzer_->getAllOperatorsInPipeline(pipeline_id)) {
    if (!query_exec_state_.hasExecutionFinished(op_id)) {
      return false;
    }
  }
  return true;
}

}  // namespace quickstep
