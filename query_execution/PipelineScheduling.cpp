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

PipelineScheduling::PipelineScheduling(const DAGAnalyzer *dag_analyzer,
                                       WorkOrdersContainer *workorders_container,
                                       const QueryExecutionState &query_exec_state)
    : dag_analyzer_(dag_analyzer),
      container_(workorders_container),
      query_exec_state_(query_exec_state) {
  DCHECK(!dag_analyzer_->getPipelineSequence().empty());
  const std::vector<std::size_t> all_pipelines_sequence(dag_analyzer_->getPipelineSequence());
  dag_analyzer_->splitEssentialAndNonEssentialPipelines(
      all_pipelines_sequence, &non_essential_successors_);
  for (auto pid : dag_analyzer_->getEssentialPipelineSequence()) {
    essential_pipelines_not_started_.push(pid);
  }
  moveNextEssentialPipelineToRunning();
}

void PipelineScheduling::moveNextEssentialPipelineToRunning() {
  if (!essential_pipelines_not_started_.empty()) {
    running_pipelines_.emplace_back(essential_pipelines_not_started_.front());
    essential_pipelines_not_started_.pop();
  }
}

int PipelineScheduling::getNextOperatorHelper() const {
  for (auto running_pipeline_iter = running_pipelines_.rbegin();
       running_pipeline_iter != running_pipelines_.rend();
       ++running_pipeline_iter) {
    const std::size_t active_pipeline_id = *running_pipeline_iter;
  // for (std::size_t active_pipeline_id : running_pipelines_) {
    auto all_operators_in_active_pipeline = dag_analyzer_->getAllOperatorsInPipeline(active_pipeline_id);
    for (auto it = all_operators_in_active_pipeline.rbegin();
         it != all_operators_in_active_pipeline.rend();
         ++it) {
      const std::size_t curr_operator_id = *it;
      if (!query_exec_state_.hasExecutionFinished(curr_operator_id)
          && container_->getNumTotalWorkOrders(curr_operator_id) > 0) {
        return curr_operator_id;
      }
    }
  }
  return -1;
}

int PipelineScheduling::getNextOperator() {
  int next_operator_id = getNextOperatorHelper();
  if (next_operator_id == -1) {
    // If there's another essential pipeline that's not running, try including it.
    moveNextEssentialPipelineToRunning();
    next_operator_id = getNextOperatorHelper();
  }
  return next_operator_id;
}

void PipelineScheduling::informCompletionOfOperator(std::size_t operator_index) {
  const std::size_t pipeline_for_operator = dag_analyzer_->getPipelineIDForOperator(operator_index);
  if (isPipelineExecutionOver(pipeline_for_operator)) {
    // Remove the finished pipeline from the list of running pipelines.
    auto it = std::find(running_pipelines_.begin(), running_pipelines_.end(), pipeline_for_operator);
    running_pipelines_.erase(it);
    if (dag_analyzer_->isPipelineEssential(pipeline_for_operator)) {
      // Move all the non-essential successors of this pipeline to running pipelines.
      running_pipelines_.insert(running_pipelines_.end(),
                                non_essential_successors_[pipeline_for_operator].begin(),
                                non_essential_successors_[pipeline_for_operator].end());
      // In addition, move the next essential pipeline to running list too.
      moveNextEssentialPipelineToRunning();
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
