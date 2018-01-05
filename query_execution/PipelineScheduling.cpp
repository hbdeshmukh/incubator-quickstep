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
#include <stack>
#include <vector>

#include "query_execution/PipelineScheduling.hpp"
#include "query_execution/DAGAnalyzer.hpp"
#include "query_execution/QueryExecutionState.hpp"
#include "query_execution/WorkOrdersContainer.hpp"
#include "query_execution/intra_pipeline_scheduling/FIFO.hpp"
#include "query_execution/intra_pipeline_scheduling/LIFO.hpp"
#include "query_execution/intra_pipeline_scheduling/Random.hpp"

#include "glog/logging.h"

namespace quickstep {

namespace IP = ::quickstep::intra_pipeline;

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
  intra_pipeline_scheduling_.reset(new IP::FIFO(query_exec_state, dag_analyzer, workorders_container));
  moveNextEssentialPipelineToRunning();
}

void PipelineScheduling::moveNextEssentialPipelineToRunning() {
  if (!essential_pipelines_not_started_.empty()) {
    // running_pipelines_.emplace_back(essential_pipelines_not_started_.front());
    intra_pipeline_scheduling_->signalStartOfPipelines({essential_pipelines_not_started_.front()});
    essential_pipelines_not_started_.pop();
  }
}

bool PipelineScheduling::moveNextFusableEssentialPipelineToRunning() {
  if (!essential_pipelines_not_started_.empty()) {
    DCHECK(!intra_pipeline_scheduling_->getRunningPipelines().empty());
    if (dag_analyzer_->canPipelinesBeFused(
        intra_pipeline_scheduling_->getRunningPipelines().back(), essential_pipelines_not_started_.front())) {
    // if (dag_analyzer_->canPipelinesBeFused(running_pipelines_.back(), essential_pipelines_not_started_.front())) {
      // running_pipelines_.emplace_back(essential_pipelines_not_started_.front());
      intra_pipeline_scheduling_->signalStartOfPipelines({essential_pipelines_not_started_.front()});
      essential_pipelines_not_started_.pop();
      return true;
    }
  }
  return false;
}

void PipelineScheduling::lookAheadForWork() {
  if (!moveNextFusableEssentialPipelineToRunning()) {
    // Look ahead in the list of essential pipelines.
    std::stack<std::size_t> non_schedulable_pipelines;
    while (!essential_pipelines_not_started_.empty()) {
      std::size_t next_pipeline_id = essential_pipelines_not_started_.front();
      essential_pipelines_not_started_.pop();
      if (isPipelineSchedulable(next_pipeline_id)) {
        // running_pipelines_.emplace_back(next_pipeline_id);
        intra_pipeline_scheduling_->signalStartOfPipelines({next_pipeline_id});
        break;
      } else {
        non_schedulable_pipelines.push(next_pipeline_id);
      }
    }
    // Transfer the contents of the stack to the queue again.
    while (!non_schedulable_pipelines.empty()) {
      std::size_t next_pipeline = non_schedulable_pipelines.top();
      non_schedulable_pipelines.pop();
      essential_pipelines_not_started_.push(next_pipeline);
    }
  }
}

int PipelineScheduling::getNextOperatorHelper() const {
  return intra_pipeline_scheduling_->getNextOperator();
}

int PipelineScheduling::getNextOperator() {
  int next_operator_id = getNextOperatorHelper();
  if (next_operator_id == -1) {
    // If there's another essential and fusable pipeline that's not running,
    // add it to the list of running pipelines.
    lookAheadForWork();
    next_operator_id = getNextOperatorHelper();
  }
  return next_operator_id;
}

void PipelineScheduling::informCompletionOfOperator(std::size_t operator_index) {
  const std::size_t pipeline_for_operator = dag_analyzer_->getPipelineIDForOperator(operator_index);
  if (isPipelineExecutionOver(pipeline_for_operator)) {
    // Remove the finished pipeline from the list of running pipelines.
    /*auto it = std::find(running_pipelines_.begin(), running_pipelines_.end(), pipeline_for_operator);
    running_pipelines_.erase(it);*/
    intra_pipeline_scheduling_->signalCompletionOfPipeline(pipeline_for_operator);
    if (dag_analyzer_->isPipelineEssential(pipeline_for_operator)) {
      // Move all the non-essential successors of this pipeline to running pipelines.
      /*running_pipelines_.insert(running_pipelines_.end(),
                                non_essential_successors_[pipeline_for_operator].begin(),
                                non_essential_successors_[pipeline_for_operator].end());*/
      intra_pipeline_scheduling_->signalStartOfPipelines(non_essential_successors_[pipeline_for_operator]);
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

bool PipelineScheduling::isPipelineSchedulable(std::size_t pipeline_id) const {
  for (std::size_t op_id : dag_analyzer_->getAllOperatorsInPipeline(pipeline_id)) {
    if (container_->getNumTotalWorkOrders(op_id) > 0) {
      return true;
    }
  }
  return false;
}

}  // namespace quickstep
