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

#include "gflags/gflags.h"
#include "glog/logging.h"

namespace quickstep {

namespace IP = ::quickstep::intra_pipeline;

DECLARE_int32(intra_pipeline_scheduling_strategy);

DEFINE_bool(fuse_pipelines, true, "Whether two pipelines should be fused");

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
  switch (FLAGS_intra_pipeline_scheduling_strategy) {
    case 0: {
      intra_pipeline_scheduling_.reset(new IP::LIFO(query_exec_state, dag_analyzer, workorders_container));
      break;
    } case 1: {
      intra_pipeline_scheduling_.reset(new IP::FIFO(query_exec_state, dag_analyzer, workorders_container));
      break;
    } case 2: {
      intra_pipeline_scheduling_.reset(new IP::Random(query_exec_state, dag_analyzer, workorders_container));
      break;
    } default: {
      LOG(ERROR) << "Invalid intra pipeline scheduling strategy used. "
          << "Valid choices are 0 (LIFO), 1 (FIFO), and 2 (Random)";
    }
  }
  moveNextEssentialPipelineToRunning();
}

void PipelineScheduling::moveNextEssentialPipelineToRunning() {
  if (!essential_pipelines_not_started_.empty()) {
    intra_pipeline_scheduling_->signalStartOfPipelines({essential_pipelines_not_started_.front()});
    essential_pipelines_not_started_.pop();
    if (FLAGS_fuse_pipelines) {
      moveNextFusableEssentialPipelineToRunning();
    }
  }
}

bool PipelineScheduling::moveNextFusableEssentialPipelineToRunning() {
  if (!essential_pipelines_not_started_.empty()) {
    DCHECK(!intra_pipeline_scheduling_->getRunningPipelines().empty());
    if (dag_analyzer_->canPipelinesBeFused(
        intra_pipeline_scheduling_->getRunningPipelines().back(),
        essential_pipelines_not_started_.front())) {
      intra_pipeline_scheduling_->signalStartOfPipelines(
          {essential_pipelines_not_started_.front()});
      essential_pipelines_not_started_.pop();
      return true;
    }
  }
  return false;
}

int PipelineScheduling::getNextOperator() {
  return intra_pipeline_scheduling_->getNextOperator();
}

void PipelineScheduling::informCompletionOfOperator(std::size_t operator_index) {
  const std::size_t pipeline_for_operator = dag_analyzer_->getPipelineIDForOperator(operator_index);
  if (isPipelineExecutionOver(pipeline_for_operator)) {
    // Remove the finished pipeline from the list of running pipelines.
    intra_pipeline_scheduling_->signalCompletionOfPipeline(pipeline_for_operator);
    if (dag_analyzer_->isPipelineEssential(pipeline_for_operator)) {
      // Move all the non-essential successors of this pipeline to running pipelines.
      intra_pipeline_scheduling_->signalStartOfPipelines(non_essential_successors_[pipeline_for_operator]);
      // In addition, move the next essential pipeline to running list too.
      const std::vector<std::size_t> &running_pipelines = intra_pipeline_scheduling_->getRunningPipelines();
      const std::size_t num_essential_running_pipelines = std::count_if(running_pipelines.begin(),
                                                                        running_pipelines.end(),
                                                                        [this](std::size_t pid) {
                                                                          return dag_analyzer_->isPipelineEssential(pid);
                                                                        });
      if (num_essential_running_pipelines == 0) {
        moveNextEssentialPipelineToRunning();
      }
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
