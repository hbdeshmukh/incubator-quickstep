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

#ifndef QUICKSTEP_QUERY_EXECUTION_INTRA_PIPELINE_SCHEDULING_RANDOM_HPP_
#define QUICKSTEP_QUERY_EXECUTION_INTRA_PIPELINE_SCHEDULING_RANDOM_HPP_

#include <random>
#include <vector>

#include "glog/logging.h"

#include "query_execution/DAGAnalyzer.hpp"
#include "query_execution/QueryExecutionState.hpp"
#include "query_execution/WorkOrdersContainer.hpp"
#include "query_execution/intra_pipeline_scheduling/IntraPipelineScheduling.hpp"
#include "utility/Macros.hpp"

namespace quickstep {
namespace intra_pipeline {

/** \addtogroup IntraPipelineScheduling
 *  @{
 */

class Random : public IntraPipelineScheduling {
 public:
  Random(const QueryExecutionState &query_exec_state,
         const DAGAnalyzer *dag_analyzer,
         WorkOrdersContainer *workorders_container) : IntraPipelineScheduling(query_exec_state,
                                                                              dag_analyzer,
                                                                              workorders_container),
                                                      execution_state_(query_exec_state),
                                                      dag_analyzer_(dag_analyzer),
                                                      workorders_container_(workorders_container),
                                                      mt_(std::random_device()()) {}

  int getNextOperator() override {
    if (current_operators_.empty()) {
      return -1;
    } else if (current_operators_.size() == 1u) {
      return current_operators_.front();
    } else {
      std::vector<std::size_t> eligible_operators;
      for (std::size_t op : current_operators_) {
        if (!execution_state_.hasExecutionFinished(op) && workorders_container_->getNumTotalWorkOrders(op) > 0) {
          eligible_operators.emplace_back(op);
        }
      }
      if (eligible_operators.empty()) {
        return -1;
      } else if (eligible_operators.size() == 1u) {
        return eligible_operators.front();
      } else {
        std::uniform_int_distribution<std::size_t> dist(0, eligible_operators.size() - 1);
        return static_cast<int>(eligible_operators[dist(mt_)]);
      }
    }
  }

  void signalCompletionOfPipeline(std::size_t pipeline_id) override {
    auto it = std::find(current_pipelines_.begin(), current_pipelines_.end(), pipeline_id);
    DCHECK(current_pipelines_.end() != it);
    current_pipelines_.erase(it);
    removeOperatorsFromPipeline(pipeline_id);
  }

  void signalStartOfPipelines(const std::vector<std::size_t> &pipelines) override {
    current_pipelines_.insert(current_pipelines_.end(), pipelines.begin(), pipelines.end());
    for (std::size_t pid : pipelines) {
      addOperatorsFromPipeline(pid);
    }
  }

  const std::vector<size_t>& getRunningPipelines() const override {
    return current_pipelines_;
  }

 private:
  void addOperatorsFromPipeline(std::size_t pipeline_id) {
    auto all_operators = dag_analyzer_->getAllOperatorsInPipeline(pipeline_id);
    current_operators_.insert(current_operators_.end(), all_operators.begin(), all_operators.end());
  }

  void removeOperatorsFromPipeline(std::size_t pipeline_id) {
    auto all_operators = dag_analyzer_->getAllOperatorsInPipeline(pipeline_id);
    for (std::size_t removable_op : all_operators) {
      auto it = std::find(current_operators_.begin(), current_operators_.end(), removable_op);
      DCHECK(current_operators_.end() != it);
      current_operators_.erase(it);
    }
  }

  const QueryExecutionState &execution_state_;
  const DAGAnalyzer *dag_analyzer_;
  WorkOrdersContainer *workorders_container_;

  std::vector<std::size_t> current_pipelines_;

  std::vector<std::size_t> current_operators_;

  std::mt19937_64 mt_;

  DISALLOW_COPY_AND_ASSIGN(Random);
};

/** @} */

}  // namespace intra_pipeline
}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_EXECUTION_INTRA_PIPELINE_SCHEDULING_RANDOM_HPP_
