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

#ifndef QUICKSTEP_QUERY_EXECUTION_INTRA_PIPELINE_SCHEDULING_LIFO_HPP_
#define QUICKSTEP_QUERY_EXECUTION_INTRA_PIPELINE_SCHEDULING_LIFO_HPP_

#include <algorithm>
#include <cstddef>
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

class LIFO : public IntraPipelineScheduling {
 public:
  LIFO(const QueryExecutionState &query_exec_state,
       const DAGAnalyzer *dag_analyzer,
       WorkOrdersContainer *workorders_container) : IntraPipelineScheduling(query_exec_state,
                                                                            dag_analyzer,
                                                                            workorders_container),
                                                    execution_state_(query_exec_state),
                                                    dag_analyzer_(dag_analyzer),
                                                    workorders_container_(workorders_container) {}

  int getNextOperator() override {
    for (auto running_pipeline_iter = current_pipelines_.rbegin();
         running_pipeline_iter != current_pipelines_.rend();
         ++running_pipeline_iter) {
      const std::size_t active_pipeline_id = *running_pipeline_iter;
      auto all_operators_in_active_pipeline = dag_analyzer_->getAllOperatorsInPipeline(active_pipeline_id);
      for (auto it = all_operators_in_active_pipeline.rbegin();
           it != all_operators_in_active_pipeline.rend();
           ++it) {
        const std::size_t curr_operator_id = *it;
        if (!execution_state_.hasExecutionFinished(curr_operator_id)
            && workorders_container_->getNumTotalWorkOrders(curr_operator_id) > 0) {
          return static_cast<int>(curr_operator_id);
        }
      }
    }
    return -1;
  }

  void signalCompletionOfPipeline(std::size_t pipeline_id) override {
    auto it = std::find(current_pipelines_.begin(), current_pipelines_.end(), pipeline_id);
    DCHECK(current_pipelines_.end() != it);
    current_pipelines_.erase(it);
  }

  const std::vector<size_t>& getRunningPipelines() const override {
    return current_pipelines_;
  }

  void signalStartOfPipelines(const std::vector<std::size_t> &pipelines) override {
    current_pipelines_.insert(current_pipelines_.end(), pipelines.begin(), pipelines.end());
  }

 private:
  const QueryExecutionState &execution_state_;
  const DAGAnalyzer *dag_analyzer_;
  WorkOrdersContainer *workorders_container_;

  std::vector<std::size_t> current_pipelines_;

  DISALLOW_COPY_AND_ASSIGN(LIFO);
};

/** @} */

}  // namespace intra_pipeline
}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_EXECUTION_INTRA_PIPELINE_SCHEDULING_LIFO_HPP_
