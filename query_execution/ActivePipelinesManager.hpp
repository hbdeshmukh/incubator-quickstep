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

#ifndef QUICKSTEP_QUERY_EXECUTION_ACTIVE_PIPELINES_MANAGER_HPP_
#define QUICKSTEP_QUERY_EXECUTION_ACTIVE_PIPELINES_MANAGER_HPP_

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <memory>
#include <utility>
#include <vector>

#include "query_execution/DAGAnalyzer.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {

/** \addtogroup QueryExecution
 *  @{
 */

class ActivePipeline {
 public:
  ActivePipeline(const std::size_t pipeline_id,
                 const std::vector<std::size_t> &operators_in_pipeline)
      : pipeline_id_(pipeline_id),
        operators_in_pipeline_(operators_in_pipeline),
        next_operator_id_iter_(operators_in_pipeline.cbegin()) {}

  std::size_t getPipelineID() const {
    return pipeline_id_;
  }

  const std::size_t getNextOperatorID() {
    const std::size_t next_operator_id = *next_operator_id_iter_;
    if ((next_operator_id_iter_ + 1) == operators_in_pipeline_.cend()) {
      next_operator_id_iter_ = operators_in_pipeline_.cbegin();
    } else {
      ++next_operator_id_iter_;
    }
    return next_operator_id;
  }

private:
  // TODO(harshad) - Allow the ability to mark operators as done, so that we
  // don't repeatedly assign them as the next operator ID and subsequently they
  // don't return any work.
  const std::size_t pipeline_id_;
  const std::vector<std::size_t> &operators_in_pipeline_;
  std::vector<std::size_t>::const_iterator next_operator_id_iter_;

  DISALLOW_COPY_AND_ASSIGN(ActivePipeline);
};

/**
 * @brief A class that manages the active pipelines in a query plan.
 **/
class ActivePipelinesManager {
 public:
  ActivePipelinesManager(DAGAnalyzer *dag_analyzer)
      : dag_analyzer_(dag_analyzer),
        next_pipeline_iter_(active_pipelines_.begin()) {
    auto free_pipelines = dag_analyzer_->getFreePipelinesStatic();
    for (std::size_t pid : free_pipelines) {
      addPipeline(pid);
    }
  }

  void removePipeline(std::size_t pipeline_id) {
    DCHECK(hasPipeline(pipeline_id));
    active_pipelines_.erase(std::remove_if(active_pipelines_.begin(),
                                           active_pipelines_.end(),
                                           [&](std::unique_ptr<ActivePipeline> const& ac) {
                                             return ac.get()->getPipelineID() ==
                                                    pipeline_id;
                                           }));
  }

  std::size_t getNextPipelineID() {
    const std::size_t next_pipeline_id = (*next_pipeline_iter_)->getPipelineID();
    if ((next_pipeline_iter_ + 1) == active_pipelines_.end()) {
      next_pipeline_iter_ = active_pipelines_.begin();
    } else {
      ++next_pipeline_iter_;
    }
    return next_pipeline_id;
  }

  void addPipeline(const std::size_t pipeline_id) {
    // Make sure we haven't already added the pipeline.
    DCHECK(!hasPipeline(pipeline_id));
    active_pipelines_.push_back(
        std::unique_ptr<ActivePipeline>(new ActivePipeline(
            pipeline_id,
            dag_analyzer_->getAllOperatorsInPipeline(pipeline_id))));
  }

 private:
  bool hasPipeline(const std::size_t pipeline_id) const {
    return std::find_if(active_pipelines_.begin(),
                        active_pipelines_.end(),
                        [&](std::unique_ptr<ActivePipeline> const& ap) {
                          return ap.get()->getPipelineID() == pipeline_id;
                        }) != std::end(active_pipelines_);
  }

  DAGAnalyzer *dag_analyzer_;

  std::vector<std::unique_ptr<ActivePipeline>> active_pipelines_;
  std::vector<std::unique_ptr<ActivePipeline>>::iterator next_pipeline_iter_;

  DISALLOW_COPY_AND_ASSIGN(ActivePipelinesManager);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_EXECUTION_ACTIVE_PIPELINES_MANAGER_HPP_
