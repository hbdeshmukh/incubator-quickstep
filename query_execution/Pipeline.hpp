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

#ifndef QUICKSTEP_QUERY_EXECUTION_PIPELINE_HPP_
#define QUICKSTEP_QUERY_EXECUTION_PIPELINE_HPP_

#include <algorithm>
#include <cstddef>
#include <iostream>
#include <iterator>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {

/** \addtogroup QueryExecution
 *  @{
 */

/**
 * @brief A class that represents the connection between two pipelines.
 **/
class PipelineConnection {
 public:
  /**
   * @brief Constructor.
   **/
  PipelineConnection(std::size_t connected_pipeline_id,
                     bool is_dependent,
                     bool can_pipelines_be_fused)
      : connected_pipeline_id_(connected_pipeline_id),
        is_dependent_(is_dependent),
        can_be_fused_(can_pipelines_be_fused) {}

  bool operator==(const PipelineConnection &pc) const {
    return connected_pipeline_id_ == pc.connected_pipeline_id_ &&
           is_dependent_ == pc.is_dependent_ &&
           can_be_fused_ == pc.can_be_fused_;
  }

  friend std::ostream& operator<< (std::ostream &oss, const PipelineConnection &pc) {
    oss << "(Pipeline: " << pc.connected_pipeline_id_;
    if (pc.is_dependent_) {
      oss << ", DEPENDENT, ";
    } else {
      oss << ", dependency, ";
    }
    if (pc.can_be_fused_) {
      oss << " fusable)" << std::endl;
    } else {
      oss << " NOT fusable)" << std::endl;
    }
    return oss;
  }

  std::size_t getConnectedPipelineID() const {
    return connected_pipeline_id_;
  }

  /**
   * @brief Check if the connected pipeline (i.e. the pipeline with id =
   *        connected_pipeline_id) is dependent on the given pipeline.
   **/
  bool checkConnectedPipelineIsDependent() const {
    return is_dependent_;
  }

  bool canPipelinesBeFused() const {
    return can_be_fused_;
  }

 private:
  std::size_t connected_pipeline_id_;
  // Whether the connected_pipeline is a dependent of the given pipeline.
  bool is_dependent_;
  bool can_be_fused_;
};

/**
 * @brief A class that abstracts a pipeline of relational operators in a query
 *        plan DAG.
 **/
class Pipeline {
 public:
  /**
   * @brief Constructor.
   *
   * @param operator_ids The IDs of the operator belonging to the pipeline.
   **/
  explicit Pipeline(const std::vector<std::size_t> &operator_ids)
      : operators_(operator_ids), is_essential_(false) {}

  /**
   * @brief Constructor for a single node pipeline.
   *
   * @param operator_id The ID of the operator belonging to the pipeline.
   **/
  explicit Pipeline(const std::size_t operator_id) {
    operators_.emplace_back(operator_id);
  }

  /**
   * @brief Get the IDs of the operators belonging to the pipeline.
   **/
  const std::vector<std::size_t>& getOperatorIDs() const {
    return operators_;
  }

  /**
   * @brief Add an operator to the pipeline.
   *
   * @param operator_id The ID of the operator.
   **/
  void addOperatorToPipeline(const std::size_t operator_id) {
    DCHECK(!hasOperator(operator_id));
    operators_.emplace_back(operator_id);
  }

  /**
   * @brief Check if the given operator belongs to the pipeline.
   **/
  bool hasOperator(const std::size_t operator_id) const {
    return std::find(operators_.begin(), operators_.end(), operator_id) !=
           operators_.end();
  }

  void markEssential() {
    is_essential_ = true;
  }

  bool isEssential() const {
    return is_essential_;
  }

  /**
   * @brief Get the size of the pipeline.
   **/
  std::size_t size() const {
    return operators_.size();
  }

  /**
   * @brief Get the starting node of the pipeline.
   **/
  std::size_t getPipelineStartPoint() const {
    DCHECK(!operators_.empty());
    return operators_.front();
  }

  /**
   * @brief Get the ending node of the pipeline.
   **/
  std::size_t getPipelineEndPoint() const {
    DCHECK(!operators_.empty());
    return operators_.back();
  }

  void linkPipeline(std::size_t connected_pipeline_id,
                    std::size_t connected_operator_id,
                    bool is_dependent,
                    bool can_be_fused) {
    if (!checkConnectionExists(connected_operator_id,
                               connected_pipeline_id,
                               is_dependent,
                               can_be_fused)) {
      connected_pipelines_.emplace_back(
          connected_pipeline_id, is_dependent, can_be_fused);
    }
  }

  const std::vector<PipelineConnection>& getAllConnectedPipelines() const {
    return connected_pipelines_;
  }

  const std::vector<PipelineConnection> getAllIncomingPipelines() const {
    std::vector<PipelineConnection> incoming_pipelines;
    for (auto conn : connected_pipelines_) {
      if (!conn.checkConnectedPipelineIsDependent()) {
        incoming_pipelines.emplace_back(conn);
      }
    }
    return incoming_pipelines;
  }

  const std::vector<PipelineConnection> getAllOutgoingPipelines() const {
    std::vector<PipelineConnection> outgoing_pipelines;
    for (auto conn : connected_pipelines_) {
      if (conn.checkConnectedPipelineIsDependent()) {
        outgoing_pipelines.emplace_back(conn);
      }
    }
    return outgoing_pipelines;
  }

  const std::vector<std::size_t> getAllBlockingDependencies() const {
    std::vector<std::size_t> blocking_dependencies;
    for (const PipelineConnection &pc : getAllConnectedPipelines()) {
      if (!pc.checkConnectedPipelineIsDependent() && !pc.canPipelinesBeFused()) {
        blocking_dependencies.emplace_back(pc.getConnectedPipelineID());
      }
    }
    return blocking_dependencies;
  }

 private:
  bool checkConnectionExists(const std::size_t connected_operator_id,
                             const std::size_t connected_pipeline_id,
                             bool is_dependent,
                             bool can_be_fused) const {
    PipelineConnection pc(connected_pipeline_id, is_dependent, can_be_fused);
    return std::find(connected_pipelines_.begin(),
                     connected_pipelines_.end(),
                     pc) != std::end(connected_pipelines_);
  }

  std::vector<std::size_t> operators_;

  // Key = operator ID, value = connected pipeline to the key operator.
  std::vector<PipelineConnection> connected_pipelines_;

  // Whether the pipeline is essential or non-essential (DestroyHash, DropTable)
  bool is_essential_;

  DISALLOW_COPY_AND_ASSIGN(Pipeline);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_EXECUTION_PIPELINE_HPP_
