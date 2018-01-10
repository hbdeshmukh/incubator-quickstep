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

#ifndef QUICKSTEP_QUERY_EXECUTION_DAG_ANALYZER_HPP_
#define QUICKSTEP_QUERY_EXECUTION_DAG_ANALYZER_HPP_

#include <cstddef>
#include <memory>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include "query_execution/Pipeline.hpp"
#include "relational_operators/RelationalOperator.hpp"
#include "utility/DAG.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {

/** \addtogroup QueryExecution
 *  @{
 */

/**
 * @brief A class that processes a query plan DAG and produces pipelines of
 *        relational operators from the DAG.
 **/
class DAGAnalyzer {
 public:
  /**
   * @brief Constructor. The invocation of the constructor triggers the DAG
   *        analysis and the pipelines are produced.
   *
   * @param query_plan_dag The query plan DAG.
   **/
  explicit DAGAnalyzer(DAG<RelationalOperator, bool> *query_plan_dag)
      : query_plan_dag_(query_plan_dag) {
    findPipelines();
    pipeline_sequence_ = generateFinalPipelineSequence();
    createOperatorToPipelineLookup();
  }

  /**
   * @brief Get the number of pipelines in the DAG.
   **/
  const std::size_t getNumPipelines() const {
    return pipelines_.size();
  }

  /**
   * @brief Get the IDs of those pipelines of which operator_id is a part.
   *
   * @param operator_id The ID of the given operator.
   *
   * @return Pipeline IDs if such pipeline(s) exist.
   **/
  const std::vector<std::size_t> getPipelineID(const std::size_t operator_id) const {
    std::vector<std::size_t> result_pipelines;
    for (std::size_t pipeline_id = 0;
         pipeline_id < pipelines_.size();
         ++pipeline_id) {
      if (pipelines_[pipeline_id]->hasOperator(operator_id)) {
        result_pipelines.emplace_back(pipeline_id);
      }
    }
    return result_pipelines;
  }

  const std::vector<std::size_t>& getAllOperatorsInPipeline(
      const std::size_t pipeline_id) const {
    DCHECK_LT(pipeline_id, pipelines_.size());
    return pipelines_[pipeline_id]->getOperatorIDs();
  }

  const std::vector<std::size_t>& getPipelineSequence() const {
    return pipeline_sequence_;
  }

  const std::vector<std::size_t>& getEssentialPipelineSequence() const {
    return essential_pipeline_sequence_;
  }

  const bool isPipelineEssential(const std::size_t pid) const {
    return pipelines_[pid]->isEssential();
  }

  void visualizePipelines();

  /**
   * @brief Generate the sequence of essential pipelines.
   * @note This function is called recursively. The first call has to be made from the topmost pipeline
   * in the query plan.
   * @param topmost_pipeline_id The ID of the topmost pipeline.
   * @param sequence The resulting sequence. The front of this vector is the pipeline executed earliest, and the
   * back of the vector is the pipeline executed the latest.
   */
  void generateEssentialPipelineSequence(const size_t topmost_pipeline_id,
                                         std::vector<size_t> *sequence) const;

  std::vector<std::size_t> generateFinalPipelineSequence();

  std::vector<std::size_t> generateEssentialPipelines() const;

  void splitEssentialAndNonEssentialPipelines(
      const std::vector<std::size_t> &full_sequence,
      std::unordered_map<std::size_t, std::vector<std::size_t>> *non_essential_successors) const {
    std::size_t curr_pid = 0;
    for (const std::size_t pid : full_sequence) {
      if (isEssentialNode(pid)) {
        curr_pid = pid;
        (*non_essential_successors)[pid];
      } else {
        (*non_essential_successors)[curr_pid].emplace_back(pid);
      }
    }
  }

  std::size_t getPipelineIDForOperator(std::size_t operator_id) const {
    DCHECK(operator_to_pipeline_lookup_.find(operator_id) != operator_to_pipeline_lookup_.end());
    return operator_to_pipeline_lookup_.at(operator_id);
  }

  /**
   * @brief Check if src pipeline be fused with dst pipeline.
   **/
  bool canPipelinesBeFused(const std::size_t src_pipeline_id,
                           const std::size_t dst_pipeline_id) const;

 private:
  /**
   * @brief Information of a graph node.
   */
  struct NodeInfo {
    std::size_t id;
    std::string labels;
    std::string color;
  };

  /**
   * @brief Information of a graph edge.
   */
  struct EdgeInfo {
    EdgeInfo(std::size_t src, std::size_t dst, bool can_edges_be_fused)
        : src_node_id(src),
          dst_node_id(dst),
          can_be_fused(can_edges_be_fused) {}

    std::size_t src_node_id;
    std::size_t dst_node_id;
    bool can_be_fused;
  };

  void createOperatorToPipelineLookup();

  /**
   * @brief Find initial set of pipelines in the query plan DAG.
   **/
  void findPipelines();

  /**
   * @brief Find the total number of nodes in all pipelines.
   **/
  const std::size_t getTotalNodes();

  bool checkDisplayPipelineNode(const size_t pipeline_id) const;

  bool isEssentialNode(const size_t pipeline_id) const;

  void populateDependents(
      const size_t pipeline_id,
      std::vector<std::size_t> *final_sequence,
      std::unordered_map<size_t, size_t> *incoming_pipelines_count,
      std::vector<bool> *visited) const;

  void plotSinglePipeline(std::size_t pipeline_id,
                          std::vector<struct NodeInfo> *nodes,
                          std::vector<struct EdgeInfo> *edges) const;

  std::string visualizePipelinesHelper(const std::vector<struct NodeInfo> &pipelines_info,
                                const std::vector<struct EdgeInfo> &edges);


  /**
   * @brief Rearrange the essential pipelines in top-down order.
   * @param essential_pipelines The list of essential pipelines.
   */
  void rearrangeEssentialPipelines(std::vector<size_t> *essential_pipelines) const;

  std::size_t getEssentialOutPipelinesCount(const size_t pipeline_id) const;

  void markEssentialNodes();

  DAG<RelationalOperator, bool> *query_plan_dag_;
  std::vector<std::unique_ptr<Pipeline>> pipelines_;

  // The sequence of pipelines in the order of scheduling.
  std::vector<std::size_t> pipeline_sequence_;

  // Key = operator ID, value = pipeline ID to which this operator belongs.
  std::unordered_map<std::size_t, std::size_t> operator_to_pipeline_lookup_;

  std::vector<std::size_t> essential_pipeline_sequence_;

  DISALLOW_COPY_AND_ASSIGN(DAGAnalyzer);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_EXECUTION_DAG_ANALYZER_HPP_
