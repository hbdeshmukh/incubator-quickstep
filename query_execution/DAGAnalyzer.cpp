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

#include "query_execution/DAGAnalyzer.hpp"

#include <algorithm>
#include <cstddef>
#include <iostream>
#include <memory>
#include <queue>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "utility/DAG.hpp"

#include "glog/logging.h"

namespace quickstep {

void DAGAnalyzer::findPipelines() {
  // Key = node ID, value = whether the node has been visited or not.
  std::unordered_map<std::size_t, bool> visited_nodes;
  // Pair 1st element: Operator ID
  // Pair 2nd element: The index of the pipeline in pipelines_ vector to which
  // the 1st element belongs.
  std::queue<std::pair<std::size_t, std::size_t>> to_be_checked_nodes;
  // Traverse the DAG once. Find leaf nodes and create a pipeline with one node.
  for (std::size_t node_id = 0; node_id < query_plan_dag_->size(); ++node_id) {
    if (query_plan_dag_->getDependencies(node_id).empty()) {
      pipelines_.push_back(std::unique_ptr<Pipeline>(new Pipeline(node_id)));
      to_be_checked_nodes.emplace(node_id, pipelines_.size() - 1);
      // No check needed here, as we are visiting these nodes for the first time.
      visited_nodes[node_id] = true;
    }
  }
  while (!to_be_checked_nodes.empty()) {
    auto queue_front = to_be_checked_nodes.front();
    to_be_checked_nodes.pop();
    const std::size_t pipeline_id = queue_front.second;
    const std::size_t operator_id = queue_front.first;
    auto dependents = query_plan_dag_->getDependents(operator_id);
    for (auto dependent_pair : dependents) {
      const std::size_t dependent_id = dependent_pair.first;
      const bool dependent_in_new_pipeline =
          (query_plan_dag_->getDependencies(dependent_id).size() > 1u) ||
          (dependent_pair.second);
      if (dependent_in_new_pipeline) {
        // Start a new pipeline.
        if (visited_nodes.find(dependent_id) == visited_nodes.end()) {
          pipelines_.push_back(std::unique_ptr<Pipeline>(new Pipeline(dependent_id)));
          to_be_checked_nodes.emplace(dependent_id, pipelines_.size() - 1);
          visited_nodes[dependent_id] = true;
        }
      } else {
        // This means that pipelining is enabled on this link. Add this
        // dependent to the current pipeline being processed.
        if (visited_nodes.find(dependent_id) == visited_nodes.end()) {
          pipelines_[pipeline_id]->addOperatorToPipeline(dependent_id);
          to_be_checked_nodes.emplace(dependent_id, pipeline_id);
          visited_nodes[dependent_id] = true;
        }
      }
    }
  }
  // Make sure that all nodes belong to some pipeline exactly once.
  DCHECK_EQ(query_plan_dag_->size(), getTotalNodes());
}

const std::size_t DAGAnalyzer::getTotalNodes() {
  std::size_t total = 0;
  for (std::size_t i = 0; i < pipelines_.size(); ++i) {
    total += pipelines_[i]->size();
  }
  return total;
}

void DAGAnalyzer::visualizePipelines() {
  // First start with free pipelines and then recursively plot the pipelines
  // that are connected to them.
  std::vector<NodeInfo> pipelines_info;
  std::vector<EdgeInfo> edges;
  std::unordered_map<std::size_t, bool> visited_pipelines;
  std::queue<std::size_t> to_be_checked_pipelines;
  auto free_pipelines = getFreePipelinesStatic();
  for (std::size_t free_pipeline_id : free_pipelines) {
    pipelines_info.emplace_back(plotSinglePipeline(free_pipeline_id));
    auto connected_pipelines = getPipelinesConnectedToEnd(free_pipeline_id);
    for (auto connected_pid : connected_pipelines) {
      to_be_checked_pipelines.push(connected_pid);
      edges.emplace_back(free_pipeline_id, connected_pid);
    }
    visited_pipelines[free_pipeline_id] = true;
  }
  while (!to_be_checked_pipelines.empty()) {
    std::size_t next_pipeline = to_be_checked_pipelines.front();
    to_be_checked_pipelines.pop();
    if (visited_pipelines.find(next_pipeline) == visited_pipelines.end()) {
      pipelines_info.emplace_back(plotSinglePipeline(next_pipeline));
      auto connected_pipelines = getPipelinesConnectedToEnd(next_pipeline);
      for (auto connected_pid : connected_pipelines) {
        if (visited_pipelines.find(connected_pid) == visited_pipelines.end()) {
          to_be_checked_pipelines.push(connected_pid);
          edges.emplace_back(next_pipeline, connected_pid);
        }
      }
      visited_pipelines[next_pipeline] = true;
    }
  }
  std::cout << "Visualizing pipelines: # pipelines: " << pipelines_info.size()
            << " # edges: " << edges.size() << std::endl;
  std::cout << visualizePipelinesHelper(pipelines_info, edges);
}

std::string DAGAnalyzer::visualizePipelinesHelper(
    const std::vector<struct NodeInfo> &pipelines_info,
    const std::vector<struct EdgeInfo> &edges) {
  // Format output graph
  std::ostringstream graph_oss;
  graph_oss << "digraph g {\n";
  graph_oss << "  rankdir=BT\n";
  graph_oss << "  node [penwidth=2]\n";
  graph_oss << "  edge [fontsize=16 fontcolor=gray penwidth=2]\n\n";

  // Format nodes
  for (const auto &pipeline : pipelines_info) {
    graph_oss << pipeline.id << " [ label= \"";
    graph_oss << pipeline.labels;
    graph_oss << "\"]\n";
  }

  // Format edges
  for (const EdgeInfo &edge_info : edges) {
    graph_oss << "  " << edge_info.src_node_id << " -> "
              << edge_info.dst_node_id << " [ ";
    graph_oss << "]\n";
  }
  graph_oss << "}\n";
  return graph_oss.str();
}

struct DAGAnalyzer::NodeInfo DAGAnalyzer::plotSinglePipeline(std::size_t pipeline_id) const {
  NodeInfo current_node;
  current_node.id = pipeline_id;
  current_node.labels += "[";
  current_node.labels += std::to_string(pipeline_id);
  current_node.labels += "] Operators: ";
  auto nodes_in_pipeline = pipelines_[pipeline_id]->getOperatorIDs();
  for (auto node_id : nodes_in_pipeline) {
    current_node.labels += std::to_string(node_id);
    current_node.labels += "-";
  }
  current_node.labels += "";
  return current_node;
}

}  // namespace quickstep
