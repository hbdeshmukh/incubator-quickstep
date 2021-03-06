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

#ifndef QUICKSTEP_QUERY_OPTIMIZER_RULES_REFERENCED_BASE_RELATIONS_HPP_
#define QUICKSTEP_QUERY_OPTIMIZER_RULES_REFERENCED_BASE_RELATIONS_HPP_

#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "query_optimizer/logical/Logical.hpp"
#include "query_optimizer/rules/Rule.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

class CatalogRelation;

namespace optimizer {

class OptimizerContext;

class ReferencedBaseRelations : public Rule<logical::Logical> {
 public:
  /**
   * @brief Constructor
   * @param optimizer_context The optimizer context.
   */
  explicit ReferencedBaseRelations(OptimizerContext *optimizer_context)
      : optimizer_context_(optimizer_context) {
  }

  std::string getName() const override { return "ReferencedBaseRelations"; }

  logical::LogicalPtr apply(const logical::LogicalPtr &input) override;

  /**
   * @brief Get the base relations referenced in a query.
   */
  std::vector<const CatalogRelation*> getReferencedBaseRelations() const {
    std::vector<const CatalogRelation*> ret;
    for (auto it = referenced_base_relations_.begin();
         it != referenced_base_relations_.end();
         ++it) {
      ret.push_back(it->second);
    }
    return ret;
  }

 private:
  void applyToNode(const logical::LogicalPtr &input);

  OptimizerContext *optimizer_context_;

  std::unordered_map<relation_id, const CatalogRelation*> referenced_base_relations_;

  DISALLOW_COPY_AND_ASSIGN(ReferencedBaseRelations);
};

}  // namespace optimizer
}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_OPTIMIZER_RULES_REFERENCED_BASE_RELATIONS_HPP_
