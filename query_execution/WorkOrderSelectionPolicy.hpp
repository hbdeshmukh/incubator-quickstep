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

#ifndef QUICKSTEP_QUERY_EXECUTION_WORK_ORDER_SELECTION_POLICY_HPP_
#define QUICKSTEP_QUERY_EXECUTION_WORK_ORDER_SELECTION_POLICY_HPP_

#include <cstddef>
#include <queue>
#include <stack>
#include <unordered_map>

#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {

/** \addtogroup QueryExecution
 *  @{
 */

/**
 * @brief Base class for a policy to select work orders for query execution.
 **/
class WorkOrderSelectionPolicy {
 public:
  /**
   * @brief Whether there is an available work order for execution.
   *
   * @return True if a work order is available. Otherwise, false.
   **/
  virtual bool hasWorkOrder() const = 0;

  /**
   * @brief Add work order.
   *
   * @param operator_index The operator index for added work order.
   **/
  virtual void addWorkOrder(const std::size_t operator_index) = 0;

  /**
   * @brief Decrement the count of the work order for the given operator.
   *
   * @param operator_index The index of the operator.
   * This field may be ignored for some policies.
   *
   * @return The ID of the operator whose work order was removed.
   */
  virtual std::size_t decrementWorkOrder(const std::size_t operator_index = 0) = 0;

  /**
   * @brief Choose the operator index for next workorder execution based on the policy.
   *
   * @return The operator index chosen for next workorder execution.
   **/
  virtual std::size_t getOperatorIndexForNextWorkOrder() = 0;

 protected:
  /**
   * @brief Constructor.
   **/
  WorkOrderSelectionPolicy() {}

 private:
  DISALLOW_COPY_AND_ASSIGN(WorkOrderSelectionPolicy);
};

/**
 * @brief Choose the next work order in a first-in-first-out manner.
 **/
class FifoWorkOrderSelectionPolicy final : public WorkOrderSelectionPolicy {
 public:
  /**
   * @brief Constructor.
   **/
  FifoWorkOrderSelectionPolicy() = default;

  bool hasWorkOrder() const override {
    return !work_orders_.empty();
  }

  void addWorkOrder(const std::size_t operator_index) override {
    work_orders_.push(operator_index);
  }

  std::size_t decrementWorkOrder(const std::size_t operator_index) override {
    DCHECK(hasWorkOrder());
    const std::size_t op_index = work_orders_.front();

    work_orders_.pop();
    return op_index;
  }

  std::size_t getOperatorIndexForNextWorkOrder() override {
    return decrementWorkOrder(0);
  }

 private:
  std::queue<std::size_t> work_orders_;

  DISALLOW_COPY_AND_ASSIGN(FifoWorkOrderSelectionPolicy);
};

/**
 * @brief Choose the next work order in a last-in-first-out manner.
 **/
class LifoWorkOrderSelectionPolicy final : public WorkOrderSelectionPolicy {
 public:
  /**
   * @brief Constructor.
   **/
  LifoWorkOrderSelectionPolicy() = default;

  bool hasWorkOrder() const override {
    return !work_orders_.empty();
  }

  void addWorkOrder(const std::size_t operator_index) override {
    work_orders_.push(operator_index);
  }

  std::size_t decrementWorkOrder(const size_t operator_index) override {
    DCHECK(hasWorkOrder());
    const std::size_t op = work_orders_.top();
    work_orders_.pop();

    return op;
  }

  std::size_t getOperatorIndexForNextWorkOrder() override {
    return decrementWorkOrder(0);
  }

 private:
  std::stack<std::size_t> work_orders_;

  DISALLOW_COPY_AND_ASSIGN(LifoWorkOrderSelectionPolicy);
};

class HashBasedWorkOrderSelectionPolicy final : public WorkOrderSelectionPolicy {
 public:
  HashBasedWorkOrderSelectionPolicy(const std::size_t num_operators) {
    for (std::size_t i = 0; i < num_operators; ++i) {
      workorders_count_[i] = 0;
    }
  }

  bool hasWorkOrder() const override {
    for (auto count_pair: workorders_count_) {
      if (hasWorkOrderHelper(count_pair.first)) {
        return true;
      }
    }
    return false;
  }

  void addWorkOrder(const std::size_t operator_index) override {
    ++workorders_count_[operator_index];
  }

  std::size_t decrementWorkOrder(const size_t operator_index) override {
    DCHECK(hasWorkOrderHelper(operator_index));
    --workorders_count_[operator_index];
    return operator_index;
  }

  /**
   * @brief Note that this function does not make sense for this policy.
   */
  std::size_t getOperatorIndexForNextWorkOrder() override {
    return 0;
  }

 private:

  bool hasWorkOrderHelper(const std::size_t operator_index) const {
    DCHECK_LE(operator_index, workorders_count_.size());
    return workorders_count_.at(operator_index) > 0;
  }

  std::unordered_map<std::size_t, std::size_t> workorders_count_;

  DISALLOW_COPY_AND_ASSIGN(HashBasedWorkOrderSelectionPolicy);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_EXECUTION_WORK_ORDER_SELECTION_POLICY_HPP_
