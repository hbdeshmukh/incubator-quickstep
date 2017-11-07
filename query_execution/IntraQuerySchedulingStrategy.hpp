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

#ifndef QUICKSTEP_QUERY_EXECUTION_INTRA_QUERY_SCHEDULING_STRATEGY_HPP_
#define QUICKSTEP_QUERY_EXECUTION_INTRA_QUERY_SCHEDULING_STRATEGY_HPP_

#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup QueryExecution
 *  @{
 */

/**
 * @brief A virtual base class for implementing a scheculing strategy for a
 *        single query.
 **/
class IntraQuerySchedulingStrategy {
 public:
  virtual ~IntraQuerySchedulingStrategy() {
  }

  /**
   * @brief Get the next operator ID for scheduling a work order.
   *
   * @return The ID of the next operator for scheduling, or -1 if no such
   *         operator exists.
   **/
  virtual int getNextOperator() = 0;

  /**
   * @brief Inform the strategy that an operator has completed its execution.
   *
   * @param operator_index The ID of the operator.
   **/
  virtual void informCompletionOfOperator(std::size_t operator_index) = 0;

 protected:
  IntraQuerySchedulingStrategy() {}

 private:
  DISALLOW_COPY_AND_ASSIGN(IntraQuerySchedulingStrategy);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_EXECUTION_INTRA_QUERY_SCHEDULING_STRATEGY_HPP_
