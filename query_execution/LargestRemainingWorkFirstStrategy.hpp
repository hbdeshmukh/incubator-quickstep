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

#ifndef QUICKSTEP_QUERY_EXECUTION_LARGEST_REMAINING_WORK_FIRST_STRATEGY_HPP_
#define QUICKSTEP_QUERY_EXECUTION_LARGEST_REMAINING_WORK_FIRST_STRATEGY_HPP_

#include "query_execution/IntraQuerySchedulingStrategy.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup QueryExecution
 *  @{
 */

class LargestRemainingWorkFirstStrategy : public IntraQuerySchedulingStrategy {
 public:
  /**
   * @brief Constructor.
   **/
  LargestRemainingWorkFirstStrategy() {
  }

  ~LargestRemainingWorkFirstStrategy() override {
  }

  int getNextOperator() override {
    return -1;
  }

  void informCompletionOfOperator(std::size_t operator_index) override {
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(LargestRemainingWorkFirstStrategy);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_EXECUTION_LARGEST_REMAINING_WORK_FIRST_STRATEGY_HPP_
