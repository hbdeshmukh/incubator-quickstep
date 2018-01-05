# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Declare micro-libs:
add_library(quickstep_queryexecution_intrapipelinescheduling_IntraPipelineScheduling ../../empty_src.cpp IntraPipelineScheduling.hpp)
add_library(quickstep_queryexecution_intrapipelinescheduling_FIFO ../../empty_src.cpp FIFO.hpp)
add_library(quickstep_queryexecution_intrapipelinescheduling_LIFO ../../empty_src.cpp LIFO.hpp)
add_library(quickstep_queryexecution_intrapipelinescheduling_Random ../../empty_src.cpp Random.hpp)

# Link dependencies:
target_link_libraries(quickstep_queryexecution_intrapipelinescheduling_IntraPipelineScheduling
                      quickstep_utility_Macros)
target_link_libraries(quickstep_queryexecution_intrapipelinescheduling_FIFO
                      glog
                      quickstep_queryexecution_intrapipelinescheduling_IntraPipelineScheduling
                      quickstep_queryexecution_DAGAnalyzer
                      quickstep_queryexecution_QueryExecutionState
                      quickstep_queryexecution_WorkOrdersContainer
                      quickstep_utility_Macros)
target_link_libraries(quickstep_queryexecution_intrapipelinescheduling_LIFO
                      glog
                      quickstep_queryexecution_intrapipelinescheduling_IntraPipelineScheduling
                      quickstep_queryexecution_DAGAnalyzer
                      quickstep_queryexecution_QueryExecutionState
                      quickstep_queryexecution_WorkOrdersContainer
                      quickstep_utility_Macros)
target_link_libraries(quickstep_queryexecution_intrapipelinescheduling_Random
                      glog
                      quickstep_queryexecution_intrapipelinescheduling_IntraPipelineScheduling
                      quickstep_queryexecution_DAGAnalyzer
                      quickstep_queryexecution_QueryExecutionState
                      quickstep_queryexecution_WorkOrdersContainer
                      quickstep_utility_Macros)
# Module all-in-one library:
add_library(quickstep_queryexecution_intrapipelinescheduling ../../empty_src.cpp IntraPipelineSchedulingModule.hpp)
target_link_libraries(quickstep_queryexecution_intrapipelinescheduling
                      quickstep_queryexecution_intrapipelinescheduling_FIFO
                      quickstep_queryexecution_intrapipelinescheduling_IntraPipelineScheduling
                      quickstep_queryexecution_intrapipelinescheduling_LIFO
                      quickstep_queryexecution_intrapipelinescheduling_Random)