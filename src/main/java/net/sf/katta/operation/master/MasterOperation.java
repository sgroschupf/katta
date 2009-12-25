/**
 * Copyright 2008 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.sf.katta.operation.master;

import java.io.Serializable;
import java.util.List;

import net.sf.katta.master.MasterContext;
import net.sf.katta.operation.OperationId;
import net.sf.katta.operation.node.OperationResult;

/**
 * An operation carried out by the master node.
 * 
 * If an {@link InterruptedException} is thrown during the operations
 * {@link #execute(MasterContext)} method (which can happen during master change
 * or zookeeper reconnect) the operation can either catch and handle or rethrow
 * it. Rethrowing it will lead to complete reexecution of the operation.
 * 
 */
public interface MasterOperation extends Serializable {

  static enum ExecutionInstruction {
    EXECUTE, CANCEL, ADD_TO_QUEUE_TAIL;
  }

  /**
   * Called before {@link #execute(MasterContext, List)} to evaluate if this
   * operation is blocked, delayed, etc by another running
   * {@link MasterOperation}.
   * 
   * @param runningOperations
   * @return instruction
   * @throws Exception
   */
  ExecutionInstruction getExecutionInstruction(List<MasterOperation> runningOperations) throws Exception;

  /**
   * @param context
   * @param runningOperations
   *          currently running {@link MasterOperation}s
   * @return null or a list of operationId which have to be completed before
   *         {@link #nodeOperationsComplete(MasterContext)} method is called.
   * @throws Exception
   */
  List<OperationId> execute(MasterContext context, List<MasterOperation> runningOperations) throws Exception;

  /**
   * Called when all operations are complete or the nodes of the incomplete
   * operations went down. This method is NOT called if
   * {@link #execute(MasterContext)} returns null or an emptu list of
   * {@link OperationId}s.
   * 
   * @param context
   * @param results
   */
  void nodeOperationsComplete(MasterContext context, List<OperationResult> results) throws Exception;

}
