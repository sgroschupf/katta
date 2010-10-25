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

import java.util.List;

import net.sf.katta.master.MasterContext;
import net.sf.katta.operation.OperationId;
import net.sf.katta.operation.node.OperationResult;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.IndexMetaData;

import org.I0Itec.zkclient.ExceptionUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class BalanceIndexOperation extends AbstractIndexOperation {

  private static final long serialVersionUID = 1L;
  private final static Logger LOG = Logger.getLogger(AbstractIndexOperation.class);
  private final String _indexName;

  public BalanceIndexOperation(String indexName) {
    _indexName = indexName;
  }

  @Override
  public List<OperationId> execute(MasterContext context, List<MasterOperation> runningOperations) throws Exception {
    InteractionProtocol protocol = context.getProtocol();
    IndexMetaData indexMD = protocol.getIndexMD(_indexName);
    if (indexMD == null) {// could be undeployed in meantime
      LOG.info("skip balancing for index '" + _indexName + "' cause it is already undeployed");
      return null;
    }
    if (!canAndShouldRegulateReplication(protocol, indexMD)) {
      LOG.info("skip balancing for index '" + _indexName + "' cause there is no possible optimization");
      return null;
    }
    try {
      FileSystem fileSystem = context.getFileSystem(indexMD);
      Path path = new Path(indexMD.getPath());
      if (!fileSystem.exists(path)) {
        LOG.warn("skip balancing for index '" + _indexName + "' cause source '" + path + "' does not exists anymore");
        return null;
      }
    } catch (Exception e) {
      LOG.error("skip balancing of index '" + _indexName + "' cause failed to access source '" + indexMD.getPath()
              + "'", e);
      return null;
    }

    LOG.info("balancing shards for index '" + _indexName + "'");
    try {
      List<OperationId> operationIds = distributeIndexShards(context, indexMD, protocol.getLiveNodes(),
              runningOperations);
      return operationIds;
    } catch (Exception e) {
      ExceptionUtil.rethrowInterruptedException(e);
      LOG.error("failed to deploy balance " + _indexName, e);
      handleMasterDeployException(protocol, indexMD, e);
      return null;
    }
  }

  @Override
  public void nodeOperationsComplete(MasterContext context, List<OperationResult> results) throws Exception {
    LOG.info("balancing of index " + _indexName + " complete");
    IndexMetaData indexMD = context.getProtocol().getIndexMD(_indexName);
    if (indexMD != null) {// could be undeployed in meantime
      handleDeploymentComplete(context, results, indexMD, false);
    }
  }

  @Override
  public ExecutionInstruction getExecutionInstruction(List<MasterOperation> runningOperations) throws Exception {
    for (MasterOperation operation : runningOperations) {
      if (operation instanceof BalanceIndexOperation
              && ((BalanceIndexOperation) operation)._indexName.equals(_indexName)) {
        return ExecutionInstruction.CANCEL;
      }
    }
    return ExecutionInstruction.EXECUTE;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + ":" + Integer.toHexString(hashCode()) + ":" + _indexName;
  }
}
