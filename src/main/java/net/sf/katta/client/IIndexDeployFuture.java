package net.sf.katta.client;

import java.util.concurrent.Future;

import net.sf.katta.index.IndexMetaData.IndexState;

/**
 * 
 * Future for an index deployment.
 * 
 * @see Future for concept of a future
 */
public interface IIndexDeployFuture {

  /**
   * This method blocks until the index has been successfully deployed or the
   * deployment failed.
   */
  IndexState joinDeployment() throws InterruptedException;

  /**
   * This method blocks until the index has been successfully deployed or the
   * deployment failed or maxWaitMillis has exceeded.
   */
  IndexState joinDeployment(long maxWaitMillis) throws InterruptedException;

  /**
   * This method blocks until the each shard of the index has been successfully
   * deployed one time and the index deployment enters the state of replicating.
   */
  IndexState joinReplication() throws InterruptedException;

  IndexState getState();

}
