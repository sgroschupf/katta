package net.sf.katta.node;

import org.apache.hadoop.io.Writable;

public interface IRequestHandler {

  /**
   * Handle requests from the client, for example a search query or a getDetails request
   */
  Writable handle(Writable request);

}
