package net.sf.katta.master;

import net.sf.katta.protocol.InteractionProtocol;

public class LeaderContext {

  private final InteractionProtocol _protocol;
  private final IDeployPolicy _deployPolicy;

  public LeaderContext(InteractionProtocol protocol, IDeployPolicy deployPolicy) {
    _protocol = protocol;
    _deployPolicy = deployPolicy;
  }

  public InteractionProtocol getProtocol() {
    return _protocol;
  }

  public IDeployPolicy getDeployPolicy() {
    return _deployPolicy;
  }

}
