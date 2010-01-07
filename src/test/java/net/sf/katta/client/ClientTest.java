package net.sf.katta.client;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import net.sf.katta.AbstractTest;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.util.ClientConfiguration;
import net.sf.katta.util.ISleepServer;

import org.junit.Test;

public class ClientTest extends AbstractTest {

  @Test
  public void testClose() throws Exception {
    InteractionProtocol protocol = mock(InteractionProtocol.class);
    Client client = new Client(ISleepServer.class, new DefaultNodeSelectionPolicy(), protocol,
            new ClientConfiguration());
    client.close();

    verify(protocol).unregisterComponent(client);
    verify(protocol).disconnect();
  }
}
