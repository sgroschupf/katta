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
package net.sf.katta.protocol.upgrade;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import net.sf.katta.AbstractTest;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.Version;

import org.junit.Test;

public class UpgradeRegistryTest extends AbstractTest {

  private UpgradeAction _upgradeAction = mock(UpgradeAction.class);
  private InteractionProtocol _protocol = mock(InteractionProtocol.class);

  @Test
  public void testNoUpgradeFound() throws Exception {
    when(_protocol.getVersion()).thenReturn(createVersion("0.1"));
    Version distributionVersion = createVersion("0.2");
    assertNull(UpgradeRegistry.findUpgradeAction(_protocol, distributionVersion));
  }

  @Test
  public void testNoUpgradeNeeded() throws Exception {
    UpgradeRegistry.registerUpgradeAction("0.3", "0.4", _upgradeAction);
    when(_protocol.getVersion()).thenReturn(createVersion("0.4"));
    Version distributionVersion = createVersion("0.4");
    assertNull(UpgradeRegistry.findUpgradeAction(_protocol, distributionVersion));
  }

  @Test
  public void testUpgrade() throws Exception {
    UpgradeRegistry.registerUpgradeAction("0.3", "0.4", _upgradeAction);

    when(_protocol.getVersion()).thenReturn(createVersion("0.3"));
    Version distributionVersion = createVersion("0.4");
    assertNotNull(UpgradeRegistry.findUpgradeAction(_protocol, distributionVersion));
  }

  @Test
  public void testUpgradeWithDevVersion() throws Exception {
    UpgradeRegistry.registerUpgradeAction("0.3", "0.4", _upgradeAction);

    when(_protocol.getVersion()).thenReturn(createVersion("0.3"));
    Version distributionVersion = createVersion("0.4-dev");
    assertNotNull(UpgradeRegistry.findUpgradeAction(_protocol, distributionVersion));
  }

  private Version createVersion(String number) {
    return new Version(number, "-", "-", "-");
  }
}
