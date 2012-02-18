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
package net.sf.katta.tool.loadtest;

import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import net.sf.katta.operation.node.AbstractNodeOperationMockTest;
import net.sf.katta.testutil.mockito.AlmostVerificationMode;
import net.sf.katta.tool.loadtest.query.AbstractQueryExecutor;

import org.junit.Test;

public class LoadTestNodeOperationTest extends AbstractNodeOperationMockTest {

  @Test
  public void testNodeOperation() throws Exception {
    AbstractQueryExecutor queryExecutor = mock(AbstractQueryExecutor.class);
    String[] queries = new String[] { "a", "b", "c" };
    when(queryExecutor.getQueries()).thenReturn(queries);
    final int queryRate = 10;
    final int runTime = 2000;
    LoadTestNodeOperation nodeOperation = new LoadTestNodeOperation(queryExecutor, queryRate, runTime);
    long startTime = System.currentTimeMillis();
    nodeOperation.execute(_context);
    long endTime = System.currentTimeMillis();
    assertThat(endTime - startTime, almostEquals(runTime, 500));

    verify(queryExecutor).init(_context);
    verify(queryExecutor, new AlmostVerificationMode(queryRate * runTime / 1000, 5)).execute(eq(_context),
            argThat(isIn(queries)));
  }
}
