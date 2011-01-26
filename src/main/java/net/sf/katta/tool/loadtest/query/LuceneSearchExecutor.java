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
package net.sf.katta.tool.loadtest.query;

import net.sf.katta.lib.lucene.ILuceneClient;
import net.sf.katta.lib.lucene.LuceneClient;
import net.sf.katta.node.NodeContext;
import net.sf.katta.util.ZkConfiguration;

import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

@SuppressWarnings("serial")
public class LuceneSearchExecutor extends AbstractQueryExecutor {

  private final int _count;
  private ILuceneClient _client;
  private final ZkConfiguration _zkConfOfTargetCluster;

  public LuceneSearchExecutor(String[] indices, String[] queries, ZkConfiguration zkConfOfTargetCluster, int count) {
    super(indices, queries);
    _zkConfOfTargetCluster = zkConfOfTargetCluster;
    _count = count;
  }

  @Override
  public void init(NodeContext nodeContext) throws Exception {
    _client = new LuceneClient(_zkConfOfTargetCluster);
  }

  @Override
  public void close(NodeContext nodeContext) throws Exception {
    _client.close();
    _client = null;
  }

  @Override
  public void execute(NodeContext nodeContext, String queryString) throws Exception {
    final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse(queryString);
    _client.search(query, _indices, _count);
  }

}
