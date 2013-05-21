package org.apache.solr.handler.component;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.net.MalformedURLException;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.impl.BackupRequestLBHttpSolrServer;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.PluginInfo;

public class HttpBackupRequestShardHandlerFactory extends
    HttpShardHandlerFactory {
  private int maximumConcurrentRequests = 2;
  private int backupRequestDelay = 10 * 1000;
  // Configure the amount of time before a backup request is sent to the next server in the list in milliseconds
  static final String BACKUP_REQUEST_DELAY = "backupRequestDelay";
  // Configure the maximum request in flight due to backup requests
  static final String MAX_CONCURRENT_REQUESTS = "maximumConcurrentRequests";

  @Override
  public void init(PluginInfo info) {
    NamedList args = info.initArgs;
    this.backupRequestDelay = getParameter(args, BACKUP_REQUEST_DELAY, backupRequestDelay);
    this.maximumConcurrentRequests = getParameter(args, MAX_CONCURRENT_REQUESTS, maximumConcurrentRequests);
    super.init(info);
  }

  @Override
  protected LBHttpSolrServer createLoadbalancer(HttpClient httpClient) {
    try {
      return new BackupRequestLBHttpSolrServer(
          httpClient, getThreadPoolExecutor(),
          maximumConcurrentRequests, backupRequestDelay);
    } catch (MalformedURLException e) {
      // should be impossible since we're not passing any URLs here
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }
}
