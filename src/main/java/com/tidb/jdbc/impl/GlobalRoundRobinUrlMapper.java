/*
 * Copyright 2020 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tidb.jdbc.impl;

import com.tidb.jdbc.conf.ConnUrlParser;
import com.tidb.jdbc.conf.HostInfo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class GlobalRoundRobinUrlMapper implements Function<Backend, String[]> {

  private static final String QUERY_PROCESSLIST_SQL = "SELECT C.INSTANCE,C.HOST,L.HOST AS C_HOST,SI.PORT FROM INFORMATION_SCHEMA.CLUSTER_PROCESSLIST C " +
          "LEFT JOIN INFORMATION_SCHEMA.PROCESSLIST L ON L.HOST = C.HOST " +
          "LEFT JOIN (select CONCAT(IP,\":\",STATUS_PORT) as HOST,PORT from INFORMATION_SCHEMA.TIDB_SERVERS_INFO) AS SI ON SI.HOST = C.INSTANCE";

  private Map<String,Integer> querybackendMap(Connection connection){
    Map<String,Integer> backendMap = new ConcurrentHashMap<>();
    String localHost = null;
    try (final Statement statement = connection.createStatement();
         final ResultSet resultSet = statement.executeQuery(QUERY_PROCESSLIST_SQL)) {
      while (resultSet.next()) {
        final String instance = resultSet.getString("INSTANCE");
        final String cHost = resultSet.getString("C_HOST");
        final int port = resultSet.getInt("PORT");
        if(instance != null && !"".equals(instance)){
          String ip = instance.split(":")[0];
          String host = ip + ":" + port;
          if(cHost != null && !"".equals(cHost)){
            localHost = host;
          }
          int count = backendMap.get(host) == null ? 1 : backendMap.get(host) + 1;
          backendMap.put(host,count);
        }
      }
    }catch (Exception e){
      throw new RuntimeException("No available endpoint to discover backends");
    }
    if(backendMap.containsKey(localHost)){
      backendMap.put(localHost,backendMap.get(localHost)-1);
    }
    return backendMap;
  }
  public String[] apply(final Backend backend) {
    String[] input = backend.getBackend();
    if(backend.getDriver() == null){
      return input;
    }
    try (final Connection connection = backend.getDriver().connect(input[0],backend.getInfo())){
      connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      Map<String,Integer> backendMap = querybackendMap(connection);
      Map<String,String> urlMap = new HashMap<>();
      for (String url : input) {
        ConnUrlParser connStrParser = ConnUrlParser.parseConnectionString(url);
        for (HostInfo hostInfo : connStrParser.getHosts()) {
          String host = hostInfo.getHost() + ":" + hostInfo.getPort();
          if (!backendMap.containsKey(host)) {
            return new String[]{url};
          }
          urlMap.put(host,url);
        }
      }
      List<String> result = new ArrayList<>();
      backendMap.entrySet().stream().sorted(Map.Entry.comparingByValue())
              .forEachOrdered(e -> {
                if(urlMap.containsKey(e.getKey())){
                  result.add(urlMap.get(e.getKey()));
                }});
      return result.toArray(new String[0]);
    } catch (Exception e){
      throw new RuntimeException("No available endpoint to discover backends");
    }
  }
}
