/*
 * Copyright 2024
 */

package com.tidb.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class DiscoveryThresholdClient {

  private static final String DEFAULT_URL =
      "jdbc:mysql://127.0.0.1:4000/test"
          + "?tidb.jdbc.url-mapper=roundrobin"
          + "&tidb.jdbc.discovery-threshold=3";
  private static final int DEFAULT_ITERATIONS = 200;
  private static final int DEFAULT_SLEEP_MS = 50;
  private static final int DEFAULT_THREADS = 1;
  private static final int DEFAULT_HOLD_MS = 0;

  public static void main(String[] args) throws Exception {
    String url = args.length > 0 && !args[0].trim().isEmpty() ? args[0] : DEFAULT_URL;
    int iterations = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_ITERATIONS;
    int sleepMs = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_SLEEP_MS;
    int threads = args.length > 3 ? Integer.parseInt(args[3]) : DEFAULT_THREADS;
    int holdMs = args.length > 4 ? Integer.parseInt(args[4]) : DEFAULT_HOLD_MS;
    if (threads < 1) {
      threads = DEFAULT_THREADS;
    }
    if (holdMs < 0) {
      holdMs = DEFAULT_HOLD_MS;
    }
    final String finalUrl = url;
    final int finalIterations = iterations;
    final int finalSleepMs = sleepMs;
    final int finalHoldMs = holdMs;

    Map<String, LongAdder> counts = new ConcurrentHashMap<>();
    LongAdder failures = new LongAdder();
    AtomicInteger total = new AtomicInteger();

    Runnable task = () -> {
      Driver driver = new Driver();
      Properties props = new Properties();
      props.setProperty("user", System.getProperty("tidb.user", "root"));
      props.setProperty("password", System.getProperty("tidb.password", ""));
      for (int i = 0; i < finalIterations; i++) {
        try (Connection conn = driver.connect(finalUrl, props);
            Statement stmt = conn.createStatement()) {
          stmt.execute("SELECT 1");
          if (!sleepMs(finalHoldMs)) {
            return;
          }
          String backend = resolveBackendId(stmt, conn);
          counts.computeIfAbsent(backend, (k) -> new LongAdder()).increment();
        } catch (SQLException e) {
          failures.increment();
          System.err.println("connection failed: " + e.getMessage());
        }

        if (!sleepMs(finalSleepMs)) {
          return;
        }
        int current = total.incrementAndGet();
        if (current % 50 == 0) {
          printStats(current, counts, failures);
        }
      }
    };

    if (threads == 1) {
      task.run();
    } else {
      ExecutorService exec = Executors.newFixedThreadPool(threads);
      for (int i = 0; i < threads; i++) {
        exec.submit(task);
      }
      exec.shutdown();
      exec.awaitTermination(10, TimeUnit.MINUTES);
    }

    printStats(total.get(), counts, failures);
  }

  private static String extractBackend(String jdbcUrl) {
    if (jdbcUrl == null) {
      return "unknown";
    }
    int start = jdbcUrl.indexOf("//");
    if (start < 0) {
      return jdbcUrl;
    }
    start += 2;
    int end = jdbcUrl.indexOf('/', start);
    if (end < 0) {
      end = jdbcUrl.length();
    }
    return jdbcUrl.substring(start, end);
  }

  private static String resolveBackendId(Statement stmt, Connection conn) throws SQLException {
    String port = queryVariable(stmt, "port");
    if (port != null) {
      String host = queryVariable(stmt, "hostname");
      if (host != null && !host.isEmpty()) {
        return host + ":" + port;
      }
      return "port=" + port;
    }
    String value = queryVariable(stmt, "tidb_server_id");
    if (value != null) {
      return "tidb_server_id=" + value;
    }
    value = queryVariable(stmt, "server_id");
    if (value != null) {
      return "server_id=" + value;
    }
    return extractBackend(conn.getMetaData().getURL());
  }

  private static String queryVariable(Statement stmt, String variable) throws SQLException {
    try (ResultSet rs = stmt.executeQuery("SELECT @@"+ variable)) {
      if (rs.next()) {
        return rs.getString(1);
      }
    } catch (SQLException e) {
      return null;
    }
    return null;
  }

  private static void printStats(int total, Map<String, LongAdder> counts, LongAdder failures) {
    List<String> keys = new ArrayList<>(counts.keySet());
    Collections.sort(keys);
    StringBuilder sb = new StringBuilder();
    sb.append("total=").append(total).append(" failures=").append(failures.sum());
    for (String key : keys) {
      sb.append(" ").append(key).append("=").append(counts.get(key).sum());
    }
    System.out.println(sb.toString());
  }

  private static boolean sleepMs(int durationMs) {
    if (durationMs <= 0) {
      return true;
    }
    try {
      Thread.sleep(durationMs);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }
}
