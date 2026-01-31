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

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Best Response Time Balance URL Mapper
 *
 * <p>This mapper sorts backend URLs based on their response times, prioritizing
 * backends with better (lower) response times. It's inspired by MySQL Connector/J's
 * BestResponseTimeBalanceStrategy.
 *
 * <p><b>Strategy:</b>
 * <ul>
 *   <li>Tracks average response time for each backend</li>
 *   <li>Sorts backends by response time (ascending - best first)</li>
 *   <li>Backends with no response time data are placed at the beginning for exploration</li>
 *   <li>Failed connections are tracked and penalized</li>
 * </ul>
 *
 * <p><b>Usage:</b>
 * <pre>
 * String url = "jdbc:tidb://host1:4000,host2:4000/test?tidb.jdbc.url-mapper=bestresponsetime";
 * </pre>
 *
 * @see com.mysql.cj.jdbc.ha.BestResponseTimeBalanceStrategy
 */
public class BestResponseTimeBalanceMapper implements Function<Backend, String[]> {

  /**
   * Response time statistics for each backend URL
   * Thread-safe using atomic operations
   */
  public static class ResponseTimeStats {
    private final AtomicLong totalResponseTime = new AtomicLong(0);
    private final AtomicLong requestCount = new AtomicLong(0);
    private final AtomicLong failureCount = new AtomicLong(0);
    private final AtomicLong lastUpdateTime = new AtomicLong(System.currentTimeMillis());

    public void recordResponse(long responseTimeMs) {
      totalResponseTime.addAndGet(responseTimeMs);
      requestCount.incrementAndGet();
      lastUpdateTime.set(System.currentTimeMillis());
    }

    public void recordFailure() {
      failureCount.incrementAndGet();
      lastUpdateTime.set(System.currentTimeMillis());
    }

    /**
     * Get average response time in milliseconds.
     * Uses atomic snapshot to ensure consistent values.
     *
     * @return average response time, or -1 if no data available
     */
    public long getAverageResponseTime() {
      // Take atomic snapshots to ensure consistency
      long count = requestCount.get();
      if (count == 0) {
        return -1; // No data yet
      }
      long total = totalResponseTime.get();
      return total / count;
    }

    public long getFailureCount() {
      return failureCount.get();
    }

    public long getRequestCount() {
      return requestCount.get();
    }

    /**
     * Calculate failure rate as a value between 0.0 and 1.0.
     * Uses atomic snapshot to ensure consistent values.
     *
     * @return failure rate (0.0 = no failures, 1.0 = all failures)
     */
    public double getFailureRate() {
      // Take atomic snapshots to ensure consistency
      long reqCount = requestCount.get();
      long failCount = failureCount.get();
      long total = reqCount + failCount;
      if (total == 0) {
        return 0.0;
      }
      return (double) failCount / (double) total;
    }

    public long getLastUpdateTime() {
      return lastUpdateTime.get();
    }
  }

  // Map to store response time statistics for each backend URL
  private final Map<String, ResponseTimeStats> responseTimeMap = new ConcurrentHashMap<>();

  // Maximum penalty for failed backends (in milliseconds)
  private static final long FAILURE_PENALTY_MS = 5000;

  // Age weight for response time data (older data is less relevant)
  private static final long DATA_STALE_MS = 300000; // 5 minutes

  @Override
  public String[] apply(final Backend backend) {
    String[] input = backend.getBackend();

    // If only one backend, return it as-is
    if (input.length <= 1) {
      return input;
    }

    // Create a copy and sort by response time
    String[] sorted = Arrays.copyOf(input, input.length);

    Arrays.sort(sorted, new Comparator<String>() {
      @Override
      public int compare(String url1, String url2) {
        ResponseTimeStats stats1 = responseTimeMap.get(url1);
        ResponseTimeStats stats2 = responseTimeMap.get(url2);

        // Both have no data - keep original order
        if (stats1 == null && stats2 == null) {
          return 0;
        }

        // url1 has no data - prioritize it for exploration
        if (stats1 == null) {
          return -1;
        }

        // url2 has no data - prioritize it for exploration
        if (stats2 == null) {
          return 1;
        }

        // Both have data - compare effective response time
        long effectiveTime1 = getEffectiveResponseTime(stats1);
        long effectiveTime2 = getEffectiveResponseTime(stats2);

        return Long.compare(effectiveTime1, effectiveTime2);
      }
    });

    return sorted;
  }

  /**
   * Calculate the effective response time for a backend,
   * taking into account failures and data staleness
   */
  private long getEffectiveResponseTime(ResponseTimeStats stats) {
    long avgResponseTime = stats.getAverageResponseTime();

    // Apply failure penalty
    long failures = stats.getFailureCount();
    if (failures > 0) {
      avgResponseTime += (failures * FAILURE_PENALTY_MS);
    }

    // Apply staleness penalty (older data is less reliable)
    long age = System.currentTimeMillis() - stats.getLastUpdateTime();
    if (age > DATA_STALE_MS) {
      // Increase effective time for stale data
      avgResponseTime += (age - DATA_STALE_MS);
    }

    return avgResponseTime;
  }

  /**
   * Record a successful connection with its response time
   *
   * @param url The backend URL
   * @param responseTimeMs Response time in milliseconds
   */
  public void recordSuccess(String url, long responseTimeMs) {
    responseTimeMap.computeIfAbsent(url, k -> new ResponseTimeStats())
        .recordResponse(responseTimeMs);
  }

  /**
   * Record a failed connection attempt
   *
   * @param url The backend URL
   */
  public void recordFailure(String url) {
    responseTimeMap.computeIfAbsent(url, k -> new ResponseTimeStats())
        .recordFailure();
  }

  /**
   * Get response time statistics for a backend
   *
   * @param url The backend URL
   * @return Response time statistics, or null if no data available
   */
  public ResponseTimeStats getStats(String url) {
    return responseTimeMap.get(url);
  }

  /**
   * Clear all response time statistics
   * Useful for testing or manual reset
   */
  public void clearStats() {
    responseTimeMap.clear();
  }

  /**
   * Remove statistics for a specific backend
   *
   * @param url The backend URL to remove
   */
  public void removeStats(String url) {
    responseTimeMap.remove(url);
  }

  /**
   * Get the number of backends being tracked
   *
   * @return Number of backends with statistics
   */
  public int getTrackedBackendCount() {
    return responseTimeMap.size();
  }
}
