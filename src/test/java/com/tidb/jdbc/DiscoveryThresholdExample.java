/*
 * Copyright 2021 TiDB Project Authors.
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

package com.tidb.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Example demonstrating how to use the tidb.jdbc.discovery-threshold configuration
 *
 * <p>The discovery-threshold parameter controls how many backend failures trigger
 * an automatic reload of the backend discovery process.
 *
 * <p>Default value: 3 (reload when 3 or more backends have failed)
 */
public class DiscoveryThresholdExample {

  /**
   * Example 1: Using default threshold (3)
   *
   * <p>Connection string:
   * jdbc:tidb://localhost:4000/test
   *
   * <p>Behavior: When 3 backends fail, automatically trigger rediscovery
   */
  public static void example1_defaultThreshold() throws SQLException {
    String url = "jdbc:tidb://127.0.0.1:4000/test";
    Connection conn = DriverManager.getConnection(url);
    // use connection...
    conn.close();
  }

  /**
   * Example 2: Custom threshold - more sensitive (reload after 1 failure)
   *
   * <p>Connection string:
   * jdbc:tidb://127.0.0.1:4000/test?tidb.jdbc.discovery-threshold=1
   *
   * <p>Behavior: Immediately trigger rediscovery after ANY backend fails
   *
   * <p>Use case: Critical systems where you want immediate recovery
   */
  public static void example2_thresholdOne() throws SQLException {
    String url = "jdbc:tidb://127.0.0.1:4000/test?tidb.jdbc.discovery-threshold=1";
    Connection conn = DriverManager.getConnection(url);
    // use connection...
    conn.close();
  }

  /**
   * Example 3: Custom threshold - less sensitive (reload after 5 failures)
   *
   * <p>Connection string:
   * jdbc:tidb://127.0.0.1:4000/test?tidb.jdbc.discovery-threshold=5
   *
   * <p>Behavior: Only trigger rediscovery after 5 backends fail
   *
   * <p>Use case: Large clusters where occasional single node failures are acceptable
   */
  public static void example3_thresholdFive() throws SQLException {
    String url = "jdbc:tidb://127.0.0.1:4000/test?tidb.jdbc.discovery-threshold=5";
    Connection conn = DriverManager.getConnection(url);
    // use connection...
    conn.close();
  }

  /**
   * Example 4: Using Properties object
   *
   * <p>Alternative way to configure threshold using Properties
   */
  public static void example4_usingProperties() throws SQLException {
    String url = "jdbc:tidb://127.0.0.1:4000/test";
    Properties props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", "");
    props.setProperty("tidb.jdbc.discovery-threshold", "2");

    Connection conn = DriverManager.getConnection(url, props);
    // use connection...
    conn.close();
  }

  /**
   * Example 5: Combined with other configurations
   *
   * <p>Combining discovery-threshold with other TiDB JDBC parameters
   */
  public static void example5_combinedConfig() throws SQLException {
    String url = "jdbc:tidb://127.0.0.1:4000/test?"
        + "tidb.jdbc.discovery-threshold=2"
        + "&tidb.jdbc.min-discovery-interval=500"
        + "&tidb.jdbc.max-discovery-interval=60000";

    Connection conn = DriverManager.getConnection(url);
    // use connection...
    conn.close();
  }

  /**
   * Invalid configurations examples (will throw IllegalArgumentException)
   */
  public static class InvalidExamples {

    public static void invalidExample1_zero() {
      // INVALID: threshold cannot be 0
      String url = "jdbc:tidb://127.0.0.1:4000/test?tidb.jdbc.discovery-threshold=0";
      try {
        Connection conn = DriverManager.getConnection(url);
      } catch (SQLException e) {
        // Will wrap IllegalArgumentException
        System.out.println("Error: " + e.getCause().getMessage());
        // Output: Error: tidb.jdbc.discovery-threshold must be >= 1, got: 0
      }
    }

    public static void invalidExample2_negative() {
      // INVALID: threshold cannot be negative
      String url = "jdbc:tidb://127.0.0.1:4000/test?tidb.jdbc.discovery-threshold=-1";
      try {
        Connection conn = DriverManager.getConnection(url);
      } catch (SQLException e) {
        System.out.println("Error: " + e.getCause().getMessage());
      }
    }

    public static void invalidExample3_nonNumeric() {
      // INVALID: threshold must be a number
      String url = "jdbc:tidb://127.0.0.1:4000/test?tidb.jdbc.discovery-threshold=abc";
      try {
        Connection conn = DriverManager.getConnection(url);
      } catch (SQLException e) {
        System.out.println("Error: " + e.getCause().getMessage());
        // Output: Error: Invalid value for tidb.jdbc.discovery-threshold: abc
      }
    }
  }

  /**
   * Recommended configurations for different scenarios
   */
  public static class Recommendations {

    /**
     * Small cluster (1-3 nodes)
     * Recommendation: threshold=1 (immediate recovery)
     */
    public static String smallCluster() {
      return "jdbc:tidb://host1:4000/test?tidb.jdbc.discovery-threshold=1";
    }

    /**
     * Medium cluster (4-10 nodes)
     * Recommendation: threshold=2-3 (balanced)
     */
    public static String mediumCluster() {
      return "jdbc:tidb://host1:4000/test?tidb.jdbc.discovery-threshold=3";
    }

    /**
     * Large cluster (10+ nodes)
     * Recommendation: threshold=5-10 (tolerate some failures)
     */
    public static String largeCluster() {
      return "jdbc:tidb://host1:4000/test?tidb.jdbc.discovery-threshold=10";
    }

    /**
     * Mission-critical systems
     * Recommendation: threshold=1 (fastest recovery)
     */
    public static String missionCritical() {
      return "jdbc:tidb://host1:4000/test?tidb.jdbc.discovery-threshold=1"
          + "&tidb.jdbc.min-discovery-interval=500";
    }
  }

  /**
   * How threshold-based reload works
   *
   * <p>Logic Flow:
   *
   * <pre>
   * Connection Request
   *        ↓
   * Check: (failedBackends >= threshold) OR (timeSinceLastReload > minInterval)?
   *        ↓
   *    YES → getAndReload() → Rediscover all backends → Clear failed list
   *        ↓
   *    NO  → get() → Return cached backends (excluding failed ones)
   * </pre>
   *
   * <p>Example with threshold=3:
   * <ul>
   *   <li>Backend 1 fails → failedBackends=1 → No reload</li>
   *   <li>Backend 2 fails → failedBackends=2 → No reload</li>
   *   <li>Backend 3 fails → failedBackends=3 → Trigger reload! ✓</li>
   * </ul>
   */
  public static void howItWorks() {
    // See javadoc above
  }

  /**
   * Main method for manual testing
   */
  public static void main(String[] args) {
    System.out.println("Discovery Threshold Configuration Examples");
    System.out.println("===========================================");
    System.out.println();

    System.out.println("Example 1: Default threshold (3)");
    System.out.println("  URL: jdbc:tidb://127.0.0.1:4000/test");
    System.out.println();

    System.out.println("Example 2: Immediate reload (threshold=1)");
    System.out.println("  URL: jdbc:tidb://127.0.0.1:4000/test?tidb.jdbc.discovery-threshold=1");
    System.out.println();

    System.out.println("Example 3: Relaxed reload (threshold=10)");
    System.out.println("  URL: jdbc:tidb://127.0.0.1:4000/test?tidb.jdbc.discovery-threshold=10");
    System.out.println();

    System.out.println("Recommendations:");
    System.out.println("  Small cluster (1-3 nodes): threshold=1");
    System.out.println("  Medium cluster (4-10 nodes): threshold=3");
    System.out.println("  Large cluster (10+ nodes): threshold=10");
    System.out.println("  Mission-critical: threshold=1 with min-interval=500");
  }
}
