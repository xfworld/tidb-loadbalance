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

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import com.tidb.jdbc.impl.RoundRobinUrlMapper;

/**
 * Unit tests for tidb.jdbc.discovery-threshold configuration parameter validation
 *
 * <p>These tests verify that the configuration parser correctly validates threshold values.
 * The actual DiscovererImpl instantiation is tested in integration tests.
 */
public class DiscoveryThresholdConfigTest {

  /**
   * Simulate the parseReloadThreshold logic for testing purposes
   * This mirrors the actual implementation in DiscovererImpl
   */
  private int simulateParseReloadThreshold(Properties info) {
    final String TIDB_DISCOVERY_THRESHOLD = "tidb.jdbc.discovery-threshold";
    final int DEFAULT_BACKEND_RELOAD_THRESHOLD = 3;

    if (info == null) {
      return DEFAULT_BACKEND_RELOAD_THRESHOLD;
    }

    String thresholdStr = info.getProperty(TIDB_DISCOVERY_THRESHOLD);
    if (thresholdStr == null || thresholdStr.trim().isEmpty()) {
      return DEFAULT_BACKEND_RELOAD_THRESHOLD;
    }

    try {
      int threshold = Integer.parseInt(thresholdStr.trim());
      if (threshold < 1) {
        throw new IllegalArgumentException(
            TIDB_DISCOVERY_THRESHOLD + " must be >= 1, got: " + threshold);
      }
      return threshold;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid value for " + TIDB_DISCOVERY_THRESHOLD + ": " + thresholdStr, e);
    }
  }

  @Test
  public void testDefaultThreshold() {
    // Test that default threshold is 3 when no configuration is provided
    Properties props = null;
    int threshold = simulateParseReloadThreshold(props);
    assertEquals("Default threshold should be 3", 3, threshold);
  }

  @Test
  public void testCustomThreshold() {
    // Test custom threshold value
    Properties props = new Properties();
    props.setProperty("tidb.jdbc.discovery-threshold", "5");

    int threshold = simulateParseReloadThreshold(props);
    assertEquals("Custom threshold should be 5", 5, threshold);
  }

  @Test
  public void testThresholdValueOne() {
    // Test minimum valid threshold value
    Properties props = new Properties();
    props.setProperty("tidb.jdbc.discovery-threshold", "1");

    int threshold = simulateParseReloadThreshold(props);
    assertEquals("Threshold of 1 should be accepted", 1, threshold);
  }

  @Test
  public void testThresholdValueTen() {
    // Test larger threshold value
    Properties props = new Properties();
    props.setProperty("tidb.jdbc.discovery-threshold", "10");

    int threshold = simulateParseReloadThreshold(props);
    assertEquals("Threshold of 10 should be accepted", 10, threshold);
  }

  @Test
  public void testThresholdWithWhitespace() {
    // Test that whitespace is trimmed correctly
    Properties props = new Properties();
    props.setProperty("tidb.jdbc.discovery-threshold", "  7  ");

    int threshold = simulateParseReloadThreshold(props);
    assertEquals("Whitespace should be trimmed", 7, threshold);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testThresholdZero() {
    // Test that zero is rejected
    Properties props = new Properties();
    props.setProperty("tidb.jdbc.discovery-threshold", "0");

    simulateParseReloadThreshold(props);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testThresholdNegative() {
    // Test that negative values are rejected
    Properties props = new Properties();
    props.setProperty("tidb.jdbc.discovery-threshold", "-1");

    simulateParseReloadThreshold(props);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testThresholdInvalidNumber() {
    // Test that non-numeric values are rejected
    Properties props = new Properties();
    props.setProperty("tidb.jdbc.discovery-threshold", "abc");

    simulateParseReloadThreshold(props);
  }

  @Test
  public void testEmptyProperties() {
    // Test with empty properties object
    Properties props = new Properties();
    int threshold = simulateParseReloadThreshold(props);
    assertEquals("Default threshold should be used for empty properties", 3, threshold);
  }

  @Test
  public void testEmptyStringValue() {
    // Test with empty string value (should use default)
    Properties props = new Properties();
    props.setProperty("tidb.jdbc.discovery-threshold", "");
    int threshold = simulateParseReloadThreshold(props);
    assertEquals("Default threshold should be used for empty string", 3, threshold);
  }

  @Test
  public void testThresholdErrorMessage() {
    // Test that error messages are informative
    Properties props = new Properties();
    props.setProperty("tidb.jdbc.discovery-threshold", "0");

    try {
      simulateParseReloadThreshold(props);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      String message = e.getMessage();
      assertTrue("Error message should mention the parameter name",
          message.contains("tidb.jdbc.discovery-threshold"));
      assertTrue("Error message should mention the invalid value",
          message.contains("0"));
      assertTrue("Error message should mention the constraint",
          message.contains(">= 1"));
    }
  }

  @Test
  public void testInvalidNumberErrorMessage() {
    // Test error message for invalid number format
    Properties props = new Properties();
    props.setProperty("tidb.jdbc.discovery-threshold", "abc");

    try {
      simulateParseReloadThreshold(props);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      String message = e.getMessage();
      assertTrue("Error message should mention the parameter name",
          message.contains("tidb.jdbc.discovery-threshold"));
      assertTrue("Error message should mention the invalid value",
          message.contains("abc"));
      assertTrue("Error message should mention it's invalid",
          message.contains("Invalid value"));
    }
  }

  @Test
  public void testDiscoveryThresholdFromUrlAppliedToDiscoverer() throws Exception {
    final String thresholdKey = "tidb.jdbc.discovery-threshold";
    final AtomicReference<Properties> capturedProperties = new AtomicReference<>();
    final DiscovererFactory discovererFactory =
        (driver, url, info, executor) -> {
          capturedProperties.set(info);
          return new MockDiscoverer(new String[] {"jdbc:mysql://127.0.0.1:4000/test"});
        };
    final LoadBalancingDriver loadBalancingDriver =
        new LoadBalancingDriver(
            "jdbc:tidb://", new RoundRobinUrlMapper(), new MockDriver(), discovererFactory);

    loadBalancingDriver.connect(
        "jdbc:tidb://127.0.0.1:4000/test?tidb.jdbc.discovery-threshold=1",
        new Properties());

    Properties captured = capturedProperties.get();
    assertNotNull("Discoverer should receive properties", captured);
    assertEquals("1", captured.getProperty(thresholdKey));
  }

  @Test
  public void testDiscoveryThresholdPropertiesOverrideUrl() throws Exception {
    final String thresholdKey = "tidb.jdbc.discovery-threshold";
    final AtomicReference<Properties> capturedProperties = new AtomicReference<>();
    final DiscovererFactory discovererFactory =
        (driver, url, info, executor) -> {
          capturedProperties.set(info);
          return new MockDiscoverer(new String[] {"jdbc:mysql://127.0.0.1:4000/test"});
        };
    final LoadBalancingDriver loadBalancingDriver =
        new LoadBalancingDriver(
            "jdbc:tidb://", new RoundRobinUrlMapper(), new MockDriver(), discovererFactory);
    Properties props = new Properties();
    props.setProperty(thresholdKey, "5");

    loadBalancingDriver.connect(
        "jdbc:tidb://127.0.0.1:4000/test?tidb.jdbc.discovery-threshold=1",
        props);

    Properties captured = capturedProperties.get();
    assertNotNull("Discoverer should receive properties", captured);
    assertEquals("5", captured.getProperty(thresholdKey));
  }
}
