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

package com.tidb.jdbc;

import com.tidb.jdbc.impl.Backend;
import com.tidb.jdbc.impl.BestResponseTimeBalanceMapper;

import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

/**
 * Unit tests for BestResponseTimeBalanceMapper
 */
public class BestResponseTimeBalanceMapperTest {

  private BestResponseTimeBalanceMapper mapper;
  private Backend backend;

  private static final String URL1 = "jdbc:mysql://host1:4000/test";
  private static final String URL2 = "jdbc:mysql://host2:4000/test";
  private static final String URL3 = "jdbc:mysql://host3:4000/test";

  @Before
  public void setUp() {
    mapper = new BestResponseTimeBalanceMapper();
    backend = new Backend();
  }

  @Test
  public void testSingleBackend() {
    // Single backend should be returned as-is
    backend.setBackend(new String[]{URL1});
    String[] result = mapper.apply(backend);

    assertArrayEquals(new String[]{URL1}, result);
  }

  @Test
  public void testMultipleBackendsWithNoData() {
    // When no response time data, backends should keep their order
    backend.setBackend(new String[]{URL1, URL2, URL3});
    String[] result = mapper.apply(backend);

    assertArrayEquals(new String[]{URL1, URL2, URL3}, result);
  }

  @Test
  public void testBackendsWithResponseTimeData() {
    // Record response times: URL1 fastest, URL2 medium, URL3 slowest
    mapper.recordSuccess(URL1, 50);  // 50ms
    mapper.recordSuccess(URL2, 100); // 100ms
    mapper.recordSuccess(URL3, 200); // 200ms

    backend.setBackend(new String[]{URL3, URL1, URL2}); // Initial order: slow, fast, medium
    String[] result = mapper.apply(backend);

    // Should be sorted by response time: URL1 (50), URL2 (100), URL3 (200)
    assertArrayEquals(new String[]{URL1, URL2, URL3}, result);
  }

  @Test
  public void testBackendWithFailures() {
    // URL1: fast but has failures
    mapper.recordSuccess(URL1, 50);
    mapper.recordFailure(URL1);
    mapper.recordFailure(URL1);

    // URL2: slower but no failures
    mapper.recordSuccess(URL2, 150);

    backend.setBackend(new String[]{URL1, URL2});
    String[] result = mapper.apply(backend);

    // URL2 should be first despite being slower (no failures)
    assertEquals(URL2, result[0]);
    assertEquals(URL1, result[1]);
  }

  @Test
  public void testUnknownBackendPrioritized() {
    // URL1 has good response time
    mapper.recordSuccess(URL1, 50);

    // URL2 has no data
    // URL3 has poor response time
    mapper.recordSuccess(URL3, 500);

    backend.setBackend(new String[]{URL3, URL2, URL1});
    String[] result = mapper.apply(backend);

    // Unknown backend (URL2) should be prioritized for exploration
    assertEquals(URL2, result[0]);
    // Then the fast known backend (URL1)
    assertEquals(URL1, result[1]);
    // Then the slow known backend (URL3)
    assertEquals(URL3, result[2]);
  }

  @Test
  public void testAveragingResponseTimes() {
    // Record multiple response times for URL1
    mapper.recordSuccess(URL1, 100);
    mapper.recordSuccess(URL1, 200);
    mapper.recordSuccess(URL1, 300);
    // Average = (100 + 200 + 300) / 3 = 200

    // URL2 is faster on average
    mapper.recordSuccess(URL2, 150);
    mapper.recordSuccess(URL2, 150);
    // Average = 150

    backend.setBackend(new String[]{URL1, URL2});
    String[] result = mapper.apply(backend);

    // URL2 should be first (150ms avg vs 200ms avg)
    assertEquals(URL2, result[0]);
    assertEquals(URL1, result[1]);
  }

  @Test
  public void testEqualResponseTimes() {
    // When response times are equal, maintain order
    mapper.recordSuccess(URL1, 100);
    mapper.recordSuccess(URL2, 100);
    mapper.recordSuccess(URL3, 100);

    backend.setBackend(new String[]{URL1, URL2, URL3});
    String[] result = mapper.apply(backend);

    assertArrayEquals(new String[]{URL1, URL2, URL3}, result);
  }

  @Test
  public void testMixedSuccessAndFailure() {
    // URL1: 1 success, 0 failures (avg: 100ms, penalty: 0)
    mapper.recordSuccess(URL1, 100);

    // URL2: 10 successes, 1 failure (avg: 100ms, penalty: 5000ms)
    for (int i = 0; i < 10; i++) {
      mapper.recordSuccess(URL2, 100);
    }
    mapper.recordFailure(URL2);

    // URL3: 1 success, very slow (avg: 1000ms, penalty: 0)
    mapper.recordSuccess(URL3, 1000);

    backend.setBackend(new String[]{URL1, URL2, URL3});
    String[] result = mapper.apply(backend);

    // URL1: 100ms (no failures) - fastest
    // URL3: 1000ms (no failures) - slowest
    // URL2: 100ms + 1*5000ms = 5100ms (with failure penalty) - penalized
    // Expected order: URL1, URL3, URL2
    assertEquals(URL1, result[0]);
    assertEquals(URL3, result[1]);
    assertEquals(URL2, result[2]);
  }

  @Test
  public void testGetStats() {
    mapper.recordSuccess(URL1, 100);
    mapper.recordSuccess(URL1, 200);
    mapper.recordFailure(URL1);

    BestResponseTimeBalanceMapper.ResponseTimeStats stats = mapper.getStats(URL1);

    assertNotNull(stats);
    assertEquals(150, stats.getAverageResponseTime()); // (100 + 200) / 2
    assertEquals(2, stats.getRequestCount());
    assertEquals(1, stats.getFailureCount());
    assertTrue(stats.getFailureRate() > 0);
  }

  @Test
  public void testGetStatsForUnknownBackend() {
    BestResponseTimeBalanceMapper.ResponseTimeStats stats = mapper.getStats(URL1);

    assertNull(stats);
  }

  @Test
  public void testClearStats() {
    mapper.recordSuccess(URL1, 100);
    mapper.recordSuccess(URL2, 200);

    assertEquals(2, mapper.getTrackedBackendCount());

    mapper.clearStats();

    assertEquals(0, mapper.getTrackedBackendCount());
    assertNull(mapper.getStats(URL1));
    assertNull(mapper.getStats(URL2));
  }

  @Test
  public void testRemoveStats() {
    mapper.recordSuccess(URL1, 100);
    mapper.recordSuccess(URL2, 200);

    assertEquals(2, mapper.getTrackedBackendCount());

    mapper.removeStats(URL1);

    assertEquals(1, mapper.getTrackedBackendCount());
    assertNull(mapper.getStats(URL1));
    assertNotNull(mapper.getStats(URL2));
  }

  @Test
  public void testConcurrentUpdates() throws InterruptedException {
    // Test thread safety with concurrent updates
    int threadCount = 10;
    int updatesPerThread = 100;

    Thread[] threads = new Thread[threadCount];
    for (int i = 0; i < threadCount; i++) {
      final String url = "jdbc:mysql://host" + i + ":4000/test";
      threads[i] = new Thread(() -> {
        for (int j = 0; j < updatesPerThread; j++) {
          mapper.recordSuccess(url, 100);
          // Record failure every 11th time (0, 11, 22, 33, ...)
          if (j % 11 == 0) {
            mapper.recordFailure(url);
          }
        }
      });
      threads[i].start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    // All backends should be tracked
    assertEquals(threadCount, mapper.getTrackedBackendCount());

    // Each backend should have the correct number of records
    // Loop runs j = 0 to 99 (100 times)
    // Success recorded every time: 100 times
    // Failure recorded when j % 11 == 0: j = 0, 11, 22, 33, 44, 55, 66, 77, 88, 99 = 10 times
    for (int i = 0; i < threadCount; i++) {
      String url = "jdbc:mysql://host" + i + ":4000/test";
      BestResponseTimeBalanceMapper.ResponseTimeStats stats = mapper.getStats(url);
      assertNotNull(stats);
      assertEquals(100, stats.getRequestCount());
      assertEquals(10, stats.getFailureCount());
    }
  }

  @Test
  public void testEmptyBackend() {
    backend.setBackend(new String[]{});
    String[] result = mapper.apply(backend);

    assertArrayEquals(new String[]{}, result);
  }
}
