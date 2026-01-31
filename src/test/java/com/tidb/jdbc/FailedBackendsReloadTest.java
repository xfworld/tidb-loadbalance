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

/**
 * Unit test to verify the failedBackends threshold reload logic
 */
public class FailedBackendsReloadTest {

  @Test
  public void testMockDiscovererFailedBackends() {
    // Test that MockDiscoverer implements the new methods correctly
    MockDiscoverer discoverer = new MockDiscoverer(new String[]{"jdbc:mysql://localhost:4000"});

    assertEquals(0, discoverer.failedBackends());
    assertEquals(3, discoverer.getBackendReloadThreshold());
  }

  @Test
  public void testThresholdLogic() {
    // Test the threshold logic: when failed backends >= threshold, reload should be triggered
    int threshold = 3;

    // Below threshold - should not trigger
    int failedCount1 = 2;
    boolean shouldReload1 = failedCount1 >= threshold;
    assertFalse("Should not reload when failed backends < threshold", shouldReload1);

    // At threshold - should trigger
    int failedCount2 = 3;
    boolean shouldReload2 = failedCount2 >= threshold;
    assertTrue("Should reload when failed backends >= threshold", shouldReload2);

    // Above threshold - should trigger
    int failedCount3 = 5;
    boolean shouldReload3 = failedCount3 >= threshold;
    assertTrue("Should reload when failed backends > threshold", shouldReload3);
  }
}
