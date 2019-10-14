/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.kylin.distinctcount;

import com.google.common.collect.ImmutableMap;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.collections.bitmap.MutableBitmap;
import io.druid.collections.bitmap.RoaringBitmapFactory;
import io.druid.data.input.MapBasedRow;
import io.druid.query.aggregation.AggregationTestHelper;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.epinephelinae.GrouperTestUtil;
import io.druid.query.groupby.epinephelinae.TestColumnSelectorFactory;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DistinctCountBufferAggregatorTest
{
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testRelocation()
  {
    DistinctCountDruidModule module = new DistinctCountDruidModule();
    final AggregationTestHelper helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        module.getJacksonModules(),
        new GroupByQueryConfig(),
        tempFolder
    );
    final TestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    RoaringBitmapFactory bitmapFactory = new RoaringBitmapFactory(true);
    MutableBitmap bitmap1 = bitmapFactory.makeEmptyMutableBitmap();
    bitmap1.add(1);

    columnSelectorFactory.setRow(new MapBasedRow(0,
                                                 ImmutableMap.<String, Object>of(
                                                     "kylin",
                                                     bitmapFactory.makeImmutableBitmap(bitmap1)
                                                 )
    ));
    ImmutableBitmap[] bitmaps = helper.runRelocateVerificationTest(
        new DistinctCountAggregatorFactory(
            "kylin",
            "kylin"
        ),
        columnSelectorFactory,
        ImmutableBitmap.class
    );
    Assert.assertEquals(bitmaps[0].size(), bitmaps[1].size(), 0);
  }
}
