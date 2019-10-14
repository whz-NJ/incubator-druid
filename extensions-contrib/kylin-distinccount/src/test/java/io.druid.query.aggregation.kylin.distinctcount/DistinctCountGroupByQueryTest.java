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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.collections.bitmap.MutableBitmap;
import io.druid.collections.bitmap.RoaringBitmapFactory;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.Row;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Intervals;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQueryRunnerTest;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryRunnerFactory;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.segment.TestHelper;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.QueryableIndex;
import io.druid.segment.IndexSpec;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.data.CompressionFactory;
import io.druid.segment.data.CompressionStrategy;
import io.druid.segment.data.ConciseBitmapSerdeFactory;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.serde.ComplexMetrics;
import io.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class DistinctCountGroupByQueryTest
{
  private static final IndexSpec INDEX_SPEC = new IndexSpec(
          new ConciseBitmapSerdeFactory(),
          CompressionStrategy.LZ4,
          CompressionStrategy.LZ4,
          CompressionFactory.LongEncodingStrategy.LONGS
  );
  private static IndexMerger INDEX_MERGER;

  private static ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  private static IndexIO INDEX_IO;

  static {
    if (ComplexMetrics.getSerdeForType("kylin-distinctCount") == null) {
      ComplexMetrics.registerSerde("kylin-distinctCount", new DistinctCountSerde());
    }

    for (Module mod : new DistinctCountDruidModule().getJacksonModules()) {
      JSON_MAPPER.registerModule(mod);
    }


    INDEX_IO = new IndexIO(JSON_MAPPER, OffHeapMemorySegmentWriteOutMediumFactory
            .instance(), new ColumnConfig()
    {
      @Override
      public int columnCacheSizeBytes()
      {
        return 0;
      }
    });

    INDEX_MERGER = new IndexMergerV9(JSON_MAPPER, INDEX_IO, OffHeapMemorySegmentWriteOutMediumFactory.instance());
  }

  @Test
  public void testGroupByWithDistinctCountAgg() throws Exception
  {
    final GroupByQueryConfig config = new GroupByQueryConfig();
    config.setMaxIntermediateRows(100000000);
    final GroupByQueryRunnerFactory factory = GroupByQueryRunnerTest.makeQueryRunnerFactory(config);

    String visitor_id = "visitor_id";
    String client_type = "client_type";
    String kylin_distinctCount = "kylin";
    long timestamp = System.currentTimeMillis();

    IncrementalIndex index = new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withMetrics(new AggregatorFactory[]{
                    new DistinctCountAggregatorFactory(
                        "kylin",
                        "kylin"
                    )
                })
                .withRollup(false)
                .build()
        )
        .setReportParseExceptions(false)
        .setConcurrentEventAdd(true)
        .setMaxRowCount(1000000)
        .buildOnheap();

    RoaringBitmapFactory bitmapFactory = new RoaringBitmapFactory(true);
    MutableBitmap bitmap1 = bitmapFactory.makeEmptyMutableBitmap();
    bitmap1.add(1);
    bitmap1.add(2);
    bitmap1.add(3);

    MutableBitmap bitmap2 = bitmapFactory.makeEmptyMutableBitmap();
    bitmap2.add(4);

    MutableBitmap bitmap3 = bitmapFactory.makeEmptyMutableBitmap();
    bitmap3.add(1);

    index.add(new MapBasedInputRow(
        timestamp,
        Lists.newArrayList(visitor_id, client_type),
        ImmutableMap.<String, Object>of(
            visitor_id,
            "0",
            client_type,
            "iphone",
            kylin_distinctCount,
            bitmapFactory.makeImmutableBitmap(bitmap1)
        )
    ));
    index.add(new MapBasedInputRow(
        timestamp,
        Lists.newArrayList(visitor_id, client_type),
        ImmutableMap.<String, Object>of(
            visitor_id,
            "0",
            client_type,
            "iphone",
            kylin_distinctCount,
            bitmapFactory.makeImmutableBitmap(bitmap2)
        )
    ));
    index.add(new MapBasedInputRow(
        timestamp,
        Lists.newArrayList(visitor_id, client_type),
        ImmutableMap.<String, Object>of(
            visitor_id,
            "0",
            client_type,
            "android",
            kylin_distinctCount,
            bitmapFactory.makeImmutableBitmap(bitmap3)
        )
    ));

    final File finalFile = new File("tmp");
    INDEX_MERGER.persist(
        index,
        Intervals.of("1970-01-01T00:00:00.000Z/2020-01-01T00:00:00.000Z"),
        finalFile,
        INDEX_SPEC,
        null
    );

    QueryableIndex queryableIndex = INDEX_IO.loadIndex(finalFile);
    QueryableIndexSegment segment = new QueryableIndexSegment("index", queryableIndex);

    GroupByQuery query = new GroupByQuery.Builder().setDataSource(QueryRunnerTestHelper.dataSource)
                                                   .setGranularity(QueryRunnerTestHelper.allGran)
                                                   .setDimensions(Arrays.<DimensionSpec>asList(new DefaultDimensionSpec(
                                                       client_type,
                                                       client_type
                                                   )))
                                                   .setInterval(QueryRunnerTestHelper.fullOnInterval)
                                                   .setLimitSpec(
                                                       new DefaultLimitSpec(
                                                           Lists.newArrayList(
                                                               new OrderByColumnSpec(
                                                                   client_type,
                                                                   OrderByColumnSpec.Direction.DESCENDING
                                                               )
                                                           ), 10))
                                                   .setAggregatorSpecs(Lists.newArrayList(new DistinctCountAggregatorFactory(
                                                       "kylin",
                                                       "kylin"
                                                   )))
                                                   .build();

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, factory.createRunner(segment), query);

    bitmap1.or(bitmap2);
    System.out.println("bitmap1 :" + bitmap1.size());
    System.out.println("bitmap3 :" + bitmap3.size());

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            client_type, "iphone",
            "kylin", bitmap1
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            client_type, "android",
            "kylin", bitmap3
        )
    );

    TestHelper.assertExpectedObjects(expectedResults, results, "distinct-count");

    FileUtils.deleteDirectory(finalFile);
  }
}
