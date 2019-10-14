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

import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.IAE;
import io.druid.segment.GenericColumnSerializer;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.IOPeon;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.LargeColumnSupportedComplexColumnSerializer;

import java.nio.ByteBuffer;

public class DistinctCountSerde extends ComplexMetricSerde
{

  public DistinctCountSerde()
  {
  }

  @Override
  public String getTypeName()
  {
    return DistinctCountDruidModule.DISTINCT_COUNT;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<ImmutableBitmap> extractedClass()
      {
        return ImmutableBitmap.class;
      }

      @Override
      public ImmutableBitmap extractValue(InputRow inputRow, String metricName)
      {
        Object rawValue = inputRow.getRaw(metricName);

        if (ImmutableBitmap.class.isAssignableFrom(rawValue.getClass())) {
          return (ImmutableBitmap) rawValue;
        } else {
          throw new IAE("The class must be MutableBitmap");
        }
      }
    };
  }

  @Override
  public void deserializeColumn(ByteBuffer byteBuffer, ColumnBuilder columnBuilder)
  {
    final GenericIndexed column = GenericIndexed.read(byteBuffer, getObjectStrategy());
    columnBuilder.setComplexColumn(new ComplexColumnPartSupplier(getTypeName(), column));
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    RoaringBitmapSerdeFactory bitmapSerdeFactory = new RoaringBitmapSerdeFactory(true);
    return bitmapSerdeFactory.getObjectStrategy();
  }

  @Override
  public GenericColumnSerializer getSerializer(IOPeon peon, String column)
  {
    return LargeColumnSupportedComplexColumnSerializer.create(peon, column, this.getObjectStrategy());
  }
}
