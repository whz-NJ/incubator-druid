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

package io.druid.query.aggregation.decimal;

import com.google.common.primitives.Ints;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.IAE;
import io.druid.segment.GenericColumnSerializer;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.IOPeon;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.LargeColumnSupportedComplexColumnSerializer;
import org.apache.commons.lang.ArrayUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public abstract class DecimalSerde extends ComplexMetricSerde
{

  public DecimalSerde()
  {
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<BigDecimal> extractedClass()
      {
        return BigDecimal.class;
      }

      @Override
      public BigDecimal extractValue(InputRow inputRow, String metricName)
      {
        Object rawValue = inputRow.getRaw(metricName);

        if (BigDecimal.class.isAssignableFrom(rawValue.getClass())) {
          return (BigDecimal) rawValue;
        } else {
          throw new IAE("The class must be ExtendByteArray");
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
    return new ObjectStrategy()
    {
      @Override
      public Class getClazz()
      {
        return BigDecimal.class;
      }

      @Override
      public Object fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
        readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);

        int scale = readOnlyBuffer.getInt();
        byte[] bytes = new byte[readOnlyBuffer.remaining()];
        readOnlyBuffer.get(bytes, 0, bytes.length);

        return new BigDecimal(new BigInteger(bytes), scale);
      }

      @Override
      public byte[] toBytes(Object val)
      {
        if (val == null) {
          return new byte[]{};
        }

        if (val instanceof BigDecimal) {
          BigDecimal value = (BigDecimal) val;

          byte[] scaleBytes = Ints.toByteArray(value.scale());
          byte[] bytes = value.unscaledValue().toByteArray();

          return ArrayUtils.addAll(scaleBytes, bytes);
        } else {
          throw new IAE("Unknown class[%s], toString[%s]", val.getClass(), val);
        }
      }

      @Override
      public int compare(Object o1, Object o2)
      {
        return ((BigDecimal) o1).compareTo((BigDecimal) o2);
      }
    };
  }

  @Override
  public GenericColumnSerializer getSerializer(IOPeon peon, String column)
  {
    return LargeColumnSupportedComplexColumnSerializer.create(peon, column, this.getObjectStrategy());
  }
}
