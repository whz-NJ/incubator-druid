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

import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnValueSelector;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public abstract class DecimalBufferAggregator implements BufferAggregator
{
  protected final ColumnValueSelector selector;

  public DecimalBufferAggregator(ColumnValueSelector selector)
  {
    this.selector = selector;
  }

  protected BigDecimal readTFromBuffer(ByteBuffer buf, int position)
  {
    int oldPosition = buf.position();
    int oldLimit = buf.limit();

    buf.position(position);
    short size = buf.getShort();
    int scale = buf.getInt();
    buf.limit(position + size);
    byte[] bytes = new byte[buf.remaining()];
    buf.get(bytes, 0, bytes.length);

    buf.limit(oldLimit);
    buf.position(oldPosition);

    return new BigDecimal(new BigInteger(bytes), scale);
  }

  protected void writeToBuffer(BigDecimal value, ByteBuffer buf, int position)
  {
    int oldPosition = buf.position();

    byte[] sumBytes = value.unscaledValue().toByteArray();
    short sumSize = (short) (6 + sumBytes.length);

    buf.position(position);
    buf.putShort(sumSize);
    buf.putInt(value.scale());
    buf.put(sumBytes);

    buf.position(oldPosition);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return readTFromBuffer(buf, position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("DecimalBufferAggregator does not support getFloat()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("DecimalBufferAggregator does not support getLong()");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("DecimalBufferAggregator does not support getDouble()");
  }

  @Override
  public void close()
  {
  }
}
