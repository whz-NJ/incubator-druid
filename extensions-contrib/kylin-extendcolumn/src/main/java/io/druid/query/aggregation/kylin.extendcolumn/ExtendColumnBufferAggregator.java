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

package io.druid.query.aggregation.kylin.extendcolumn;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnValueSelector;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class ExtendColumnBufferAggregator implements BufferAggregator
{
  private final ColumnValueSelector selector;
  private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

  public ExtendColumnBufferAggregator(ColumnValueSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    int oldPosition = buf.position();

    buf.position(position);
    buf.putShort((short) 0);

    buf.position(oldPosition);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    String tmp = (String) selector.getObject();

    if (tmp == null || tmp.length() == 0) {
      return;
    }

    int oldPosition = buf.position();

    byte[] bytes = tmp.getBytes(UTF8_CHARSET);
    short size = (short) bytes.length;

    buf.position(position);
    buf.putShort(size);
    buf.put(bytes);

    buf.position(oldPosition);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    int oldPosition = buf.position();
    int oldLimit = buf.limit();

    buf.position(position);
    short size = buf.getShort();
    if (size == 0) {
      buf.position(oldPosition);
      return null;
    }

    buf.limit(position + size + 2);

    byte[] bytes = new byte[buf.remaining()];
    buf.get(bytes, 0, bytes.length);
    String result = new String(bytes, 0, bytes.length, UTF8_CHARSET);

    buf.limit(oldLimit);
    buf.position(oldPosition);

    return result;
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("ExtendColumnBufferAggregator does not support getFloat()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("ExtendColumnBufferAggregator does not support getLong()");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("ExtendColumnBufferAggregator does not support getDouble()");
  }

  @Override
  public void close()
  {
  }
}
