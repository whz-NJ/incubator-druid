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
import io.druid.collections.bitmap.RoaringBitmapFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnValueSelector;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.nio.ByteBuffer;
import java.util.IdentityHashMap;

public class DistinctCountBufferAggregator implements BufferAggregator
{
  private final ColumnValueSelector selector;
  private final IdentityHashMap<ByteBuffer, Int2ObjectMap<ImmutableBitmap>> bitmaps = new IdentityHashMap<>();

  public DistinctCountBufferAggregator(ColumnValueSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    createNewBitmap(buf, position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    ImmutableBitmap updateBitmap = (ImmutableBitmap) selector.getObject();
    if (updateBitmap == null) {
      return;
    }

    ImmutableBitmap oldBitmap = getBitmap(buf, position);
    ImmutableBitmap mergedBitmap = oldBitmap.intersection(updateBitmap);
    bitmaps.get(buf).put(position, mergedBitmap);
  }

  private ImmutableBitmap createNewBitmap(ByteBuffer buf, int position)
  {
    ImmutableBitmap immutableBitmap = new RoaringBitmapFactory().makeEmptyImmutableBitmap();
    Int2ObjectMap<ImmutableBitmap> bitmapMap = bitmaps.get(buf);
    if (bitmapMap == null) {
      bitmapMap = new Int2ObjectOpenHashMap<>();
      bitmaps.put(buf, bitmapMap);
    }
    bitmapMap.put(position, immutableBitmap);
    return immutableBitmap;
  }

  //Note that this is not threadsafe and I don't think it needs to be
  private ImmutableBitmap getBitmap(ByteBuffer buf, int position)
  {
    Int2ObjectMap<ImmutableBitmap> bitmapMap = bitmaps.get(buf);
    ImmutableBitmap bitmap = bitmapMap != null ? bitmapMap.get(position) : null;
    if (bitmap != null) {
      return bitmap;
    }
    return createNewBitmap(buf, position);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return getBitmap(buf, position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("DistinctCountBufferAggregator does not support getFloat()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("DistinctCountBufferAggregator does not support getLong()");
  }

  @Override
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    createNewBitmap(newBuffer, newPosition);
    Int2ObjectMap<ImmutableBitmap> bitmapMap = bitmaps.get(oldBuffer);
    if (bitmapMap != null) {
      bitmaps.get(newBuffer).put(newPosition, bitmapMap.get(oldPosition));
      bitmapMap.remove(oldPosition);
      if (bitmapMap.isEmpty()) {
        bitmaps.remove(oldBuffer);
      }
    }
  }

  @Override
  public void close()
  {
  }
}
