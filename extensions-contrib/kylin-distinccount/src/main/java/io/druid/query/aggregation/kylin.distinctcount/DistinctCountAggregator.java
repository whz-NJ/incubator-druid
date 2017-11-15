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
import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;

public class DistinctCountAggregator implements Aggregator
{

  private final ObjectColumnSelector selector;
  private ImmutableBitmap bitmap = new RoaringBitmapFactory().makeEmptyImmutableBitmap();

  public DistinctCountAggregator(ObjectColumnSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public void aggregate()
  {
    bitmap = bitmap.union((ImmutableBitmap) selector.getObject());
  }

  @Override
  public void reset()
  {
    bitmap = null;
  }

  @Override
  public Object get()
  {
    return bitmap;
  }

  @Override
  public void close()
  {
    bitmap = null;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("DistinctCountAggregator does not support getFloat()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("DistinctCountAggregator does not support getLong()");
  }
}
