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

import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;

public class ExtendColumnAggregator implements Aggregator
{

  private final ObjectColumnSelector selector;
  private String result;

  public ExtendColumnAggregator(ObjectColumnSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public void aggregate()
  {
    String tmp = (String) selector.getObject();

    if (tmp == null || tmp.length() == 0) {
      return;
    }

    result = tmp;
  }

  @Override
  public void reset()
  {
    result = null;
  }

  @Override
  public Object get()
  {
    return result;
  }

  @Override
  public void close()
  {
    result = null;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("ExtendColumnAggregator does not support getFloat()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("ExtendColumnAggregator does not support getLong()");
  }

  @Override
  public double getDouble()
  {
    throw new UnsupportedOperationException("ExtendColumnAggregator does not support getDouble()");
  }
}
