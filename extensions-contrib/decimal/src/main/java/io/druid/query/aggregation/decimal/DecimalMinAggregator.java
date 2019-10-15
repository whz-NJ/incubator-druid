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

import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ColumnValueSelector;

import java.math.BigDecimal;

public class DecimalMinAggregator implements Aggregator
{
  private final ColumnValueSelector selector;
  private BigDecimal min = null;

  public DecimalMinAggregator(ColumnValueSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public void aggregate()
  {
    BigDecimal value = (BigDecimal) selector.getObject();

    if (min == null) {
      min = value;
    } else if (min.compareTo(value) > 0) {
      min = value;
    }
  }

  @Override
  public Object get()
  {
    return min;
  }

  @Override
  public void close()
  {
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