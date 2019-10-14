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

public class DecimalMaxAggregator implements Aggregator
{
  private final ColumnValueSelector selector;
  private BigDecimal max = null;

  public DecimalMaxAggregator(ColumnValueSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public void aggregate()
  {
    BigDecimal value = (BigDecimal) selector.getObject();
    if (max == null) {
      max = value;
    } else if (max.compareTo(value) < 0) {
      max = value;
    }
  }

  @Override
  public Object get()
  {
    return max;
  }

  @Override
  public void close()
  {
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("DecimalMaxAggregator does not support getFloat()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("DecimalMaxAggregator does not support getLong()");
  }

  @Override
  public double getDouble()
  {
    throw new UnsupportedOperationException("DecimalMaxAggregator does not support getDouble()");
  }
}
