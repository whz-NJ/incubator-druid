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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.StringUtils;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnValueSelector;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * Created by kangkaisen on 2017/11/28.
 */
public class DecimalMinAggregatorFactory extends DecimalAggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 29;

  @JsonCreator
  public DecimalMinAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("precision") Integer precision
  )
  {
    super(name, fieldName, precision);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory columnFactory)
  {
    ColumnValueSelector selector = columnFactory.makeColumnValueSelector(fieldName);
    if (selector == null) {
      throw new IAE("selector in ExtendColumnAggregatorFactory should not be Null");
    } else {
      return new DecimalMinAggregator(selector);
    }
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnFactory)
  {
    ColumnValueSelector selector = columnFactory.makeColumnValueSelector(fieldName);

    final Class classOfObject = selector.classOfObject();
    if (!classOfObject.equals(Object.class) && !BigDecimal.class.isAssignableFrom(classOfObject)) {
      throw new IAE("Incompatible type for metric[%s], expected a ExtendByteArray, got a %s", fieldName, classOfObject);
    }

    return new DecimalMinBufferAggregator(selector);
  }

  @Override
  public Object combine(Object o1, Object o2)
  {
    BigDecimal left = (BigDecimal) o1;
    BigDecimal right = (BigDecimal) o2;

    if (left.compareTo(right) < 0) {
      return left;
    } else {
      return right;
    }
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DecimalMinAggregatorFactory(name, name, precision);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Arrays.<AggregatorFactory>asList(new DecimalMinAggregatorFactory(fieldName, fieldName, precision));
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);
    return ByteBuffer.allocate(2 + fieldNameBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .put(AggregatorUtil.STRING_SEPARATOR)
                     .array();
  }

  @Override
  public String getTypeName()
  {
    return DecimalDruidModule.DECIMALMIN;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DecimalMinAggregatorFactory that = (DecimalMinAggregatorFactory) o;

    if (!fieldName.equals(that.fieldName)) {
      return false;
    }

    if (!name.equals(that.name)) {
      return false;
    }

    return true;
  }

  @Override
  public String toString()
  {
    return "DecimalMinAggregatorFactory {" + "name='" + name + '\'' + ", fieldName='" + fieldName + '\'' + '}';
  }
}
