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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.StringUtils;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnValueSelector;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class ExtendColumnAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 26;

  private final String name;
  private final String fieldName;
  private final Integer precision;

  public static final String EXTEND_COLUMN = "kylin-extendcolumn";

  @JsonCreator
  public ExtendColumnAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("precision") Integer precision
  )
  {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(fieldName);
    this.name = name;
    this.fieldName = fieldName;
    this.precision = precision;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory columnFactory)
  {
    ColumnValueSelector selector = columnFactory.makeColumnValueSelector(fieldName);
    if (selector == null) {
      throw new IAE("selector in ExtendColumnAggregatorFactory should not be Null");
    } else {
      return new ExtendColumnAggregator(selector);
    }
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnFactory)
  {
    ColumnValueSelector selector = columnFactory.makeColumnValueSelector(fieldName);

    final Class classOfObject = selector.classOfObject();
    if (!classOfObject.equals(Object.class) && !String.class.isAssignableFrom(classOfObject)) {
      throw new IAE("Incompatible type for metric[%s], expected a ExtendByteArray, got a %s", fieldName, classOfObject);
    }

    return new ExtendColumnBufferAggregator(selector);
  }

  @Override
  public Comparator getComparator()
  {
    return new Comparator()
    {
      @Override
      public int compare(Object o1, Object o2)
      {
        String left = (String) o1;

        if (left == null || left.length() == 0) {
          return -1;
        }

        return 1;
      }
    };
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    String left = (String) lhs;

    if (left == null || left.length() == 0) {
      return rhs;
    }

    return left;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new ExtendColumnAggregatorFactory(name, name, precision);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Arrays.<AggregatorFactory>asList(new ExtendColumnAggregatorFactory(fieldName, fieldName, precision));
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return object;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public Integer getPrecision()
  {
    return precision;
  }

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(fieldName);
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
    return EXTEND_COLUMN;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return precision + 2;
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

    ExtendColumnAggregatorFactory that = (ExtendColumnAggregatorFactory) o;

    if (!fieldName.equals(that.fieldName)) {
      return false;
    }

    if (!name.equals(that.name)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + fieldName.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "ExtendColumnAggregatorFactory {" + "name='" + name + '\'' + ", fieldName='" + fieldName + '\'' + '}';
  }
}
