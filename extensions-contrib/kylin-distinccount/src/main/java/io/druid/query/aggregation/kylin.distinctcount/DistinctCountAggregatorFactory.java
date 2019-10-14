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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.collections.bitmap.RoaringBitmapFactory;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.StringUtils;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.commons.codec.binary.Base64;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class DistinctCountAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 21;

  private final String name;
  private final String fieldName;

  @JsonCreator
  public DistinctCountAggregatorFactory(@JsonProperty("name") String name, @JsonProperty("fieldName") String fieldName)
  {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(fieldName);
    this.name = name;
    this.fieldName = fieldName;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory columnFactory)
  {
    ObjectColumnSelector selector = columnFactory.makeObjectColumnSelector(fieldName);
    if (selector == null) {
      throw new IAE("selector in DistinctCountAggregatorFactory should not be Null");
    } else {
      return new DistinctCountAggregator(selector);
    }
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnFactory)
  {
    ObjectColumnSelector selector = columnFactory.makeObjectColumnSelector(fieldName);

    final Class classOfObject = selector.classOfObject();
    if (!classOfObject.equals(Object.class) && !ImmutableBitmap.class.isAssignableFrom(classOfObject)) {
      throw new IAE("Incompatible type for metric[%s], expected a MutableBitmap, got a %s", fieldName, classOfObject);
    }

    return new DistinctCountBufferAggregator(selector);
  }

  @Override
  public Comparator getComparator()
  {
    return new Comparator()
    {
      @Override
      public int compare(Object o, Object o1)
      {
        return new RoaringBitmapSerdeFactory(true).getObjectStrategy()
                                                  .compare((ImmutableBitmap) o, (ImmutableBitmap) o1);
      }
    };
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    if (rhs == null) {
      return lhs;
    }
    if (lhs == null) {
      return rhs;
    }

    return ((ImmutableBitmap) lhs).union((ImmutableBitmap) rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DistinctCountAggregatorFactory(name, name);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Arrays.<AggregatorFactory>asList(new DistinctCountAggregatorFactory(fieldName, fieldName));
  }

  @Override
  public Object deserialize(Object object)
  {
    final ByteBuffer buffer;
    if (object instanceof String) {
      buffer = ByteBuffer.wrap(Base64.decodeBase64(StringUtils.toUtf8((String) object)));
    } else if (object instanceof byte[]) {
      buffer = ByteBuffer.wrap((byte[]) object);
    } else if (object instanceof ByteBuffer) {
      // Be conservative, don't assume we own this buffer.
      buffer = ((ByteBuffer) object).duplicate();
    } else {
      return object;
    }
    return new RoaringBitmapFactory().mapImmutableBitmap(buffer);
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
    return DistinctCountDruidModule.DISTINCT_COUNT;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return 4;
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

    DistinctCountAggregatorFactory that = (DistinctCountAggregatorFactory) o;

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
    return "DistinctCountAggregatorFactory{" + "name='" + name + '\'' + ", fieldName='" + fieldName + '\'' + '}';
  }
}
