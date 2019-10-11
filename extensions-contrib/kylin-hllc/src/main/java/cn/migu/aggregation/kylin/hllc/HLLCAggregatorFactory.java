package cn.migu.aggregation.kylin.hllc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;
import org.apache.commons.codec.binary.Base64;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * @author whz
 * @create 2019-09-26 17:19
 * @desc TODO: add description here
 **/
public class HLLCAggregatorFactory extends AggregatorFactory
{
    private final Logger logger = new Logger(HLLCAggregatorFactory.class);

    private final String name;

    private final String fieldName;

    private Integer precision;

    private final byte CACHE_TYPE_ID = (byte) 21;

    @JsonCreator public HLLCAggregatorFactory(@JsonProperty("name") String name,
            @JsonProperty("fieldName") String fieldName,
            @JsonProperty("precision") Integer precision)
    {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(fieldName);
        this.name = name;
        this.fieldName = fieldName;
        this.precision = precision;
    }

    @Override public Aggregator factorize(ColumnSelectorFactory columnFactory)
    {
        ObjectColumnSelector selector = columnFactory
                .makeObjectColumnSelector(fieldName);
        if (selector == null) {
            throw new IAE(
                    "selector in HLLCAggregatorFactory should not be Null");
        } else {
            return new HLLCAggregator(selector, precision);
        }
    }

    @Override public BufferAggregator factorizeBuffered(
            ColumnSelectorFactory metricFactory)
    {
        ObjectColumnSelector selector = metricFactory
                .makeObjectColumnSelector(fieldName);

        final Class classOfObject = selector.classOfObject();
        if (!classOfObject.equals(Object.class) && !WrappedHLLCounter.class
                .isAssignableFrom(classOfObject)) {
            throw new IAE(
                    "Incompatible type for metric[%s], expected a WrappedHLLCounter, got a %s",
                    fieldName, classOfObject);
        }

        return new HLLCBufferAggregator(selector, precision);
    }

    @Override public Comparator getComparator()
    {
        return new Comparator()
        {
            @Override public int compare(Object o, Object o1)
            {
                // TODO getCountEstimate should be defined as a member
                long cnt = ((WrappedHLLCounter) o).getCountEstimate();
                long cnt1 = ((WrappedHLLCounter) o1).getCountEstimate();
                return (cnt > cnt1) ? 1 : (cnt < cnt1) ? -1 : 0;
            }
        };
    }

    @Override public Object combine(Object lhs, Object rhs)
    {
        if (rhs == null) {
            return lhs;
        }
        if (lhs == null) {
            return rhs;
        }
        ((WrappedHLLCounter) lhs).merge((WrappedHLLCounter) rhs);
        return lhs;
    }

    @Override public AggregatorFactory getCombiningFactory()
    {
        return new HLLCAggregatorFactory(name, fieldName, precision);
    }

    @Override public List<AggregatorFactory> getRequiredColumns()
    {
        return Arrays.<AggregatorFactory>asList(
                new HLLCAggregatorFactory(name, fieldName, precision));
    }

    @Override public Object deserialize(Object object)
    {
        byte[] bytes = null;
        try {
            if (object instanceof String) {
                bytes = Base64
                        .decodeBase64(StringUtils.toUtf8((String) object));
            } else if (object instanceof byte[]) {
                bytes = (byte[]) object;
            } else if (object instanceof ByteBuffer) {
                // Be conservative, don't assume we own this buffer.
                ByteBuffer buffer = ((ByteBuffer) object).duplicate();
                bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
            } else {
                return object;
            }
            return new WrappedHLLCounter(HLLCModule.fromBytes(bytes));
        }
        catch (Exception e) {
            logger.error(e, "failed to deserialize WrappedHLLCounter");
            throw new IAE("failed to deserialize WrappedHLLCounter");
        }
    }

    @Override public Object finalizeComputation(Object object)
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

    @Override public List<String> requiredFields()
    {
        return Arrays.asList(fieldName);
    }

    @Override public byte[] getCacheKey()
    {
        byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);
        return ByteBuffer.allocate(2 + fieldNameBytes.length)
                .put(CACHE_TYPE_ID)
                .put(fieldNameBytes)
                .put(AggregatorUtil.STRING_SEPARATOR)
                .array();
    }

    @Override public String getTypeName()
    {
        return HLLCModule.KYLIN_HLL_COUNT;
    }

    @Override public int getMaxIntermediateSize()
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

        HLLCAggregatorFactory that = (HLLCAggregatorFactory) o;

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

    @Override public String toString()
    {
        return "HLLCAggregatorFactory{" + "name='" + name + '\''
                + ", fieldName='" + fieldName + '\'' + ", precision="
                + precision + '}';
    }
}