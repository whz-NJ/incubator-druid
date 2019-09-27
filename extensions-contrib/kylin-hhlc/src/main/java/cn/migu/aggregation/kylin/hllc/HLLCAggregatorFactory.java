package cn.migu.aggregation.kylin.hhlc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.java.util.common.IAE;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.cache.CacheKeyBuilder;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;
import org.apache.commons.codec.binary.Base64;
import io.druid.java.util.common.StringUtils;
import org.apache.kylin.measure.hllc.HLLCAggregator;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * @author whz
 * @create 2019-09-26 17:19
 * @desc TODO: add description here
 **/
public class HHLCAggregatorFactory extends AggregatorFactory
{

    private final String name;

    private final String fieldName;

    private Integer precision;

    private final byte cacheTypeId = (byte) 0xf6;

    @JsonCreator public HHLCAggregatorFactory(@JsonProperty("name") String name,
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
                    "selector in HHLCAggregatorFactory should not be Null");
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
        if (!classOfObject.equals(Object.class) && !HyperLogLog.class
                .isAssignableFrom(classOfObject)) {
            throw new IAE(
                    "Incompatible type for metric[%s], expected a HyperLogLog, got a %s",
                    fieldName, classOfObject);
        }

        return new HHLCBufferAggregator(selector, precision);
    }

    @Override public Comparator getComparator()
    {
        return new Comparator()
        {
            @Override public int compare(Object o, Object o1)
            {
                // TODO cardinality should be defined as a member
                long card = ((HyperLogLog) o).cardinality();
                long card1 = ((HyperLogLog) o1).cardinality();
                return (card > card1) ? 1 : (card < card1) ? -1 : 0;
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

        try {
            return ((HyperLogLog) lhs).merge((HyperLogLog) rhs);
        }
        catch (CardinalityMergeException e) {
            throw new IAE("failed to merge to hyperLogLog.", e);
        }
    }

    @Override public AggregatorFactory getCombiningFactory()
    {
        return new HHLCAggregatorFactory(name, fieldName, precision);
    }

    @Override public List<AggregatorFactory> getRequiredColumns()
    {
        return Arrays.<AggregatorFactory>asList(
                new HHLCAggregatorFactory(name, fieldName, precision));
    }

    @Override public Object deserialize(Object object)
    {
        try {
            if (object instanceof String) {
                return HyperLogLog.Builder.build(Base64
                        .decodeBase64(StringUtils.toUtf8((String) object)));
            } else if (object instanceof byte[]) {
                return HyperLogLog.Builder.build((byte[]) object);
            } else if (object instanceof ByteBuffer) {
                // Be conservative, don't assume we own this buffer.
                ByteBuffer buffer = ((ByteBuffer) object).duplicate();
                byte[] bytes = new byte[buffer.remaining()];
                ((ByteBuffer) object).get(bytes);
                return HyperLogLog.Builder.build(bytes);
            } else {
                return object;
            }
        }
        catch (Exception e) {
            throw new IAE("failed to deserialize HyperLogLog", e);
        }
    }

    @Override public Object finalizeComputation(Object object)
    {
        return object;
    }

    @Override public String getName()
    {
        return name;
    }

    @Override public List<String> requiredFields()
    {
        return Arrays.asList(fieldName);
    }

    @Override public String getTypeName()
    {
        return HHLCModule.KYLIN_HHL_COUNT;
    }

    @Override public int getMaxIntermediateSize()
    {
        return 4;
    }

    @Override public byte[] getCacheKey()
    {
        return new CacheKeyBuilder(cacheTypeId).appendString(name)
                .appendString(fieldName).build();
    }
}