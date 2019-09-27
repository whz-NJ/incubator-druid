package cn.migu.aggregation.kylin.hllc;

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
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.measure.hllc.RegisterType;

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

    private final String name;

    private final String fieldName;

    private Integer precision;

    private final byte cacheTypeId = (byte) 0xf6;

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
        if (!classOfObject.equals(Object.class) && !HLLCounter.class
                .isAssignableFrom(classOfObject)) {
            throw new IAE(
                    "Incompatible type for metric[%s], expected a HLLCounter, got a %s",
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
                long cnt = ((HLLCounter) o).getCountEstimate();
                long cnt1 = ((HLLCounter) o1).getCountEstimate();
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
        ((HLLCounter) lhs).merge((HLLCounter) rhs);
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
            int precision = HLLCModule.bytesToInt(bytes);
            if (precision != this.precision) {
                throw new IAE(
                        "unexpected precision(%d), expected precison(%d).",
                        precision, this.precision);
            }
            ByteBuffer hllBuffer = ByteBuffer
                    .wrap(bytes, Integer.BYTES, (bytes.length - Integer.BYTES));
            HLLCounter hllCounter = new HLLCounter(precision,
                    RegisterType.DENSE);
            hllCounter.readRegisters(hllBuffer);
            return hllCounter;
        }
        catch (Exception e) {
            throw new IAE("failed to deserialize HLLCounter", e);
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
        return HLLCModule.KYLIN_HHL_COUNT;
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