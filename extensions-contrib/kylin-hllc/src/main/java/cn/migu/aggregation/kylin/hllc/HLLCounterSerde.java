package cn.migu.aggregation.kylin.hllc;

import io.druid.data.input.InputRow;
import io.druid.java.util.common.IAE;
import io.druid.segment.GenericColumnSerializer;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.IOPeon;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.LargeColumnSupportedComplexColumnSerializer;
import org.apache.kylin.measure.hllc.HLLCounter;

import java.nio.ByteBuffer;

/**
 * @author whz
 * @create 2019-09-26 17:19
 * @desc TODO: add description here
 **/
public class HLLCounterSerde extends ComplexMetricSerde
{
    @Override public String getTypeName()
    {
        return HLLCModule.KYLIN_HLL_COUNT;
    }

    @Override public ComplexMetricExtractor getExtractor()
    {
        return new ComplexMetricExtractor()
        {
            @Override public Class<HLLCounter> extractedClass()
            {
                return HLLCounter.class;
            }

            @Override public HLLCounter extractValue(InputRow inputRow,
                    String metricName)
            {
                Object rawValue = inputRow.getRaw(metricName);

                if (HLLCounter.class.isAssignableFrom(rawValue.getClass())) {
                    return (HLLCounter) rawValue;
                } else {
                    throw new IAE("The class must be HLLCounter");
                }
            }
        };
    }

    @Override
    public void deserializeColumn(ByteBuffer byteBuffer, ColumnBuilder columnBuilder)
    {
        final GenericIndexed column = GenericIndexed.read(byteBuffer, getObjectStrategy());
        columnBuilder.setComplexColumn(new ComplexColumnPartSupplier(getTypeName(), column));
    }

    @Override public ObjectStrategy getObjectStrategy()
    {
        return new ObjectStrategy()
        {
            @Override public Class getClazz()
            {
                return HLLCounter.class;
            }

            @Override public Object fromByteBuffer(ByteBuffer buffer,
                    int numBytes)
            {
                return HLLCModule.fromByteBuffer(buffer, numBytes);
            }

            @Override public byte[] toBytes(Object val)
            {
                if (val == null) {
                    return new byte[] {};
                }

                if (val instanceof HLLCounter) {
                    return HLLCModule.toBytes((HLLCounter) val);

                } else {
                    throw new IAE("Unknown class[%s], toString[%s]",
                            val.getClass(), val);
                }
            }

            @Override public int compare(Object o1, Object o2)
            {
                // TODO cardinality should be a member of HyperLogLog
                long card1 = ((HLLCounter) o1).getCountEstimate();
                long card2 = ((HLLCounter) o2).getCountEstimate();
                return (card1 > card2) ? 1 : (card1 < card2) ? -1 : 0;
            }
        };
    }

    @Override
    public GenericColumnSerializer getSerializer(IOPeon peon, String column)
    {
        return LargeColumnSupportedComplexColumnSerializer.create(peon, column, this.getObjectStrategy());
    }
}