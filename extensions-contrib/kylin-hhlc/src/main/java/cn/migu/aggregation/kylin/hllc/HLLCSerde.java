package cn.migu.aggregation.kylin.hhlc;

import io.druid.data.input.InputRow;
import io.druid.java.util.common.IAE;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;
import org.apache.kylin.measure.hllc.HLLCSerializer;
import org.apache.kylin.measure.hllc.HLLCounter;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author whz
 * @create 2019-09-26 17:19
 * @desc TODO: add description here
 **/
public class HHLCSerde extends ComplexMetricSerde
{
    HLLCSerializer hhlcSerializer = new HLLCSerializer();

    @Override public String getTypeName()
    {
        return HHLCModule.KYLIN_HHL_COUNT;
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
                final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
                readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
                byte[] bytes = new byte[readOnlyBuffer.remaining()];
                readOnlyBuffer.get(bytes, 0, bytes.length);
                try {
                    return HyperLogLog.Builder.build(bytes);
                }
                catch (IOException e) {
                    throw new IAE("failed to deserialize HyperLogLog", e);
                }
            }

            @Override public byte[] toBytes(Object val)
            {
                if (val == null) {
                    return new byte[] {};
                }

                if (val instanceof HyperLogLog) {
                    HyperLogLog value = (HyperLogLog) val;
                    try {
                        return value.getBytes();
                    }
                    catch (IOException e) {
                        throw new IAE("failed to serialize HyperLogLog", e);
                    }
                } else {
                    throw new IAE("Unknown class[%s], toString[%s]",
                            val.getClass(), val);
                }
            }

            @Override public int compare(Object o1, Object o2)
            {
                // TODO cardinality should be a member of HyperLogLog
                long card1 = ((HyperLogLog) o1).cardinality();
                long card2 = ((HyperLogLog) o2).cardinality();
                return (card1 > card2) ? 1 : (card1 < card2) ? -1 : 0;
            }
        };
    }

    @Override public void deserializeColumn(ByteBuffer byteBuffer,
            ColumnBuilder columnBuilder)
    {
        final GenericIndexed column = GenericIndexed
                .read(byteBuffer, getObjectStrategy());
        columnBuilder.setComplexColumn(
                new ComplexColumnPartSupplier(getTypeName(), column));
    }
}