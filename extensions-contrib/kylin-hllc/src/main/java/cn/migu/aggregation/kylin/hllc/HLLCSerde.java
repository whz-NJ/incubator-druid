package cn.migu.aggregation.kylin.hllc;

import io.druid.data.input.InputRow;
import io.druid.java.util.common.IAE;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.measure.hllc.RegisterType;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author whz
 * @create 2019-09-26 17:19
 * @desc TODO: add description here
 **/
public class HLLCSerde extends ComplexMetricSerde
{
    private ThreadLocal<ByteBuffer> hllByteBuf = new ThreadLocal<>();

    @Override public String getTypeName()
    {
        return HLLCModule.KYLIN_HHL_COUNT;
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
                // Be conservative, don't assume we own this buffer.
                final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
                readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);

                byte[] bytes = new byte[readOnlyBuffer.remaining()];
                readOnlyBuffer.get(bytes, 0, bytes.length);

                int precision = HLLCModule.bytesToInt(bytes);
                HLLCounter hllCounter = new HLLCounter(precision,
                        RegisterType.DENSE);

                ByteBuffer hllBuffer = ByteBuffer.wrap(bytes, Integer.BYTES,
                        (bytes.length - Integer.BYTES));
                try {
                    hllCounter.readRegisters(hllBuffer);
                }
                catch (IOException e) {
                    throw new IAE("failed to deserialize HLLCounter", e);
                }
                return hllCounter;
            }

            @Override public byte[] toBytes(Object val)
            {
                if (val == null) {
                    return new byte[] {};
                }

                if (val instanceof HLLCounter) {
                    HLLCounter hllCounter = (HLLCounter) val;
                    ByteBuffer hllBuf = hllByteBuf.get();
                    if (hllBuf == null) {
                        hllBuf = ByteBuffer.allocate(
                                BufferedMeasureCodec.DEFAULT_BUFFER_SIZE);
                        hllByteBuf.set(hllBuf);
                    }
                    try {
                        hllBuf.clear();
                        hllBuf.put(HLLCModule
                                .intToBytes(hllCounter.getPrecision()));
                        hllCounter.writeRegisters(hllBuf);
                        hllBuf.flip();
                        byte[] bytes = new byte[hllBuf.remaining()];
                        hllBuf.get(bytes);
                        return bytes;
                    }
                    catch (IOException e) {
                        throw new IAE("failed to serialize HLLCounter", e);
                    }

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

    @Override public void deserializeColumn(ByteBuffer byteBuffer,
            ColumnBuilder columnBuilder)
    {
        final GenericIndexed column = GenericIndexed
                .read(byteBuffer, getObjectStrategy());
        columnBuilder.setComplexColumn(
                new ComplexColumnPartSupplier(getTypeName(), column));
    }
}