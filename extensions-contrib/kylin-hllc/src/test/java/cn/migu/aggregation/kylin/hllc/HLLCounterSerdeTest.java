package cn.migu.aggregation.kylin.hllc;

import io.druid.segment.data.ObjectStrategy;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * @author whz
 * @create 2019-09-27 14:02
 * @desc TODO: add description here
 **/
public class HLLCounterSerdeTest
{
    @Test public void testFromByteBuffer() throws Exception
    {
        HLLCounterSerde hllcSerde = new HLLCounterSerde();
        ObjectStrategy objectStrategy = hllcSerde.getObjectStrategy();
        WrappedHLLCounter hllc = new WrappedHLLCounter(10);
        for (int i = 0; i < 10; i++) {
            hllc.add(i);
        }
        assert ((hllc.getCountEstimate() > 7) && (hllc.getCountEstimate()
                < 13));

        byte[] bytes = objectStrategy.toBytes(hllc);
        WrappedHLLCounter hllc2 = (WrappedHLLCounter) objectStrategy
                .fromByteBuffer(ByteBuffer.wrap(bytes), bytes.length);
        assert (hllc.getCountEstimate() == hllc2.getCountEstimate());

        WrappedHLLCounter hllc3 = new WrappedHLLCounter(10);
        for (int i = 0; i < 100; i++) {
            hllc3.add(i);
        }
        assert ((hllc3.getCountEstimate() > 80) && (hllc3.getCountEstimate()
                < 110));

        bytes = objectStrategy.toBytes(hllc3);
        WrappedHLLCounter hllc4 = (WrappedHLLCounter) objectStrategy
                .fromByteBuffer(ByteBuffer.wrap(bytes), bytes.length);
        assert (hllc3.getCountEstimate() == hllc4.getCountEstimate());

    }
}