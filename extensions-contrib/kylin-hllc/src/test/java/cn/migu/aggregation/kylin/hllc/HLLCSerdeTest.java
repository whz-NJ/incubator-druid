package cn.migu.aggregation.kylin.hllc;

import io.druid.segment.data.ObjectStrategy;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.measure.hllc.RegisterType;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * @author whz
 * @create 2019-09-27 14:02
 * @desc TODO: add description here
 **/
public class HLLCSerdeTest
{
    @Test public void testFromByteBuffer() throws Exception
    {
        HLLCSerde hllcSerde = new HLLCSerde();
        ObjectStrategy objectStrategy = hllcSerde.getObjectStrategy();
        HLLCounter hllc = new HLLCounter(10, RegisterType.DENSE);
        for (int i = 0; i < 10; i++) {
            hllc.add(i);
        }
        assert ((hllc.getCountEstimate() > 7) && (hllc.getCountEstimate()
                < 13));

        byte[] bytes = objectStrategy.toBytes(hllc);
        HLLCounter hllc2 = (HLLCounter) objectStrategy
                .fromByteBuffer(ByteBuffer.wrap(bytes), bytes.length);
        assert (hllc.getCountEstimate() == hllc2.getCountEstimate());

        HLLCounter hllc3 = new HLLCounter(10, RegisterType.DENSE);
        for (int i = 0; i < 100; i++) {
            hllc3.add(i);
        }
        assert ((hllc3.getCountEstimate() > 80) && (hllc3.getCountEstimate()
                < 110));

        bytes = objectStrategy.toBytes(hllc3);
        HLLCounter hllc4 = (HLLCounter) objectStrategy
                .fromByteBuffer(ByteBuffer.wrap(bytes), bytes.length);
        assert (hllc3.getCountEstimate() == hllc4.getCountEstimate());

    }
}