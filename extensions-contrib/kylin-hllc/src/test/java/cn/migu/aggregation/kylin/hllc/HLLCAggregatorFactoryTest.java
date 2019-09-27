package cn.migu.aggregation.kylin.hllc;

import io.druid.segment.data.ObjectStrategy;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.measure.hllc.RegisterType;
import org.junit.Test;

/**
 * @author whz
 * @create 2019-09-27 15:09
 * @desc TODO: add description here
 **/
public class HLLCAggregatorFactoryTest
{
    @Test public void testDeserialize()
    {
        int precision = 10;
        HLLCAggregatorFactory factory = new HLLCAggregatorFactory("name",
                "field", precision);
        HLLCSerde hllcSerde = new HLLCSerde();
        ObjectStrategy objectStrategy = hllcSerde.getObjectStrategy();
        HLLCounter hllc = new HLLCounter(precision, RegisterType.DENSE);
        for (int i = 0; i < 10; i++) {
            hllc.add(i);
        }
        assert ((hllc.getCountEstimate() > 7) && (hllc.getCountEstimate()
                < 13));

        byte[] bytes = objectStrategy.toBytes(hllc);
        HLLCounter hllc2 = (HLLCounter) factory.deserialize(bytes);

        assert (hllc.getCountEstimate() == hllc2.getCountEstimate());
    }
}