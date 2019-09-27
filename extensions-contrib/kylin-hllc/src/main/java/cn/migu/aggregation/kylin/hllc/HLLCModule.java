package cn.migu.aggregation.kylin.hllc;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import io.druid.initialization.DruidModule;
import io.druid.segment.serde.ComplexMetrics;

import java.util.List;

/**
 * @author whz
 * @create 2019-09-26 17:15
 * @desc TODO: add description here
 **/
public class HLLCModule implements DruidModule
{

    public static final String KYLIN_HHL_COUNT = "kylin-hhlc";

    @Override public List<? extends Module> getJacksonModules()
    {
        return ImmutableList.of(new SimpleModule("KylinHHLCModule")
                .registerSubtypes(new NamedType(HLLCAggregatorFactory.class,
                        KYLIN_HHL_COUNT)));
    }

    @Override public void configure(Binder binder)
    {
        if (ComplexMetrics.getSerdeForType(KYLIN_HHL_COUNT) == null) {
            ComplexMetrics.registerSerde(KYLIN_HHL_COUNT, new HLLCSerde());
        }
    }

    public static int bytesToInt(byte[] bytes)
    {
        int b0 = bytes[0] & 0xFF;
        int b1 = bytes[1] & 0xFF;
        int b2 = bytes[2] & 0xFF;
        int b3 = bytes[3] & 0xFF;
        return (b0 << 24) | (b1 << 16) | (b2 << 8) | b3;
    }

    public static byte[] intToBytes(int i)
    {
        byte[] targets = new byte[4];
        targets[3] = (byte) (i & 0xFF);
        targets[2] = (byte) (i >> 8 & 0xFF);
        targets[1] = (byte) (i >> 16 & 0xFF);
        targets[0] = (byte) (i >> 24 & 0xFF);
        return targets;
    }
}