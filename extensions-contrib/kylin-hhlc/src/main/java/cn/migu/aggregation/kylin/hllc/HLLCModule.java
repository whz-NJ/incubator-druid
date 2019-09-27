package cn.migu.aggregation.kylin.hhlc;

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
public class HHLCModule implements DruidModule
{

    public static final String KYLIN_HHL_COUNT = "kylin-hhlc";

    @Override public List<? extends Module> getJacksonModules()
    {
        return ImmutableList.of(new SimpleModule("KylinHHLCModule")
                .registerSubtypes(new NamedType(HHLCAggregatorFactory.class,
                        KYLIN_HHL_COUNT)));
    }

    @Override public void configure(Binder binder)
    {
        if (ComplexMetrics.getSerdeForType(KYLIN_HHL_COUNT) == null) {
            ComplexMetrics.registerSerde(KYLIN_HHL_COUNT, new HHLCSerde());
        }
    }
}