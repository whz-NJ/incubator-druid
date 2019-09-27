package cn.migu.aggregation.kylin.hllc;

import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.measure.hllc.RegisterType;

/**
 * @author whz
 * @create 2019-09-26 17:26
 * @desc TODO: add description here
 **/
public class HLLCAggregator implements Aggregator
{

    private final ObjectColumnSelector selector;

    private HLLCounter hhlCounter;

    public HLLCAggregator(ObjectColumnSelector selector, Integer precision)
    {
        this.selector = selector;
        this.hhlCounter = new HLLCounter(precision, RegisterType.DENSE);
    }

    @Override public void aggregate()
    {
        hhlCounter.merge((HLLCounter) selector.getObject());
    }

    @Override public void reset()
    {
        hhlCounter = null;
    }

    @Override public Object get()
    {
        return hhlCounter;
    }

    @Override public float getFloat()
    {
        throw new UnsupportedOperationException(
                "HLLCAggregator does not support getFloat()");
    }

    @Override public long getLong()
    {
        throw new UnsupportedOperationException(
                "HLLCAggregator does not support getLong()");
    }

    @Override public void close()
    {
        hhlCounter = null;
    }
}