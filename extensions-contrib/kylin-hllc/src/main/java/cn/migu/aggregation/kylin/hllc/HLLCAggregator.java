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

    private HLLCounter hllCounter;

    public HLLCAggregator(ObjectColumnSelector selector, Integer precision)
    {
        this.selector = selector;
        this.hllCounter = new HLLCounter(precision, RegisterType.DENSE);
    }

    @Override public void aggregate()
    {
        hllCounter.merge((HLLCounter) selector.getObject());
    }

    @Override public void reset()
    {
        hllCounter = null;
    }

    @Override public Object get()
    {
        return hllCounter;
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
        hllCounter = null;
    }
}