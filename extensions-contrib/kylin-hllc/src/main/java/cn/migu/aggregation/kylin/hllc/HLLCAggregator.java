package cn.migu.aggregation.kylin.hllc;

import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;

/**
 * @author whz
 * @create 2019-09-26 17:26
 * @desc TODO: add description here
 **/
public class HLLCAggregator implements Aggregator
{

    private final ObjectColumnSelector selector;

    private WrappedHLLCounter wrappedHLLCounter;

    public HLLCAggregator(ObjectColumnSelector selector, Integer precision)
    {
        this.selector = selector;
        this.wrappedHLLCounter = new WrappedHLLCounter(precision);
    }

    @Override public void aggregate()
    {
        wrappedHLLCounter.merge((WrappedHLLCounter) selector.getObject());
    }

    @Override public void reset()
    {
        wrappedHLLCounter = null;
    }

    @Override public Object get()
    {
        return wrappedHLLCounter;
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
        wrappedHLLCounter = null;
    }
}