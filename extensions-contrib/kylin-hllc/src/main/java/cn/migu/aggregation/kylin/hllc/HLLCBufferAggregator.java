package cn.migu.aggregation.kylin.hllc;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnValueSelector;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.nio.ByteBuffer;
import java.util.IdentityHashMap;

/**
 * @author whz
 * @create 2019-09-26 20:15
 * @desc TODO: add description here
 **/
public class HLLCBufferAggregator implements BufferAggregator
{
    private final ColumnValueSelector selector;

    private Integer precision;

    private final IdentityHashMap<ByteBuffer, Int2ObjectMap<WrappedHLLCounter>> sketches = new IdentityHashMap<>();

    public HLLCBufferAggregator(ColumnValueSelector selector, int precision)
    {
        this.selector = selector;
        this.precision = precision;
    }

    @Override public void init(ByteBuffer buf, int position)
    {
        createNewHLL(buf, position);
    }

    private WrappedHLLCounter createNewHLL(ByteBuffer buf, int position)
    {
        WrappedHLLCounter wrappedHLLCounter = new WrappedHLLCounter(precision);
        Int2ObjectMap<WrappedHLLCounter> hllMap = sketches.get(buf);
        if (hllMap == null) {
            hllMap = new Int2ObjectOpenHashMap<>();
            sketches.put(buf, hllMap);
        }
        hllMap.put(position, wrappedHLLCounter);
        return wrappedHLLCounter;
    }

    //Note that this is not threadsafe and I don't think it needs to be
    private WrappedHLLCounter getHLL(ByteBuffer buf, int position)
    {
        Int2ObjectMap<WrappedHLLCounter> hllMap = sketches.get(buf);
        WrappedHLLCounter hll = hllMap != null ? hllMap.get(position) : null;
        if (hll != null) {
            return hll;
        }
        return createNewHLL(buf, position);
    }

    @Override public void aggregate(ByteBuffer buf, int position)
    {
        WrappedHLLCounter updateHLL = (WrappedHLLCounter) selector.getObject();
        if (updateHLL == null) {
            return;
        }

        WrappedHLLCounter currentHLL = getHLL(buf, position);
        currentHLL.merge(updateHLL);
    }

    @Override public Object get(ByteBuffer buf, int position)
    {
        return getHLL(buf, position);
    }

    @Override public float getFloat(ByteBuffer buf, int position)
    {
        throw new UnsupportedOperationException(
                "HLLCBufferAggregator does not support getFloat()");
    }

    @Override public long getLong(ByteBuffer buf, int position)
    {
        throw new UnsupportedOperationException(
                "HLLCBufferAggregator does not support getLong()");
    }

    @Override public void relocate(int oldPosition, int newPosition,
            ByteBuffer oldBuffer, ByteBuffer newBuffer)
    {
        createNewHLL(newBuffer, newPosition);
        Int2ObjectMap<WrappedHLLCounter> hllMap = sketches.get(oldBuffer);
        if (hllMap != null) {
            sketches.get(newBuffer).put(newPosition, hllMap.get(oldPosition));
            hllMap.remove(oldPosition);
            if (hllMap.isEmpty()) {
                sketches.remove(oldBuffer);
            }
        }
    }

    @Override public void close()
    {

    }
}