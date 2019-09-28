package cn.migu.aggregation.kylin.hllc;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ObjectColumnSelector;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.measure.hllc.RegisterType;

import java.nio.ByteBuffer;
import java.util.IdentityHashMap;

/**
 * @author whz
 * @create 2019-09-26 20:15
 * @desc TODO: add description here
 **/
public class HLLCBufferAggregator implements BufferAggregator
{
    private final ObjectColumnSelector selector;

    private Integer precision;

    private final IdentityHashMap<ByteBuffer, Int2ObjectMap<HLLCounter>> sketches = new IdentityHashMap<>();

    public HLLCBufferAggregator(ObjectColumnSelector selector, int precision)
    {
        this.selector = selector;
        this.precision = precision;
    }

    @Override public void init(ByteBuffer buf, int position)
    {
        createNewHLL(buf, position);
    }

    private HLLCounter createNewHLL(ByteBuffer buf, int position)
    {
        HLLCounter hllRet = new HLLCounter(precision, RegisterType.DENSE);
        Int2ObjectMap<HLLCounter> hllMap = sketches.get(buf);
        if (hllMap == null) {
            hllMap = new Int2ObjectOpenHashMap<>();
            sketches.put(buf, hllMap);
        }
        hllMap.put(position, hllRet);
        return hllRet;
    }

    //Note that this is not threadsafe and I don't think it needs to be
    private HLLCounter getHLL(ByteBuffer buf, int position)
    {
        Int2ObjectMap<HLLCounter> hllMap = sketches.get(buf);
        HLLCounter hll = hllMap != null ? hllMap.get(position) : null;
        if (hll != null) {
            return hll;
        }
        return createNewHLL(buf, position);
    }

    @Override public void aggregate(ByteBuffer buf, int position)
    {
        HLLCounter updateHLL = (HLLCounter) selector.getObject();
        if (updateHLL == null) {
            return;
        }

        HLLCounter currentHLL = getHLL(buf, position);
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
        Int2ObjectMap<HLLCounter> hllMap = sketches.get(oldBuffer);
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