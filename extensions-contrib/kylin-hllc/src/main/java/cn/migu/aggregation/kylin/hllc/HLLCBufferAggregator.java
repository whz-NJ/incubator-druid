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
        HLLCounter hhlRet = new HLLCounter(precision, RegisterType.DENSE);
        Int2ObjectMap<HLLCounter> hhlMap = sketches.get(buf);
        if (hhlMap == null) {
            hhlMap = new Int2ObjectOpenHashMap<>();
            sketches.put(buf, hhlMap);
        }
        hhlMap.put(position, hhlRet);
        return hhlRet;
    }

    //Note that this is not threadsafe and I don't think it needs to be
    private HLLCounter getHHL(ByteBuffer buf, int position)
    {
        Int2ObjectMap<HLLCounter> hhlMap = sketches.get(buf);
        HLLCounter hhl = hhlMap != null ? hhlMap.get(position) : null;
        if (hhl != null) {
            return hhl;
        }
        return createNewHLL(buf, position);
    }

    @Override public void aggregate(ByteBuffer buf, int position)
    {
        HLLCounter updateHHL = (HLLCounter) selector.getObject();
        if (updateHHL == null) {
            return;
        }

        HLLCounter currentHHL = getHHL(buf, position);
        currentHHL.merge(updateHHL);
    }

    @Override public Object get(ByteBuffer buf, int position)
    {
        return getHHL(buf, position);
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
        Int2ObjectMap<HLLCounter> hhlMap = sketches.get(oldBuffer);
        if (hhlMap != null) {
            sketches.get(newBuffer).put(newPosition, hhlMap.get(oldPosition));
            hhlMap.remove(oldPosition);
            if (hhlMap.isEmpty()) {
                sketches.remove(oldBuffer);
            }
        }
    }

    @Override public void close()
    {

    }
}