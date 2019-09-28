package cn.migu.aggregation.kylin.hllc;

import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.measure.hllc.RegisterType;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author whz
 * @create 2019-09-28 19:33
 * @desc TODO: add description here
 **/
public class WrappedHLLCounter
{
    private HLLCounter hllCounter;

    public WrappedHLLCounter(HLLCounter hllCounter)
    {
        this.hllCounter = hllCounter;
    }

    public WrappedHLLCounter(int precision)
    {
        this.hllCounter = new HLLCounter(precision, RegisterType.DENSE);
    }

    @JsonValue public byte[] toBytes()
    {
        return HLLCModule.toBytes(this);
    }

    public long getCountEstimate()
    {
        return hllCounter.getCountEstimate();
    }

    public HLLCounter getHllCounter()
    {
        return hllCounter;
    }

    public void merge(WrappedHLLCounter anotherHLL)
    {
        hllCounter.merge(anotherHLL.hllCounter);
    }

    public int getPrecision()
    {
        return hllCounter.getPrecision();
    }
    public void writeRegisters(ByteBuffer byteBuffer) throws IOException
    {
        hllCounter.writeRegisters(byteBuffer);
    }

    public void add(int i)
    {
        hllCounter.add(i);
    }
}