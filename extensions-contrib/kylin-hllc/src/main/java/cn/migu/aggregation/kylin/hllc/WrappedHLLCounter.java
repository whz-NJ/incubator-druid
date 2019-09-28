package cn.migu.aggregation.kylin.hllc;

import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.kylin.measure.hllc.HLLCounter;

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

    @JsonValue public byte[] toBytes()
    {
        return HLLCModule.toBytes(hllCounter);
    }

    public long getCountEstimate()
    {
        return hllCounter.getCountEstimate();
    }

    public HLLCounter getHllCounter()
    {
        return hllCounter;
    }
}