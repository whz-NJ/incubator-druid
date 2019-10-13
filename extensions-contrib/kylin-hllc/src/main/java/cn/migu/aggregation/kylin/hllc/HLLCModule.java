package cn.migu.aggregation.kylin.hllc;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import io.druid.initialization.DruidModule;
import io.druid.java.util.common.IAE;
import io.druid.segment.serde.ComplexMetrics;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.measure.hllc.RegisterType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author whz
 * @create 2019-09-26 17:15
 * @desc TODO: add description here
 **/
public class HLLCModule implements DruidModule
{

    public static final String KYLIN_HLL_COUNT = "kylin-hllc";

    private static ThreadLocal<ByteBuffer> hllByteBuf = new ThreadLocal<>();

    @Override public List<? extends Module> getJacksonModules()
    {
        return ImmutableList.of(new SimpleModule("KylinHLLCModule")
                .registerSubtypes(new NamedType(HLLCAggregatorFactory.class,
                        KYLIN_HLL_COUNT)));
    }

    @Override public void configure(Binder binder)
    {
        if (ComplexMetrics.getSerdeForType(KYLIN_HLL_COUNT) == null) {
            ComplexMetrics.registerSerde(KYLIN_HLL_COUNT, new HLLCounterSerde());
        }
    }

    public static int bytesToInt(byte[] bytes, int offset)
    {
        int b0 = bytes[offset] & 0xFF;
        int b1 = bytes[offset + 1] & 0xFF;
        int b2 = bytes[offset + 2] & 0xFF;
        int b3 = bytes[offset + 3] & 0xFF;
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

    public static byte[] toBytes(HLLCounter hllCounter)
    {
        ByteBuffer hllBuf = hllByteBuf.get();
        if (hllBuf == null) {
            hllBuf = ByteBuffer.allocate(
                    BufferedMeasureCodec.DEFAULT_BUFFER_SIZE);
            hllByteBuf.set(hllBuf);
        }
        try {
            hllBuf.clear();
            hllBuf.put(intToBytes(hllCounter.getPrecision()));
            hllBuf.put(intToBytes(hllCounter.getRegisterType().ordinal()));
            hllCounter.writeRegisters(hllBuf);
            hllBuf.flip();
            byte[] bytes = new byte[hllBuf.remaining()];
            hllBuf.get(bytes);
            return bytes;
        }
        catch (IOException e) {
            throw new IAE("failed to serialize HLLCounter", e);
        }
    }

    public static HLLCounter fromByteBuffer(ByteBuffer buffer,
            int numBytes)
    {
        // Be conservative, don't assume we own this buffer.
        final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
        readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);

        byte[] bytes = new byte[readOnlyBuffer.remaining()];
        readOnlyBuffer.get(bytes, 0, bytes.length);

        return fromBytes(bytes);
    }

    public static HLLCounter fromBytes(byte[] bytes)
    {
        int precision = bytesToInt(bytes, 0);
        RegisterType registerType = RegisterType.values()[bytesToInt(bytes, Integer.BYTES)];
        HLLCounter hllCounter = new HLLCounter(precision, registerType);

        ByteBuffer hllBuffer = ByteBuffer.wrap(bytes, 2 * Integer.BYTES,
                (bytes.length - (2 * Integer.BYTES)));
        try {
            hllCounter.readRegisters(hllBuffer);
        }
        catch (IOException e) {
            throw new IAE("failed to deserialize HLLCounter", e);
        }
        return hllCounter;
    }
}