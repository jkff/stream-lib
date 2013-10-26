package com.clearspring.analytics.util.vla;

public class VarintCounterArray
{
    private SparseByteArray bytes;
    private SparseShortArray shorts;
    private SparseIntArray ints;
    private SparseLongArray longs;
    private SparseByteArray.Pointer byteP = new SparseByteArray.Pointer();
    private SparseShortArray.Pointer shortP = new SparseShortArray.Pointer();
    private SparseIntArray.Pointer intP = new SparseIntArray.Pointer();
    private SparseLongArray.Pointer longP = new SparseLongArray.Pointer();
    private long[] codes;
    // TODO: Reuse the 'codes' array in the item arrays, instead of using
    // separate bitsets.

    public VarintCounterArray(int capacity) {
        int blockSizeBytes = 1024;
        this.bytes = new SparseByteArray(capacity, blockSizeBytes, 4);
        this.shorts = new SparseShortArray(capacity, blockSizeBytes/2, 2);
        this.ints = new SparseIntArray(capacity, blockSizeBytes/4, 1);
        this.longs = new SparseLongArray(capacity, blockSizeBytes/8, 1);
        this.codes = new long[capacity/32 + 1];
    }

    public long get(int i) {
        switch (whichArray(i)) {
        case 0:
            return bytes.get(i) & 0xFFL;
        case 1:
            return shorts.get(i) & 0xFFFFL;
        case 2:
            return ints.get(i) & 0xFFFFFFFFL;
        case 3:
            return longs.get(i);
        default:
            throw new AssertionError();
        }
    }

    public void set(int i, long value)
    {
        byte which = whichArray(i);
        switch(which) {
        case 0:
            if (value <= 0xFF) {
                bytes.set(i, (byte)value);
                return;
            }
            bytes.set(byteP, (byte)0);
            setInternal(i, value);
            return;

        case 1:
            if (value <= 0xFFFF) {
                shorts.set(shortP, (short) value);
                return;
            }
            shorts.set(shortP, (short)0);
            setInternal(i, value);
            return;

        case 2:
            if (value < 0xFFFFFFFFL) {
                ints.set(intP, (int) value);
                return;
            }
            ints.set(intP, 0);
            setInternal(i, value);
            return;

        case 3:
            longs.locate(i, longP, false);
            longs.set(longP, value);
            return;

        default:
            throw new AssertionError();
        }
    }

    public void addTo(int i, long increment) {
        assert increment > 0;
        byte which = whichArray(i);
        switch(which) {
        case 0:
            bytes.locate(i, byteP, false);
            long bCur = byteP.value & 0xFF;
            if (increment + bCur <= 0xFF) {
                bytes.set(byteP, (byte)(bCur + increment));
                return;
            }
            bytes.set(byteP, (byte)0);
            setInternal(i, bCur + increment);
            return;

        case 1:
            shorts.locate(i, shortP, false);
            long sCur = shortP.value & 0xFFFF;
            if (increment + sCur <= 0xFFFF) {
                shorts.set(shortP, (short) (sCur + increment));
                return;
            }
            shorts.set(shortP, (short)0);
            setInternal(i, sCur + increment);
            return;

        case 2:
            ints.locate(i, intP, false);
            long iCur = intP.value & 0xFFFFFFFFL;
            if (increment < 0xFFFFFFFFL - iCur) {
                ints.set(intP, (int) (iCur + increment));
                return;
            }
            ints.set(intP, 0);
            setInternal(i, iCur + increment);
            return;

        case 3:
            longs.locate(i, longP, false);
            longs.set(longP, longP.value + increment);
            return;

        default:
            throw new AssertionError();
        }
    }

    private void setInternal(int i, long value) {
        if (value == (byte)value) {
            bytes.locate(i, byteP, false);
            bytes.set(byteP, (byte)value);
            setWhichArray(i, (byte)0);
        } else if (value == (short)value) {
            shorts.locate(i, shortP, false);
            shorts.set(shortP, (short) value);
            setWhichArray(i, (byte)1);
        } else if (value == (int)value) {
            ints.locate(i, intP, false);
            ints.set(intP, (int) value);
            setWhichArray(i, (byte)2);
        } else {
            longs.locate(i, longP, false);
            longs.set(longP, value);
            setWhichArray(i, (byte)3);
        }
    }

    private byte whichArray(int i) {
        // bits 2*i, 2*i+1 encode which array the item is in.
        long word = codes[i/32];
        byte rem = (byte)((2*i) % 64);
        // (2*x) % (2*32) = 2 * (x%32)
        return (byte) (((byte)(word >>> (62 - rem))) & 3);
    }

    private void setWhichArray(int i, byte which) {
        byte rem = (byte)((2*i)%64);
        // set bits rem, rem+1 to 'which'
        switch(which) {
        case 0:
            codes[i/32] &= ~(0xC000000000000000L >>> rem);
            break;
        case 1:
            codes[i/32] &= ~(0x8000000000000000L >>> rem);
            codes[i/32] |= (0x8000000000000000L >>> (rem+1));
            break;
        case 2:
            codes[i/32] |= (0x8000000000000000L >>> rem);
            codes[i/32] &= ~(0x8000000000000000L >>> (rem+1));
            break;
        case 3:
            codes[i/32] |= (0xC000000000000000L >>> rem);
            break;

        default:
            throw new AssertionError();
        }
    }

    public int bytesUsed() {
        return bytes.bytesUsed() + shorts.bytesUsed() + ints.bytesUsed() + longs.bytesUsed() + 8 * codes.length;
    }
}
