package com.clearspring.analytics.util.vla;

import java.util.Arrays;

public class SparseLongArray
{
    public static final double GROWTH_FACTOR = 1.2;
    private final int itemsPerBlock;
    private final long[] isSet;
    private final byte[] setCounts;
    // Group entries into blocks of up to itemsPerBlock items. Overhead per block ~ 16 bytes.
    // Every block is completely rebuilt when inserting/removing entries.
    private final long[][] blocks;
    // Number of items actually present in each block.
    private final int[] blockSizes;
    private final int initialBlockCapacity;

    public static class Pointer {
        boolean present;
        long value;
        int whichBlock;
        int whichEntryInBlock;
        int whichBitBlock;
        int bitBlockOffset;
    }

    public SparseLongArray(int capacity, int itemsPerBlock, int initialBlockCapacity) {
        if (itemsPerBlock % 64 != 0) {
            // Round up to a multiple of 64, to simplify indexing into isSet.
            itemsPerBlock += 64 - (itemsPerBlock % 64);
        }
        this.itemsPerBlock = itemsPerBlock;
        this.isSet = new long[capacity/64 + 1];
        this.setCounts = new byte[capacity/64 + 1];
        this.blocks = new long[capacity/itemsPerBlock+1][];
        this.blockSizes = new int[capacity/itemsPerBlock+1];
        this.initialBlockCapacity = initialBlockCapacity;
    }

    public long get(int i) {
        Pointer p = new Pointer();
        locate(i, p, true);
        return p.value;
    }

    public void set(int i, long value) {
        Pointer out = new Pointer();
        locate(i, out, false);
        set(out, value);
    }

    public void locate(int i, Pointer out, boolean onlyIfPresent) {
        int iWord = i/64;
        int iRem = i % 64;
        if (0 == (isSet[iWord] & (0x8000000000000000L >>> iRem))) {
            out.present = false;
            out.value = 0;
            if (onlyIfPresent) {
                return;
            }
        } else {
            out.present = true;
        }
        int whichBlock = i / itemsPerBlock;
        int blockStart = whichBlock * itemsPerBlock;
        // Compute number of ones in isSet[blockStart..i).
        int whichEntryInBlock = 0;
        int j = blockStart / 64;
        while (j < iWord) {
            whichEntryInBlock += setCounts[j++];
        }
        // Signed right shift used intentionally: get iRem-1 ones on the left.
        if (iRem > 0) {
            whichEntryInBlock += Long.bitCount(isSet[j] & (0x8000000000000000L >> (iRem - 1)));
        }
        out.whichBlock = whichBlock;
        out.whichEntryInBlock = whichEntryInBlock;
        out.whichBitBlock = j;
        out.bitBlockOffset = iRem;
        if (out.present) {
            out.value = blocks[whichBlock][whichEntryInBlock];
        }
    }

    public void set(Pointer p, long value) {
        int whichBlock = p.whichBlock;
        int whichEntryInBlock = p.whichEntryInBlock;
        int whichBitBlock = p.whichBitBlock;
        if (p.present) {
            long[] block = blocks[whichBlock];
            if (value != 0) {
                // Entry exists, overwrite it.
                block[whichEntryInBlock] = value;
            } else {
                // Delete existing entry: rewrite the block.
                int size = blockSizes[whichBlock];
                if (size - 1 <= block.length / 4) {
                    // Shrink block 2x
                    long[] newBlock = Arrays.copyOf(block, block.length/2);
                    block = blocks[whichBlock] = newBlock;
                }
                // Shift items from whichEntryInBlock+1 left by 1.
                System.arraycopy(
                        block, whichEntryInBlock + 1,
                        block, whichEntryInBlock, size - whichEntryInBlock - 1);
                block[size-1] = 0;
                blockSizes[whichBlock]--;
                isSet[whichBitBlock] &= ~(0x8000000000000000L >>> p.bitBlockOffset);
                setCounts[whichBitBlock]--;
            }
        } else if (value != 0) {
            long[] block = blocks[whichBlock];
            // Entry does not exist, rewrite the block.
            if (block == null) {
                // Create a new block with initial capacity 4.
                // 4 because more will introduce overhead for near-empty buckets,
                // and less will cause too much thrashing when adding more items
                // to the block. But maybe better to make this a parameter.
                block = blocks[whichBlock] = new long[initialBlockCapacity];
            }
            int size = blockSizes[whichBlock];
            if (size + 1 >= block.length) {
                block = Arrays.copyOf(blocks[whichBlock], (int)Math.max(size + 1, GROWTH_FACTOR * block.length));
                System.arraycopy(blocks[whichBlock], 0, block, 0, whichEntryInBlock);
                blocks[whichBlock] = block;
            }
            System.arraycopy(
                    block, whichEntryInBlock,
                    block, whichEntryInBlock + 1, size - whichEntryInBlock);
            block[whichEntryInBlock] = value;
            blockSizes[whichBlock]++;
            isSet[whichBitBlock] |= (0x8000000000000000L >>> p.bitBlockOffset);
            setCounts[whichBitBlock]++;
        } // else value == 0 - stays absent.
    }

    public int bytesUsed() {
        int ARRAY_HEADER_SIZE = 16;
        int OBJECT_HEADER_SIZE = 12;
        int totalBlockSizes = 0;
        for (long[] block : blocks) {
            if (block != null) {
                totalBlockSizes += ARRAY_HEADER_SIZE + Long.SIZE * block.length;
            }
        }
        return OBJECT_HEADER_SIZE +
               4 + // itemsPerBlock
               ARRAY_HEADER_SIZE + 8 * isSet.length +
               ARRAY_HEADER_SIZE + setCounts.length +
               ARRAY_HEADER_SIZE + 8 * blocks.length +
               totalBlockSizes +
               ARRAY_HEADER_SIZE + 4 * blockSizes.length +
               4; // initialBlockCapacity
    }

    public void printBytesUsed() {
        int ARRAY_HEADER_SIZE = 16;
        int totalBlockSizes = 0;
        for (long[] block : blocks) {
            if (block != null) {
                totalBlockSizes += ARRAY_HEADER_SIZE + block.length;
            }
        }
        System.out.println("isSet: " + (ARRAY_HEADER_SIZE + 8 * isSet.length));
        System.out.println("setCounts: " + (ARRAY_HEADER_SIZE + setCounts.length));
        System.out.println("blocks: " + (ARRAY_HEADER_SIZE + 8 * blocks.length));
        System.out.println("block contents (#blocks=" + blocks.length + ", overhead=" + (ARRAY_HEADER_SIZE * blocks.length) + "): " + totalBlockSizes);
        System.out.println("block sizes: " + (ARRAY_HEADER_SIZE + 4 * blockSizes.length));
    }
}
