
package com.clearspring.analytics.stream.membership;

import java.nio.ByteBuffer;
import java.util.Random;

// Implements the algorithm from the paper:
// http://db.disi.unitn.eu/pages/VLDBProgram/pdf/research/p477-dutta.pdf
// Streaming Quotient Filter: A Near Optimal Approximate
// Duplicate Detection Approach for Data Streams
public class StreamingQuotientFilter
{
    private Random random = new Random();

    private final int quotientBits;
    private final int remainderBits;
    private final int reducedRemBits;
    private final int reducedRemBitMask;
    private final long remainderMask;
    private final int numRows;
    private final int bucketsPerRow;
    private final int bitsPerBucket;
    private final long bucketMask;
    private final int bitsPerRow;

    private ByteBuffer data;

    public StreamingQuotientFilter(int quotientBits, int bucketsPerRow, int bitsPerBucketValue) {
        if (quotientBits > 31) {
            throw new IllegalArgumentException("Too few remainder bits: must be at least 33");
        }
        if (bitsPerBucketValue > 63) {
            throw new IllegalArgumentException("bitsPerBucketValue must be <= 63");
        }
        this.quotientBits = quotientBits;
        this.remainderBits = 64 - quotientBits;
        this.remainderMask = (1L << remainderBits) - 1;
        this.bucketsPerRow = bucketsPerRow;
        // We need to store whether each bucket is empty or not (we shall store that in
        // the least significant bit), hence the extra bit.
        this.bitsPerBucket = bitsPerBucketValue + 1;
        this.bucketMask = (1L << bitsPerBucket) - 1;
        this.bitsPerRow = bucketsPerRow * bitsPerBucket;

        // Every bucket will have:
        // * number of 1's in remainder: A = ceil(log2(remainderBits)) bits
        // * reduced remainder: bitsPerBucket - A most significant bits of remainder
        // e.g. if remainderBits = 25, and bitsPerBucket = 12, then we need
        // A = ceil(log2(25)) = 5 bits for number of 1s.
        // Reduced remainder will have 12 - 5 = 7 bits for reduced remainder.
        // Reduced remainder mask will be 1 << 7 - 1.
        int popcntBits = (int)Math.ceil(Math.log(remainderBits)/Math.log(2));
        this.reducedRemBits = bitsPerBucketValue - popcntBits;
        if (reducedRemBits <= 0) {
            throw new IllegalArgumentException(
                "Too few bucket bits: at least " + popcntBits + " required, " +
                        bitsPerBucketValue + " specified");
        }
        this.reducedRemBitMask = (1 << this.reducedRemBits) - 1;

        this.numRows = 1 << quotientBits;
        int capacity = (numRows * bitsPerRow + 7)/8 + 7;

        data = ByteBuffer.allocate(capacity);
    }

    boolean add(long hash) {
        int row /* quot */ = (int)(hash >>> remainderBits); // guaranteed to not overflow
        long rem = hash & remainderMask;
        long onesInRem = (long)Long.bitCount(rem);
        long reducedRem = rem & reducedRemBitMask;

        long value = (onesInRem << reducedRemBits) | reducedRem;

        int emptyBucket = -1;
        for (int i = 0; i < bucketsPerRow; ++i) {
            int pos = row * bitsPerRow + i * bitsPerBucket;
            // Assemble bucketBits starting at pos bits.
            // This will require 1 or 2 longs.
            long valueAtPos;
            long left = data.getLong(8*(pos/64));
            int bitsFromLeft = 64 - pos%64;
            if (bitsFromLeft >= bitsPerBucket) {
                // Sufficient bits in the current word.
                // [.........XXXXXXXXXXXXXXXXX.....]
                // [  pos%64 | bitsPerBucket | nnn ]
                // nnn = 64 - pos%64 - bitsPerBucket = bitsFromLeft - bitsPerBucket
                valueAtPos = (left >>> (bitsFromLeft - bitsPerBucket)) & bucketMask;
            } else {
                long right = data.getLong(8*(pos/64 + 1));
                int bitsFromRight = bitsPerBucket - bitsFromLeft;
                // Concatenate rightmost bitsFromLeft and leftmost bitsFromRight.
                // [.......................] [......................]
                // [         | bitsFromLeft | bitsFromRight |      ]
                long partFromLeft = left & ((1 << bitsFromLeft) - 1);
                long partFromRight = right >>> (64 - bitsFromRight);
                valueAtPos = (partFromLeft << bitsFromRight) | partFromRight;
            }
            if ((valueAtPos & 1) == 0L) {
                if (emptyBucket == -1) {
                    emptyBucket = i;
                }
                continue;
            }
            valueAtPos >>>= 1;
            if (valueAtPos == value) {
                return false;
            }
        }
        if (emptyBucket == -1) {
            // No empty buckets, need to choose and overwrite one uniformly.
            emptyBucket = random.nextInt(bucketsPerRow);
        }
        int pos = row * bitsPerRow + emptyBucket * bitsPerBucket;
        // write value,1 as 'bitsPerBucket' bits starting at bit #pos.
        value = (value << 1) | 1;
        long left = data.getLong(8*(pos/64));
        int bitsFromLeft = 64 - pos%64;
        if (bitsFromLeft >= bitsPerBucket) {
            // Need to only modify left.
            // [.........XXXXXXXXXXXXXXXXX.....]
            // [  pos%64 | bitsPerBucket | nnn ]
            // nnn = 64 - pos%64 - bitsPerBucket = bitsFromLeft - bitsPerBucket
            long mask = (0xFFFFFFFFFFFFFFFFL << (64 - bitsPerBucket)) >>> (pos % 64);
            long positionedValue = value << (bitsFromLeft - bitsPerBucket);
            long newLeft = (left & ~mask) | positionedValue;
            data.putLong(8*(pos/64), newLeft);
        } else {
            // Need to modify both left and right.
            long right = data.getLong(8*(pos/64 + 1));
            int bitsFromRight = bitsPerBucket - bitsFromLeft;
            // Concatenate rightmost bitsFromLeft and leftmost bitsFromRight.
            // [.......................] [......................]
            // [         | bitsFromLeft | bitsFromRight |       ]
            // [         | ----------value------------- |       ]
            long leftMask = 0xFFFFFFFFFFFFFFFFL >>> (64 - bitsFromLeft);
            long leftPart = value >>> bitsFromRight;
            long rightMask = 0xFFFFFFFFFFFFFFFFL << (64 - bitsFromRight);
            long rightPart = value << (64 - bitsFromRight);
            data.putLong(8*(pos/64), (left & ~leftMask) | leftPart);
            data.putLong(8*(pos/64 + 1), (right & ~rightMask) | rightPart);
        }
        return true;
    }

    private String toBinary(long value, int bits) {
        char[] res = new char[bits];
        for (int i = 0; i < bits; ++i) {
            res[bits-i-1] = ((value&1) == 0) ? '0' : '1';
            value >>>= 1;
        }
        return new String(res);
    }

    private boolean getBit(int pos) {
        return (data.get(pos / 8) & (1L << (7 - pos % 8))) != 0;
    }

    public String bucketToString(int row, int bucket) {
        StringBuilder res = new StringBuilder();
        for (int i = 0; i < bitsPerBucket; ++i) {
            res.append(getBit(row * bitsPerRow + bucket * bitsPerBucket + i) ? '1' : '0');
        }
        return res.toString();
    }

    public String rowToString(int row) {
        StringBuilder res = new StringBuilder();
        for(int i = 0; i < bucketsPerRow; ++i) {
            if (res.length() > 0) {
                res.append("  ");
            }
            res.append(bucketToString(row, i));
        }
        return res.toString();
    }

    public String toString() {
        StringBuilder res = new StringBuilder();
        for (int i = 0; i < numRows; ++i) {
            res.append(toBinary(i, quotientBits)).append(": ");
            res.append(rowToString(i)).append("\n");
        }
        return res.toString();
    }

    public void setRandom(Random random) {
        this.random = random;
    }
}
