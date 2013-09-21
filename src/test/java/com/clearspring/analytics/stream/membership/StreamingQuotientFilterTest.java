package com.clearspring.analytics.stream.membership;

import org.junit.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StreamingQuotientFilterTest
{
    public StreamingQuotientFilterTest()
    {
    }

    @Test
    public void testWhiteBox()
    {
        // 4 bits for quotient (60 bits for remainder) = 16 rows
        // 5 buckets, 10 bits per bucket.
        //
        // Bucket size:
        // Popcnt of remainder can have up to 6 bits.
        // Reduced remainder will have 4 bits.
        // + 1 extra presence bit
        // "Real" bucket size 11 bits.
        //
        // Row size 55 bits.
        // Total object size 55*16 = 880 bits = 110 bytes.
        StreamingQuotientFilter f = new StreamingQuotientFilter(4, 5, 10);
        // 0001,00100011010001010110011110001001000010101011110011010100,1001
        // Row 1, bit count in remainder 27 (011011), reduced remainder 1001
        // Bucket 0 was empty.
        assertTrue(f.add(0x1234567890ABCD49L));
        assertEquals("00000000000", f.bucketToString(0, 4));
        assertEquals("01101110011", f.bucketToString(1, 0));
        assertEquals("00000000000", f.bucketToString(1, 1));

        assertFalse(f.add(0x1234567890ABCD49L));
        assertEquals("00000000000", f.bucketToString(0, 4));
        assertEquals("01101110011", f.bucketToString(1, 0));
        assertEquals("00000000000", f.bucketToString(1, 1));

        // 0001,00100011010001010110011110001001000010101011110011010100,1000
        // Row 1, bit count in remainder 26 (011010), reduced remainder 1000
        assertTrue(f.add(0x1234567890ABCD48L));
        assertEquals("00000000000", f.bucketToString(0, 4));
        assertEquals("01101110011", f.bucketToString(1, 0));
        assertEquals("01101010001", f.bucketToString(1, 1));

        assertFalse(f.add(0x1234567890ABCD49L));
        assertFalse(f.add(0x1234567890ABCD48L));

        // 0001,00100011010001010110011110001001000010101011110011010100,0111
        // Row 1, bit count in remainder 28 (011100), reduced remainder 0111
        assertTrue(f.add(0x1234567890ABCD47L));
        assertEquals("00000000000", f.bucketToString(0, 4));
        assertEquals("01101110011", f.bucketToString(1, 0));
        assertEquals("01101010001", f.bucketToString(1, 1));
        assertEquals("01110001111", f.bucketToString(1, 2));

        // 0001,00100011010001010110011110001001000010101011110011010100,0110
        // Row 1, bit count in remainder 27 (011011), reduced remainder 0110
        assertTrue(f.add(0x1234567890ABCD46L));
        assertEquals("00000000000", f.bucketToString(0, 4));
        assertEquals("01101110011", f.bucketToString(1, 0));
        assertEquals("01101010001", f.bucketToString(1, 1));
        assertEquals("01110001111", f.bucketToString(1, 2));
        assertEquals("01101101101", f.bucketToString(1, 3));

        // 0001,00100011010001010110011110001001000010101011110011010100,0101
        // Row 1, bit count in remainder 27 (011011), reduced remainder 0101
        assertTrue(f.add(0x1234567890ABCD45L));
        assertEquals("00000000000", f.bucketToString(0, 4));
        assertEquals("01101110011", f.bucketToString(1, 0));
        assertEquals("01101010001", f.bucketToString(1, 1));
        assertEquals("01110001111", f.bucketToString(1, 2));
        assertEquals("01101101101", f.bucketToString(1, 3));
        assertEquals("01101101011", f.bucketToString(1, 4));

        assertFalse(f.add(0x1234567890ABCD45L));
        assertFalse(f.add(0x1234567890ABCD46L));
        assertFalse(f.add(0x1234567890ABCD47L));
        assertFalse(f.add(0x1234567890ABCD48L));
        assertFalse(f.add(0x1234567890ABCD49L));

        // 0001,00100011010001010110011110001001000000000000110011010000,0010
        // Row 1, bit count in remainder 20 (010101), reduced remainder 0010
        // Row 1 is now full, need to evict a bucket. Let's say it'll be bucket 2.
        f.setRandom(new Random() {
            @Override
            public int nextInt(int n) {
                return 2;
            }
        });
        assertTrue(f.add(0x123456789000CD02L));
        assertEquals("00000000000", f.bucketToString(0, 4));
        assertEquals("01101110011", f.bucketToString(1, 0));
        assertEquals("01101010001", f.bucketToString(1, 1));
        assertEquals("01010000101", f.bucketToString(1, 2)); // changed
        assertEquals("01101101101", f.bucketToString(1, 3));
        assertEquals("01101101011", f.bucketToString(1, 4));

        // 1101,111010101101111100000000110100011110111001111011001100111111
        // Row 13, bit count in remainder 36 (100100), reduced remainder 1111
        assertTrue(f.add(0xDEADF00D1EE7B33FL));
        assertEquals("10010011111", f.bucketToString(13, 0));
    }

    @Test
    public void testBlackBox() {
        Random r = new Random();
        Set<Long> values = new HashSet<Long>();
        StreamingQuotientFilter f = new StreamingQuotientFilter(4, 5, 10);
        int uniqueDeclaredDuplicate = 0;
        int duplicateDeclaredUnique = 0;
        for (int i = 0; i < 1000000; ++i) {
            long value = r.nextLong() % 1000000L;
            boolean approx = f.add(value);
            boolean exact = values.add(value);
            if (exact && !approx) {
                // Actually duplicate, filter says unique
                duplicateDeclaredUnique++;
            }
            if (approx && !exact) {
                // Actually unique, filter says duplicate
                uniqueDeclaredDuplicate++;
            }
        }
        System.out.println("#uniques: " + values.size());
        System.out.println("Unique declared duplicate: " + uniqueDeclaredDuplicate);
        System.out.println("Duplicate declared unique: " + duplicateDeclaredUnique);
    }
}
