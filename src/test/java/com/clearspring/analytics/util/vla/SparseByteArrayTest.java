package com.clearspring.analytics.util.vla;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

public class SparseByteArrayTest
{
    @Test
    public void testBasic() {
        SparseByteArray a = new SparseByteArray(100000, 256, 4);
        for (int i = 0; i < 1000; ++i) {
            a.set(i, (byte)(i / 3 + 1));
            assertEquals("" + i, (byte)(i / 3 + 1), a.get(i));
        }
        for (int i = 0; i < 1000; i += 100) {
            a.set(i, (byte) 0);
            assertEquals("" + i, (byte)0, a.get(i));
        }
        for (int i = 0; i < 1000; ++i) {
            if (i % 100 != 0) {
                assertEquals("" + i, (byte)(i / 3 + 1), a.get(i));
            }
        }
        for (int i = 0; i < 1000; i++) {
            if (i % 10 != 0) {
                a.set(i, (byte) 0);
                assertEquals("" + i, (byte) 0, a.get(i));
            } else if (i % 100 != 0) {
                assertEquals("" + i, (byte)(i / 3 + 1), a.get(i));
            }
        }
    }

    @Test
    public void testPerformanceScatteredSets() {
        int n = 10000000;
        SparseByteArray a = new SparseByteArray(n, 1024, 4);
        Random r = new Random();
        int[] indices = new int[n];
        byte[] contents = new byte[n];
        for (int i = 0; i < n; ++i) {
            indices[i] = r.nextInt(n);
            contents[i] = (byte)r.nextInt(256);
        }
        long t = System.currentTimeMillis();
        for (int i = 0; i < n; ++i) {
            a.set(indices[i], contents[i]);
            if (i % (n / 10) == 0) {
                int bytes = a.bytesUsed();
                double compressionFactor = n / bytes;
                System.out.println("---- " + i + ": bytes used: " + bytes +
                        " (" + (1.0 * bytes / (i + 1)) + " bytes per item), " +
                        "compression " + compressionFactor);
                a.printBytesUsed();
            }
        }
        long dt = System.currentTimeMillis() - t;
        System.out.println("Time used: " + dt + "ms: " + (n * 1000L/dt) + " sets/s");
    }

    @Test
    public void testPerformanceAsCounters() {
        int n = 10000000;
        Random r = new Random();
        // 1/5 unique items
        int[] indices = new int[n/5];
        for (int i = 0; i < indices.length; ++i) {
            indices[i] = r.nextInt(n);
        }
        SparseByteArray a = new SparseByteArray(n, 1024, 4);
        while(true) {
            long t = System.currentTimeMillis();
            SparseByteArray.Pointer p = new SparseByteArray.Pointer();
            for (int i = 0; i < n; ++i) {
                int j = i % indices.length;
                a.locate(j, p, false);
                a.set(p, (byte)(p.value+1));
//                if (i % (n / 10) == 0) {
//                    int bytes = a.bytesUsed();
//                    double compressionFactor = n / bytes;
//                    System.out.println("---- " + i + ": bytes used: " + bytes +
//                            ", compression " + compressionFactor);
//                    a.printBytesUsed();
//                }
            }
            long dt = System.currentTimeMillis() - t;
            if (dt > 1000) break;
//            System.out.println("Time used: " + dt + "ms: " + (n * 1000L/dt) + " sets/s");
//            break;
        }
    }
}
