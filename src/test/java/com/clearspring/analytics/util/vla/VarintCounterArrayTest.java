package com.clearspring.analytics.util.vla;

import cern.jet.random.Distributions;
import cern.jet.random.engine.RandomEngine;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VarintCounterArrayTest
{
    @Test
    public void testBasic() {
        int size = 1000000;
        int iters = size;
        int[] actualCounts = new int[size];

        RandomEngine re = RandomEngine.makeDefault();
        VarintCounterArray a = new VarintCounterArray(size);
        for (int i = 0; i < iters; i++)
        {
            // Mixture of a wide and narrow distribution, to have both high values and high counts.
            int z = Distributions.nextGeometric((i % 2 == 0) ? 0.0001 : 0.25, re);
            if (z > size) z = size;
            actualCounts[z]++;
            a.addTo(z, 1);
            assertEquals(z + ": " + actualCounts[z], actualCounts[z], a.get(z));
        }
        int maxValue = 0;
        int maxCount = 0;
        for (int i = 0; i < size; ++i) {
            assertEquals("" + i, actualCounts[i], a.get(i));
            if (actualCounts[i] != 0) {
                maxValue = i;
            }
            maxCount = Math.max(actualCounts[i], maxCount);
        }
        System.out.println("Maximum value achieved: " + maxValue);
        System.out.println("Maximum count achieved: " + maxCount);
        System.out.println("Bytes used: " + a.bytesUsed());
    }
    @Test
    public void testPerformanceGeometric() {
        RandomEngine re = RandomEngine.makeDefault();

        int size = 1000000;
        int iters = size * 100;
        VarintCounterArray a = new VarintCounterArray(size);
        long t0 = System.currentTimeMillis();
        for (int i = 0; i < iters; i++)
        {
            Distributions.nextGeometric((i % 2 == 0) ? 0.0001 : 0.25, re);
        }
        long dt0 = System.currentTimeMillis() - t0;
        System.out.println("Calibration done in " + dt0);

        long t = System.currentTimeMillis();
        for (int i = 0; i < iters; i++)
        {
            int z = Distributions.nextGeometric((i % 2 == 0) ? 0.0001 : 0.25, re);
            a.addTo(z, 1);
        }
        long dt = System.currentTimeMillis() - t - dt0;
        // About 10mln items/s (about 13mln with a 0.25 distribution)
        System.out.println("Took " + dt + "ms: " + (1000L*iters/dt) + " items/s");
        System.out.println("Bytes used: " + a.bytesUsed());
    }
}
