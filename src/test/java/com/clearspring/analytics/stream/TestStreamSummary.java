/*
 * Copyright (C) 2011 Clearspring Technologies, Inc. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.clearspring.analytics.stream;

import cern.jet.random.Distributions;
import cern.jet.random.engine.MersenneTwister64;
import cern.jet.random.engine.RandomEngine;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class TestStreamSummary
{
    private static final int NUM_ITERATIONS = 100000;

    @Test
    public void testBasic()
    {
        StreamSummary<String> vs = new StreamSummary<String>(5);
        vs.setDebugMode(true);
        String[] stream = {"X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "C", "A", "A"};
        for (String i : stream)
        {
            vs.offer(i);
            System.out.println(i + " => " + vs);
        }
        System.out.println(vs);
        assertEquals("[X, A, C]", vs.peek(3).toString());
        assertEquals("[X, A, C]", vs.peek(3).toString());
    }

    @Test
    public void testBasicWithIncrement()
    {
        StreamSummary<String> vs = new StreamSummary<String>(5);
        vs.setDebugMode(true);
        String[] stream = {"X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "C", "A", "A"};
        for (String i : stream)
        {
            vs.offer(i, 10);
        }
        System.out.println(vs);
        assertEquals("[X, A, C]", vs.peek(3).toString());
        assertEquals("[X, A, C]", vs.peek(3).toString());
    }

    @Test
    public void testGeometricDistribution()
    {
        StreamSummary<Integer> vs = new StreamSummary<Integer>(10);
        vs.setDebugMode(true);
        RandomEngine re = new MersenneTwister64(923713931);

        for (int i = 0; i < NUM_ITERATIONS; i++)
        {
            int z = Distributions.nextGeometric(0.25, re);
            vs.offer(z);
        }

        List<Integer> top = vs.peek(5);
        System.out.println("Geometric:");
        for (Integer e : top)
        {
            System.out.println(e);
        }

        int tippyTop = top.get(0);
        assertEquals(0, tippyTop);
        System.out.println(vs);
    }


    @SuppressWarnings("unchecked")
    @Test
    public void testCounterSerialization() throws IOException, ClassNotFoundException
    {
        StreamSummary<String> vs = new StreamSummary<String>(3);
        String[] stream = {"X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "C", "A", "A"};
        for (String i : stream)
        {
            vs.offer(i);
        }
        List<Counter<String>> topK = vs.topK(3);
        for (Counter<String> c : topK)
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutput oo = new ObjectOutputStream(baos);
            oo.writeObject(c);
            oo.close();

            ObjectInput oi = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
            Counter<String> clone = (Counter<String>) oi.readObject();
            assertEquals(c.getCount(), clone.getCount());
            assertEquals(c.getError(), clone.getError());
            assertEquals(c.getItem(), clone.getItem());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSerialization() throws IOException, ClassNotFoundException
    {
        StreamSummary<String> vs = new StreamSummary<String>(5);
        vs.setDebugMode(true);
        String[] stream = {"X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "C", "A", "A"};
        for (String i : stream)
        {
            vs.offer(i);
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutput oo = new ObjectOutputStream(baos);
        oo.writeObject(vs);
        oo.close();

        ObjectInput oi = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
        StreamSummary<String> clone = (StreamSummary<String>) oi.readObject();

        assertEquals(vs.toString(), clone.toString());
    }


    @Test
    public void testByteSerialization() throws IOException, ClassNotFoundException
    {
        StreamSummary<String> vs = new StreamSummary<String>(5);
        vs.setDebugMode(true);
        String[] stream = {"X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "C", "A", "A"};
        for (String i : stream)
        {
            vs.offer(i);
        }

        testSerialization(vs);

        // Empty
        vs = new StreamSummary<String>(0);
        testSerialization(vs);
    }

    private void testSerialization(StreamSummary<?> vs) throws IOException, ClassNotFoundException
    {
        byte[] bytes = vs.toBytes();
        StreamSummary<String> clone = new StreamSummary<String>(bytes);

        assertEquals(vs.toString(), clone.toString());
    }

    @Test
    public void testPerformance() {
        for (int capacity : new int[] {10, 100, 1000, 10000, 100000, 1000000}) {
            int[] ks = {5, 10, 100, 1000, 10000, 100000};
            System.out.println("=== Capacity " + capacity);
            testPerformance(capacity, ks);
        }
    }

    private void testPerformance(int capacity, int[] ks) {
        int iterationChunk = 10000;
        long minDuration = 3000;
        StreamSummary<Integer> vs = new StreamSummary<Integer>(capacity);
        RandomEngine re = new MersenneTwister64(923713931);

        long t = System.currentTimeMillis();
        int numIterations = 0;
        while (System.currentTimeMillis() - t < minDuration) {
            for (int i = 0; i < iterationChunk; ++i) {
                int z = re.nextInt() % 1000000;
                vs.offer(z);
            }
            numIterations += iterationChunk;
        }
        long dt = System.currentTimeMillis() - t;
        System.out.println("Offer: " + (int)((1000.0 * numIterations)/dt) + " items/sec; unique " + vs.size());

        for (int k : ks) {
            if (k > capacity) {
                continue;
            }
            t = System.currentTimeMillis();
            numIterations = 0;
            while (System.currentTimeMillis() - t < minDuration) {
                for (int i = 0; i < iterationChunk; ++i) {
                    vs.peek(k).size();
                }
                numIterations += iterationChunk;
            }
            dt = System.currentTimeMillis() - t;
            System.out.println("Top " + k + ": " + (int)((1000.0 * numIterations)/dt) + " calls/sec");
        }
    }
}
