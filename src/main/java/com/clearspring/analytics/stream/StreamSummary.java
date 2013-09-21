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

import com.clearspring.analytics.util.ExternalizableUtil;
import com.clearspring.analytics.util.Pair;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.IntHeapPriorityQueue;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.io.ByteArrayInputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Based on the <i>Space-Saving</i> algorithm and the <i>Stream-Summary</i>
 * data structure as described in:
 * <i>Efficient Computation of Frequent and Top-k Elements in Data Streams</i>
 * by Metwally, Agrawal, and Abbadi
 *
 * General idea: store a table (item -> counter) of bounded size;
 * when an item arrives: if it's in the table, increment counter;
 * if it's not in the table, replace item with the smallest counter
 * (but keep the counter value! so that sum of counters is always
 * equal to number of offered items).
 *
 * The implementation differs from the one suggested in the article, because
 * the article's implementation in Java requires a lot of overhead for objects.
 * The current implementation relies mostly on primitive arrays.
 *
 * @param <T> type of data in the stream to be summarized
 */
public class StreamSummary<T> implements ITopK<T>, Externalizable
{
    private int[] maxHeap; // contents are indices into 'counts' or 'items'
    private int[] indexInMaxHeap;
    private int[] minHeap;
    private int[] indexInMinHeap;
    private long[] counts;
    private long[] errors;

    // TODO: For the case when T=Long, we could use a primitive array
    // and a primitive map here. However it makes the code messy :(
    private T[] items;
    private Map<T, Integer> indices;

    private boolean debugMode;

    /**
     * @param capacity maximum size (larger capacities improve accuracy)
     */
    public StreamSummary(int capacity)
    {
        this.maxHeap = new int[capacity];
        this.indexInMaxHeap = new int[capacity];
        this.minHeap = new int[capacity];
        this.indexInMinHeap = new int[capacity];
        this.items = (T[])new Object[capacity];
        this.indices = new Object2IntOpenHashMap<T>(capacity);
        this.counts = new long[capacity];
        this.errors = new long[capacity];
    }

    public int getCapacity()
    {
        return items.length;
    }

    public int size()
    {
        return indices.size();
    }

    /**
     * @return false if item was already in the stream summary, true otherwise.
     */
    @Override
    public boolean offer(T item)
    {
        return offer(item, 1);
    }

    /**
     * @return false if item was already in the stream summary, true otherwise
     */
    @Override
    public boolean offer(T item, long incrementCount)
    {
        return offerReturnAll(item, incrementCount).left;
    }

    /**
     * @return item dropped from summary if an item was dropped, null otherwise
     */
    public T offerReturnDropped(T item, long incrementCount)
    {
        return offerReturnAll(item, incrementCount).right;
    }

    /**
     * @return Pair(isNewItem, itemDropped) where isNewItem is the return value of offer()
     * and itemDropped is null if no item was dropped
     */
    public Pair<Boolean, T> offerReturnAll(T item, long incrementCount)
    {
        boolean isNew;
        T evicted = null;
        isNew = !indices.containsKey(item);
        int ti = isNew ? -1 : indices.get(item);
        if (ti == -1) {
            if (indices.size() == getCapacity()) {
                // Evict existing item
                ti = minHeap[0];
                evicted = items[ti];
                errors[ti] = counts[ti];
                indices.remove(evicted);
            } else {
                // Add new item
                ti = indices.size();
                minHeap[ti] = indexInMinHeap[ti] = ti;
                maxHeap[ti] = indexInMaxHeap[ti] = ti;
                siftUp(maxHeap, indexInMaxHeap, ti, true);
                siftUp(minHeap, indexInMinHeap, ti, false);
            }
            indices.put(item, ti);
            items[ti] = item;
        }
        counts[ti] += incrementCount;
        siftUp(maxHeap, indexInMaxHeap, ti, true);
        siftDown(minHeap, indexInMinHeap, ti, false);
        if (debugMode) {
            verifyInvariants();
        }
        return Pair.create(isNew, evicted);
    }

    private void siftUp(int[] heap, int[] indexInHeap, int x, boolean maxNotMin) {
        int hi = indexInHeap[x];
        while(true) {
            if (hi == 0) break;
            int parent = (hi-1)/2;
            long cur = counts[heap[hi]], atParent = counts[heap[parent]];
            boolean misplaced = (maxNotMin ? (cur > atParent) : (cur < atParent));
            if (!misplaced) break;
            swap(heap, indexInHeap, hi, parent);
            hi = parent;
        }
    }

    private void swap(int[] heap, int[] indexInHeap, int hi, int hj) {
        int tmp = heap[hi];
        heap[hi] = heap[hj];
        heap[hj] = tmp;
        indexInHeap[heap[hi]] = hi;
        indexInHeap[heap[hj]] = hj;
    }

    private void siftDown(int[] heap, int[] indexInHeap, int x, boolean maxNotMin) {
        int n = indices.size();
        int hi = indexInHeap[x];
        long negInfinity = maxNotMin ? Long.MIN_VALUE : Long.MAX_VALUE;
        while (2 * hi + 1 < n) {
            long current = counts[heap[hi]];
            int left = 2 * hi + 1, right = 2 * hi + 2;
            long atLeft = (left < n) ? counts[heap[left]] : negInfinity;
            long atRight = (right < n) ? counts[heap[right]] : negInfinity;
            if (current <= atLeft && current <= atRight) {
                break;
            }
            if (atLeft < atRight) {
                swap(heap, indexInHeap, hi, left);
                hi = left;
            } else {
                swap(heap, indexInHeap, hi, right);
                hi = right;
            }
        }
    }

    @Override
    public List<T> peek(int k)
    {
        List<T> res = new ArrayList<T>();
        for (int i : topKIndices(k)) {
            res.add(items[i]);
        }
        return res;
    }

    public List<Counter<T>> topK(int k) {
        List<Counter<T>> res = new ArrayList<Counter<T>>();
        for (int i : topKIndices(k)) {
            res.add(new Counter<T>(items[i], counts[i], errors[i]));
        }
        return res;
    }

    private int[] topKIndices(int k) {
        int n = indices.size();
        if (k > n) k = n;
        int[] res = new int[k];
        int count = 0;
        IntHeapPriorityQueue q = new IntHeapPriorityQueue(new AbstractIntComparator()
        {
            @Override
            public int compare(int i1, int i2)
            {
                long c1 = counts[maxHeap[i1]];
                long c2 = counts[maxHeap[i2]];
                if (c1 > c2) return -1;
                if (c1 < c2) return 1;
                return 0;
            }
        });
        q.enqueue(0);
        while (count < k) {
            int hi = q.dequeue();
            res[count++] = maxHeap[hi];
            if (2 * hi + 1 < n) {
                q.enqueue(2 * hi + 1);
            }
            if (2 * hi + 2 < n) {
                q.enqueue(2 * hi + 2);
            }
        }
        return res;
    }

    @Override
    public String toString()
    {
        List<Pair<T, Long>> pairs = new ArrayList<Pair<T, Long>>();
        for (T item : indices.keySet()) {
            pairs.add(Pair.create(item, counts[indices.get(item)]));
        }
        Collections.sort(pairs, new Comparator<Pair<T, Long>>()
        {
            @Override
            public int compare(Pair<T, Long> o1, Pair<T, Long> o2)
            {
                int res = Long.compare(o1.right, o2.right);
                if (res != 0) {
                    return res;
                }
                return o1.left.toString().compareTo(o2.right.toString());
            }
        });
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (Pair<T, Long> item : pairs) {
            if (sb.length() > 1) {
                sb.append(", ");
            }
            sb.append(item.left).append(": ").append(item.right);
        }
        sb.append("}");
        return sb.toString();
    }

    private void verifyInvariants() {
        int n = indices.size();
        for (int i = 0; i < n; ++i) {
            assert indices.get(items[i]) == i;
            assert minHeap[indexInMinHeap[i]] == i;
            assert maxHeap[indexInMaxHeap[i]] == i;
            int hi = indexInMinHeap[i];
            assert counts[minHeap[hi]] >= counts[minHeap[(hi-1)/2]];
            assert counts[maxHeap[hi]] <= counts[maxHeap[(hi-1)/2]];
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        int capacity = in.readInt();
        int size = in.readInt();
        this.maxHeap = new int[capacity];
        this.indexInMaxHeap = new int[capacity];
        this.minHeap = new int[capacity];
        this.indexInMinHeap = new int[capacity];
        this.items = (T[])new Object[capacity];
        this.indices = new Object2IntOpenHashMap<T>(capacity);
        this.counts = new long[capacity];
        for (int i = 0; i < size; ++i) {
            T item = (T)in.readObject();
            long count = in.readLong();
            offer(item, count);
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeInt(counts.length);
        out.writeInt(indices.size());
        for (int i = 0; i < indices.size(); ++i) {
            out.writeObject(items[i]);
            out.writeLong(counts[i]);
        }
    }

    /**
     * For de-serialization
     */
    public StreamSummary()
    {
    }

    /**
     * For de-serialization
     */
    public StreamSummary(byte[] bytes) throws IOException, ClassNotFoundException
    {
        fromBytes(bytes);
    }

    public void fromBytes(byte[] bytes) throws IOException, ClassNotFoundException
    {
        readExternal(new ObjectInputStream(new ByteArrayInputStream(bytes)));
    }

    public byte[] toBytes() throws IOException
    {
        return ExternalizableUtil.toBytes(this);
    }

    public void setDebugMode(boolean debugMode) {
        this.debugMode = debugMode;
    }
}
