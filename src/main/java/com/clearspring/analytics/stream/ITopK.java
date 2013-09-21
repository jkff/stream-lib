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

import java.util.List;

public interface ITopK<T>
{
    /**
     * offer a single element to the top.
     *
     * @param element - the element to add to the top
     * @return false if the element was already in the top
     */
    boolean offer(T element);

    /**
     * offer a single element to the top and increment the count
     * for that element by incrementCount.
     *
     * @param element        - the element to add to the top
     * @param incrementCount - the increment count for the given count
     * @return false if the element was already in the top
     */
    boolean offer(T element, long incrementCount);

    /**
     * @param k
     * @return top k elements offered (may be an approximation)
     */
    List<T> peek(int k);
}
