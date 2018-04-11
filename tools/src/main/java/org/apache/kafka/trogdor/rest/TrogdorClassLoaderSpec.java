/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.trogdor.rest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.trogdor.common.TrogdorClassLoader;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

public class TrogdorClassLoaderSpec extends Message {
    private final List<String> paths;

    @JsonCreator
    public TrogdorClassLoaderSpec(@JsonProperty("paths") List<String> paths) {
        this.paths = paths == null ? Collections.<String>emptyList() :
            Collections.unmodifiableList(paths);
    }

    @JsonProperty
    public List<String> paths() {
        return paths;
    }

    public boolean hasPaths() {
        return !paths.isEmpty();
    }

    public ClassLoader classLoader() {
        if (paths.isEmpty()) {
            return Thread.currentThread().getContextClassLoader();
        } else {
            return TrogdorClassLoader.getOrCreate(paths);
        }
    }

    public <T> T doWithClassLoader(final Callable<T> callable) throws Exception {
        if (paths.isEmpty()) {
            return callable.call();
        }
        final ClassLoader original = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(classLoader());
            return callable.call();
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }
}
