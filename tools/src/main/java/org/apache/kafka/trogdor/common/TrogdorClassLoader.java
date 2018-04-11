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
package org.apache.kafka.trogdor.common;

import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * A custom classloader that can be used to run tests with a custom classpath.
 * This class is thread-safe.
 */
public class TrogdorClassLoader extends URLClassLoader {
    private static final Logger log = LoggerFactory.getLogger(TrogdorClassLoader.class);

    private final static Map<List<String>, TrogdorClassLoader> CACHE = new HashMap<>();

    static {
        ClassLoader.registerAsParallelCapable();
    }

    private final List<String> paths;

    public static TrogdorClassLoader getOrCreate(final List<String> paths) {
        synchronized (CACHE) {
            TrogdorClassLoader classLoader = CACHE.get(paths);
            if (classLoader != null) {
                return classLoader;
            }
            classLoader = AccessController.<TrogdorClassLoader>doPrivileged(
                new PrivilegedAction() {
                    @Override
                    public TrogdorClassLoader run() {
                        return new TrogdorClassLoader(paths);
                    }
                }
            );
            CACHE.put(paths, classLoader);
            return classLoader;
        }
    }

    /**
     * Constructor that defines the system classloader as parent of this plugin classloader.
     *
     * @param paths the list of paths from which to load classes and resources for this plugin.
     */
    public TrogdorClassLoader(List<String> paths) {
        super(parseUrls(paths));
        this.paths = new ArrayList<>(paths);
    }

    private static URL[] parseUrls(List<String> paths) {
        URL[] urls = new URL[paths.size()];
        for (int i = 0; i < urls.length; i++) {
            try {
                urls[i] = Paths.get(paths.get(i)).toUri().toURL();
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
        }
        return urls;
    }

    @Override
    public String toString() {
        return "TrogdorClassLoader(" + Utils.join(paths, ":") + ")";
    }

    private static boolean useCustomClassPathForClass(String name) {
        return !(name.startsWith("java") ||
            name.startsWith("org.apache.kafka.trogdor") ||
            name.startsWith("org.omg") ||
            name.startsWith("org.slf4j"));
    }

    @Override
    protected synchronized Class<?> loadClass(String name, boolean resolve)
            throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            log.trace("NOW: loadClass(name=" + name + ")");
            Class<?> klass = findLoadedClass(name);
            if (klass == null) {
                try {
                    if (useCustomClassPathForClass(name)) {
                        klass = findClass(name);
                    }
                } catch (ClassNotFoundException e) {
                    // Not found in loader's path. Search in parents.
                    log.trace("Class '{}' not found. Delegating to parent", name);
                }
            }
            if (klass == null) {
                klass = super.loadClass(name, false);
            }
            if (resolve) {
                resolveClass(klass);
            }
            return klass;
        }
    }

}
