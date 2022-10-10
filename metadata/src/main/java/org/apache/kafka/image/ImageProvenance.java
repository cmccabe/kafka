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

package org.apache.kafka.image;

import java.util.Objects;


/**
 * The source of a metadata image.
 */
public final class ImageProvenance {
    public static final ImageProvenance EMPTY = new ImageProvenance("the empty image",
            0L,
            0,
            0L);

    private final String source;
    private final long highestOffset;
    private final int highestEpoch;
    private final long highestTimestamp;

    public ImageProvenance(
        String source,
        long highestOffset,
        int highestEpoch,
        long highestTimestamp
    ) {
        this.source = source;
        this.highestOffset = highestOffset;
        this.highestEpoch = highestEpoch;
        this.highestTimestamp = highestTimestamp;
    }

    public String source() {
        return source;
    }

    public long highestOffset() {
        return highestOffset;
    }

    public int highestEpoch() {
        return highestEpoch;
    }

    public long highestTimestamp() {
        return highestTimestamp;
    }

    public ImageProvenance copyWithSource(String newSource) {
        return new ImageProvenance(newSource,
                highestOffset,
                highestEpoch,
                highestTimestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !o.getClass().equals(this.getClass())) return false;
        ImageProvenance other = (ImageProvenance) o;
        return source.equals(other.source) &&
            highestOffset == other.highestOffset &&
            highestEpoch == other.highestEpoch &&
            highestTimestamp == other.highestTimestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            source,
            highestOffset,
            highestEpoch,
            highestTimestamp);
    }

    @Override
    public String toString() {
        return "ImageProvenance(" +
            "source=" + source +
            ", highestOffset=" + highestOffset +
            ", highestEpoch=" + highestEpoch +
            ", highestTimestamp=" + highestTimestamp +
            ")";
    }
}
