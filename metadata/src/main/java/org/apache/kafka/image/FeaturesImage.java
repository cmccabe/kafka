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

import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;


/**
 * Represents the feature levels in the metadata image.
 *
 * This class is thread-safe.
 */
public final class FeaturesImage {
    public static final FeaturesImage EMPTY = new FeaturesImage(Collections.emptyMap(), MetadataVersion.MINIMUM_KRAFT_VERSION);

    private final Map<String, Short> finalizedVersions;

    private final MetadataVersion metadataVersion;

    public FeaturesImage(Map<String, Short> finalizedVersions, MetadataVersion metadataVersion) {
        this.finalizedVersions = Collections.unmodifiableMap(finalizedVersions);
        this.metadataVersion = metadataVersion;
    }

    public boolean isEmpty() {
        return finalizedVersions.isEmpty();
    }

    public MetadataVersion metadataVersion() {
        return metadataVersion;
    }

    public Map<String, Short> finalizedVersions() {
        return finalizedVersions;
    }

    private Optional<Short> finalizedVersion(String feature) {
        return Optional.ofNullable(finalizedVersions.get(feature));
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        if (options.metadataVersion().isLessThan(MetadataVersion.MINIMUM_BOOTSTRAP_VERSION)) {
            writePreProductionVersion(writer, options);
        } else {
            writeProductionVersion(writer, options);
        }
    }

    private void writePreProductionVersion(ImageWriter writer, ImageWriterOptions options) {
        // If the metadata version is older than 3.3-IV0, we can't represent any feature flags,
        // because the FeatureLevel record is not supported.
        if (!finalizedVersions.isEmpty()) {
            List<String> features = new ArrayList<>(finalizedVersions.keySet());
            features.sort(String::compareTo);
            options.handleLoss("feature flag(s): " +
                    features.stream().collect(Collectors.joining(", ")));
        }
        // With this old metadata version, we can't even represent the metadata version itself.
        // However, we do want to put something into the snapshot that indicates the version,
        // for human consumption. The following two records set a topic config on __cluster_metadata
        // and then clear it, in order to accomplish that. (ConfigRecord goes back to the earliest
        // stable KRaft releases.)
        //
        // Another reason we do this here is that it allows us to maintain the invariant that every
        // MetadataImage generates at least one record when serialized -- even if the image is
        // empty.
        writer.write(0, new ConfigRecord().
            setResourceType(ResourceType.TOPIC.code()).
            setResourceName("__cluster_metadata").
            setName("metadata.version").
            setValue(options.metadataVersion().toString()));
        writer.write(0, new ConfigRecord().
            setResourceType(ResourceType.TOPIC.code()).
            setResourceName("__cluster_metadata").
            setName("metadata.version").
            setValue(null));
    }

    private void writeProductionVersion(ImageWriter writer, ImageWriterOptions options) {
        // It is important to write out the metadata.version record first, because it may have an
        // impact on how we decode records that come after it.
        //
        // Note: it's important that this initial FeatureLevelRecord be written with version 0 and
        // not any later version, so that any modern reader can process it.
        writer.write(0, new FeatureLevelRecord().
                setName(MetadataVersion.FEATURE_NAME).
                setFeatureLevel(options.metadataVersion().featureLevel()));

        // Write out the metadata versions for other features.
        for (Entry<String, Short> entry : finalizedVersions.entrySet()) {
            if (!entry.getKey().equals(MetadataVersion.FEATURE_NAME)) {
                writer.write(0, new FeatureLevelRecord().
                        setName(entry.getKey()).
                        setFeatureLevel(entry.getValue()));
            }
        }
    }

    @Override
    public int hashCode() {
        return finalizedVersions.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof FeaturesImage)) return false;
        FeaturesImage other = (FeaturesImage) o;
        return finalizedVersions.equals(other.finalizedVersions);
    }


    @Override
    public String toString() {
        return "FeaturesImage{" +
                "finalizedVersions=" + finalizedVersions +
                ", metadataVersion=" + metadataVersion +
                '}';
    }
}
