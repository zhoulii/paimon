/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.lookup;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.flink.FlinkConnectorOptions.LookupCacheMode;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.flink.utils.TableScanUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.OutOfRangeException;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import org.apache.paimon.shade.guava30.com.google.common.primitives.Ints;

import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_CACHE_MODE;
import static org.apache.paimon.flink.query.RemoteTableQuery.isRemoteServiceAvailable;
import static org.apache.paimon.lookup.RocksDBOptions.LOOKUP_CACHE_ROWS;
import static org.apache.paimon.lookup.RocksDBOptions.LOOKUP_CONTINUOUS_DISCOVERY_INTERVAL;
import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.predicate.PredicateBuilder.transformFieldMapping;

/** A lookup {@link TableFunction} for file store. */
public class FileStoreLookupFunction implements Serializable, Closeable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FileStoreLookupFunction.class);

    private final Table table;
    @Nullable private final DynamicPartitionLoader partitionLoader;
    private final List<String> projectFields;
    private final List<String> joinKeys;
    @Nullable private final Predicate predicate;

    private transient Duration refreshInterval;
    private transient File path;
    private transient LookupTable lookupTable;

    // timestamp when cache expires
    private transient long nextLoadTime;

    public FileStoreLookupFunction(
            Table table, int[] projection, int[] joinKeyIndex, @Nullable Predicate predicate) {
        TableScanUtils.streamingReadingValidate(table);

        this.table = table;
        this.partitionLoader = DynamicPartitionLoader.of(table);

        // join keys are based on projection fields
        this.joinKeys =
                Arrays.stream(joinKeyIndex)
                        .mapToObj(i -> table.rowType().getFieldNames().get(projection[i]))
                        .collect(Collectors.toList());

        if (partitionLoader != null) {
            partitionLoader.addJoinKeys(joinKeys);
        }

        this.projectFields =
                Arrays.stream(projection)
                        .mapToObj(i -> table.rowType().getFieldNames().get(i))
                        .collect(Collectors.toList());

        // add primary keys
        for (String field : table.primaryKeys()) {
            if (!projectFields.contains(field)) {
                projectFields.add(field);
            }
        }

        this.predicate = predicate;
    }

    public void open(FunctionContext context) throws Exception {
        String tmpDirectory = getTmpDirectory(context);
        open(tmpDirectory);
    }

    // we tag this method friendly for testing
    void open(String tmpDirectory) throws Exception {
        this.path = new File(tmpDirectory, "lookup-" + UUID.randomUUID());
        if (!path.mkdirs()) {
            throw new RuntimeException("Failed to create dir: " + path);
        }
        open();
    }

    private void open() throws Exception {
        if (partitionLoader != null) {
            partitionLoader.open();
        }

        this.nextLoadTime = -1;

        Options options = Options.fromMap(table.options());
        this.refreshInterval =
                options.getOptional(LOOKUP_CONTINUOUS_DISCOVERY_INTERVAL)
                        .orElse(options.get(CONTINUOUS_DISCOVERY_INTERVAL));

        List<String> fieldNames = table.rowType().getFieldNames();
        int[] projection = projectFields.stream().mapToInt(fieldNames::indexOf).toArray();
        FileStoreTable storeTable = (FileStoreTable) table;

        if (options.get(LOOKUP_CACHE_MODE) == LookupCacheMode.AUTO
                && new HashSet<>(table.primaryKeys()).equals(new HashSet<>(joinKeys))) {
            // 如果 primary keys 和 join keys 相同，并且是自动模式，则优先尝试使用 PrimaryKeyPartialLookupTable
            if (isRemoteServiceAvailable(storeTable)) {
                // 使用远程查询服务
                this.lookupTable =
                        PrimaryKeyPartialLookupTable.createRemoteTable(
                                storeTable, projection, joinKeys);
            } else {
                // 使用本地查询
                try {
                    this.lookupTable =
                            PrimaryKeyPartialLookupTable.createLocalTable(
                                    storeTable, projection, path, joinKeys);
                } catch (UnsupportedOperationException ignore2) {
                }
            }
        }

        if (lookupTable == null) {
            // 不能直接查 LSM ，则使用 FullCacheLookupTable
            FullCacheLookupTable.Context context =
                    new FullCacheLookupTable.Context(
                            storeTable,
                            projection,
                            predicate,
                            createProjectedPredicate(projection),
                            path,
                            joinKeys);
            this.lookupTable = FullCacheLookupTable.create(context, options.get(LOOKUP_CACHE_ROWS));
        }

        refreshDynamicPartition(false);
        lookupTable.open();
    }

    @Nullable
    private Predicate createProjectedPredicate(int[] projection) {
        Predicate adjustedPredicate = null;
        if (predicate != null) {
            // adjust to projection index
            adjustedPredicate =
                    transformFieldMapping(
                                    this.predicate,
                                    IntStream.range(0, table.rowType().getFieldCount())
                                            .map(i -> Ints.indexOf(projection, i))
                                            .toArray())
                            .orElse(null);
        }
        return adjustedPredicate;
    }

    public Collection<RowData> lookup(RowData keyRow) {
        try {
            checkRefresh();

            InternalRow key = new FlinkRowWrapper(keyRow);
            if (partitionLoader != null) {
                InternalRow partition = refreshDynamicPartition(true);
                if (partition == null) {
                    return Collections.emptyList();
                }
                key = JoinedRow.join(key, partition);
            }

            List<InternalRow> results = lookupTable.get(key);
            List<RowData> rows = new ArrayList<>(results.size());
            for (InternalRow matchedRow : results) {
                rows.add(new FlinkRowData(matchedRow));
            }
            return rows;
        } catch (OutOfRangeException e) {
            reopen();
            return lookup(keyRow);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Nullable
    private BinaryRow refreshDynamicPartition(boolean reopen) throws Exception {
        if (partitionLoader == null) {
            return null;
        }

        boolean partitionChanged = partitionLoader.checkRefresh();
        BinaryRow partition = partitionLoader.partition();
        if (partition == null) {
            return null;
        }

        lookupTable.specificPartitionFilter(createSpecificPartFilter(partition));

        if (partitionChanged && reopen) {
            lookupTable.close();
            lookupTable.open();
        }

        return partition;
    }

    private Predicate createSpecificPartFilter(BinaryRow partition) {
        RowType rowType = table.rowType();
        List<String> partitionKeys = table.partitionKeys();
        Object[] partitionSpec =
                new RowDataToObjectArrayConverter(rowType.project(partitionKeys))
                        .convert(partition);
        Map<String, Object> partitionMap = new HashMap<>(partitionSpec.length);
        for (int i = 0; i < partitionSpec.length; i++) {
            partitionMap.put(partitionKeys.get(i), partitionSpec[i]);
        }

        // create partition predicate base on rowType instead of partitionType
        return createPartitionPredicate(rowType, partitionMap);
    }

    private void reopen() {
        try {
            close();
            open();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void checkRefresh() throws Exception {
        if (nextLoadTime > System.currentTimeMillis()) {
            return;
        }
        if (nextLoadTime > 0) {
            LOG.info(
                    "Lookup table {} has refreshed after {} second(s), refreshing",
                    table.name(),
                    refreshInterval.toMillis() / 1000);
        }

        refresh();

        nextLoadTime = System.currentTimeMillis() + refreshInterval.toMillis();
    }

    @VisibleForTesting
    LookupTable lookupTable() {
        return lookupTable;
    }

    private void refresh() throws Exception {
        lookupTable.refresh();
    }

    @Override
    public void close() throws IOException {
        if (lookupTable != null) {
            lookupTable.close();
            lookupTable = null;
        }

        if (path != null) {
            FileIOUtils.deleteDirectoryQuietly(path);
        }
    }

    private static String getTmpDirectory(FunctionContext context) {
        try {
            Field field = context.getClass().getDeclaredField("context");
            field.setAccessible(true);
            StreamingRuntimeContext runtimeContext =
                    extractStreamingRuntimeContext(field.get(context));
            String[] tmpDirectories =
                    runtimeContext.getTaskManagerRuntimeInfo().getTmpDirectories();
            return tmpDirectories[ThreadLocalRandom.current().nextInt(tmpDirectories.length)];
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static StreamingRuntimeContext extractStreamingRuntimeContext(Object runtimeContext)
            throws NoSuchFieldException, IllegalAccessException {
        if (runtimeContext instanceof StreamingRuntimeContext) {
            return (StreamingRuntimeContext) runtimeContext;
        }

        Field field = runtimeContext.getClass().getDeclaredField("runtimeContext");
        field.setAccessible(true);
        return extractStreamingRuntimeContext(field.get(runtimeContext));
    }
}
