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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for metadata loading without runtime initialization. */
public class LazyFileStoreTableTest extends TableTestBase {

    @ParameterizedTest
    @CsvSource({
        "false,-1,false,BUCKET_UNAWARE",
        "false,2,false,HASH_FIXED",
        "true,2,false,HASH_FIXED",
        "true,-1,false,HASH_DYNAMIC",
        "true,-1,true,KEY_DYNAMIC",
        "true,-2,false,POSTPONE_MODE"
    })
    public void testBucketMode(
            boolean primaryKey, int buckets, boolean crossPartition, BucketMode expected)
            throws Exception {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("pt", DataTypes.INT())
                        .option("bucket", String.valueOf(buckets));
        if (primaryKey) {
            builder.primaryKey("id");
        } else if (buckets > 0) {
            builder.option("bucket-key", "id");
        }
        if (crossPartition) {
            builder.partitionKeys("pt");
        }
        catalog.createTable(identifier(), builder.build(), false);
        FileStoreTable planning = (FileStoreTable) catalog.getTableForPlanning(identifier());
        FileStoreTable runtime = (FileStoreTable) catalog.getTable(identifier());
        assertThat(planning.bucketMode()).isEqualTo(expected);
        assertThat(runtime.bucketMode()).isEqualTo(expected);
    }

    @Test
    public void testCopyWithoutTimeTravelDefersValidation() throws Exception {
        catalog.createTable(
                identifier(), Schema.newBuilder().column("id", DataTypes.INT()).build(), false);
        FileStoreTable original = (FileStoreTable) catalog.getTable(identifier());
        FileSystemSchemaManager schemas =
                new FileSystemSchemaManager(original.fileIO(), original.location());
        Map<String, String> options = new HashMap<>(original.options());
        options.put(CoreOptions.RECORD_LEVEL_TIME_FIELD.key(), "missing");
        original.fileIO()
                .overwriteFileUtf8(
                        schemas.toSchemaPath(0), original.schema().copy(options).toString());
        catalog.invalidateTable(identifier());

        FileStoreTable planning = (FileStoreTable) catalog.getTableForPlanning(identifier());
        FileStoreTable copy =
                planning.copyWithoutTimeTravel(Collections.singletonMap("read.batch-size", "32"));
        assertThat(copy.options()).containsEntry("read.batch-size", "32");
        assertThatThrownBy(copy::newScan).hasMessageContaining("missing");
        FileStoreTable repaired =
                planning.copyWithoutTimeTravel(
                        Collections.singletonMap(CoreOptions.RECORD_LEVEL_TIME_FIELD.key(), null));
        assertThat(read(repaired)).isEmpty();
    }

    @Test
    public void testStoreInitializationFailureDoesNotBlockMetadata() throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .primaryKey("id")
                        .option("bucket", "1")
                        .option("merge-engine", "partial-update")
                        .build(),
                false);
        FileStoreTable original = (FileStoreTable) catalog.getTable(identifier());
        FileSystemSchemaManager schemas =
                new FileSystemSchemaManager(original.fileIO(), original.location());
        Map<String, String> options = new HashMap<>(original.options());
        options.put("fields.v.aggregate-function", "max");
        original.fileIO()
                .overwriteFileUtf8(
                        schemas.toSchemaPath(0), original.schema().copy(options).toString());
        catalog.invalidateTable(identifier());

        FileStoreTable planning = (FileStoreTable) catalog.getTableForPlanning(identifier());
        assertThat(planning.options()).containsEntry("fields.v.aggregate-function", "max");
        assertThat(planning.rowType().getFieldNames()).containsExactly("id", "v");
        assertThatThrownBy(planning::newScan).hasMessageContaining("Must use sequence group");
        assertThatThrownBy(() -> planning.newWrite("test"))
                .hasMessageContaining("Must use sequence group");
        catalog.alterTable(
                identifier(), SchemaChange.removeOption("fields.v.aggregate-function"), false);
        FileStoreTable repaired = (FileStoreTable) catalog.getTableForPlanning(identifier());
        write(repaired, GenericRow.of(1, 10));
        assertThat(read(repaired)).extracting(row -> row.getInt(1)).containsExactly(10);
    }

    @ParameterizedTest
    @ValueSource(strings = {"scan.fallback-branch", "scan.primary-branch"})
    public void testFallbackBranchLoading(String option) throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .partitionKeys("pt")
                        .build(),
                false);
        FileStoreTable main = (FileStoreTable) catalog.getTable(identifier());
        main.createBranch("backup");
        write(main, GenericRow.of(1, 10));
        write(main.switchToBranch("backup"), GenericRow.of(1, 20), GenericRow.of(2, 30));
        catalog.alterTable(identifier(), SchemaChange.setOption(option, "backup"), false);

        FileStoreTable planning = (FileStoreTable) catalog.getTableForPlanning(identifier());
        assertThat(((LazyFileStoreTable) planning).wrapped())
                .isInstanceOf(FallbackReadFileStoreTable.class);
        assertThat(read(planning))
                .extracting(row -> row.getInt(1))
                .containsExactlyInAnyOrder(option.equals("scan.fallback-branch") ? 10 : 20, 30);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testCapturedSchemaAndSerialization(boolean primaryKey) throws Exception {
        Schema.Builder builder = Schema.newBuilder().column("id", DataTypes.INT());
        if (primaryKey) {
            builder.primaryKey("id").option("bucket", "1");
        }
        catalog.createTable(identifier(), builder.build(), false);
        FileStoreTable planning = (FileStoreTable) catalog.getTableForPlanning(identifier());
        FileStoreTable runtime = (FileStoreTable) catalog.getTable(identifier());
        assertThat(planning.bucketMode()).isEqualTo(runtime.bucketMode());
        write(runtime, GenericRow.of(1));
        catalog.alterTable(identifier(), SchemaChange.addColumn("v", DataTypes.INT()), false);

        FileStoreTable restored =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(planning), getClass().getClassLoader());
        assertThat(restored.schema().id()).isEqualTo(0L);
        assertThat(restored.rowType().getFieldNames()).containsExactly("id");
        assertThat(read(restored)).extracting(row -> row.getInt(0)).containsExactly(1);
        assertThat(((LazyFileStoreTable) restored).wrapped().schema()).isEqualTo(planning.schema());
    }

    @Test
    public void testTimeTravelSchemaAndDynamicOptions() throws Exception {
        catalog.createTable(
                identifier(), Schema.newBuilder().column("id", DataTypes.INT()).build(), false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        write(table, GenericRow.of(1));
        table.createTag("before_evolution", 1);
        catalog.alterTable(identifier(), SchemaChange.addColumn("v", DataTypes.INT()), false);
        table = (FileStoreTable) catalog.getTable(identifier());
        write(table, GenericRow.of(2, 20));

        FileStoreTable planning = (FileStoreTable) catalog.getTableForPlanning(identifier());
        FileStoreTable historical =
                planning.copy(Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), "1"));
        historical =
                historical.copy(Collections.singletonMap(CoreOptions.READ_BATCH_SIZE.key(), "32"));
        assertThat(historical.rowType().getFieldNames()).containsExactly("id");
        assertThat(historical.options()).containsEntry(CoreOptions.READ_BATCH_SIZE.key(), "32");
        assertThat(read(historical)).extracting(row -> row.getInt(0)).containsExactly(1);
        assertThat(((LazyFileStoreTable) historical).wrapped().schema())
                .isEqualTo(historical.schema());

        FileStoreTable tagged =
                planning.copy(
                        Collections.singletonMap(
                                CoreOptions.SCAN_TAG_NAME.key(), "before_evolution"));
        assertThat(tagged.schema()).isEqualTo(historical.schema().copy(tagged.options()));
        assertThat(read(tagged)).extracting(row -> row.getInt(0)).containsExactly(1);
        FileStoreTable noTimeTravel =
                planning.copyWithoutTimeTravel(
                        Collections.singletonMap(
                                CoreOptions.SCAN_TAG_NAME.key(), "before_evolution"));
        assertThat(noTimeTravel.rowType().getFieldNames()).containsExactly("id", "v");
        assertThat(read(noTimeTravel)).extracting(row -> row.getInt(0)).containsExactly(1);

        assertThatThrownBy(() -> planning.copy(Collections.singletonMap("bucket", "2")))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(
                        () ->
                                catalog.getTable(identifier())
                                        .copy(
                                                Collections.singletonMap(
                                                        CoreOptions.RECORD_LEVEL_TIME_FIELD.key(),
                                                        "missing")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
