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

package org.apache.paimon.data;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.Preconditions;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * Paimon 内部数据类型，表示 TimestampType 和 LocalZonedTimestampType.
 *
 * <p>An internal data structure representing data of {@link TimestampType} and {@link
 * LocalZonedTimestampType}.
 *
 * <p>This data structure is immutable and consists of a milliseconds and nanos-of-millisecond since
 * {@code 1970-01-01 00:00:00}. It might be stored in a compact representation (as a long value) if
 * values are small enough.
 *
 * @since 0.4.0
 */
@Public
public final class Timestamp implements Comparable<Timestamp>, Serializable {

    private static final long serialVersionUID = 1L;

    // the number of milliseconds in a day
    private static final long MILLIS_PER_DAY = 86400000; // = 24 * 60 * 60 * 1000

    // 1 毫秒等于 1000 微秒
    public static final long MICROS_PER_MILLIS = 1000L;

    // 1 微秒等于 1000 纳秒，1 毫秒等于 1000000 纳秒
    public static final long NANOS_PER_MICROS = 1000L;

    // this field holds the integral second and the milli-of-second
    private final long millisecond;

    // this field holds the nano-of-millisecond
    private final int nanoOfMillisecond;

    private Timestamp(long millisecond, int nanoOfMillisecond) {
        Preconditions.checkArgument(nanoOfMillisecond >= 0 && nanoOfMillisecond <= 999_999);
        this.millisecond = millisecond;
        this.nanoOfMillisecond = nanoOfMillisecond;
    }

    /** Returns the number of milliseconds since {@code 1970-01-01 00:00:00}. */
    public long getMillisecond() {
        return millisecond;
    }

    /**
     * Returns the number of nanoseconds (the nanoseconds within the milliseconds).
     *
     * <p>The value range is from 0 to 999,999.
     */
    public int getNanoOfMillisecond() {
        return nanoOfMillisecond;
    }

    /**
     * 转换为 {@link java.sql.Timestamp}.
     *
     * <p>Converts this {@link Timestamp} object to a {@link java.sql.Timestamp}.
     */
    public java.sql.Timestamp toSQLTimestamp() {
        return java.sql.Timestamp.valueOf(toLocalDateTime());
    }

    public Timestamp toMillisTimestamp() {
        // 转换为毫秒表示的时间戳.
        return fromEpochMillis(millisecond);
    }

    /**
     * 转换为 LocalDateTime.
     *
     * <p>Converts this {@link Timestamp} object to a {@link LocalDateTime}.
     */
    public LocalDateTime toLocalDateTime() {
        int date = (int) (millisecond / MILLIS_PER_DAY);
        int time = (int) (millisecond % MILLIS_PER_DAY);
        if (time < 0) { // 为什么可能为负数？epoch 之前的时间
            --date;
            time += MILLIS_PER_DAY;
        }
        long nanoOfDay = time * 1_000_000L + nanoOfMillisecond;
        LocalDate localDate = LocalDate.ofEpochDay(date);
        LocalTime localTime = LocalTime.ofNanoOfDay(nanoOfDay);
        return LocalDateTime.of(localDate, localTime);
    }

    /**
     * 转换成 Instant 对象.
     *
     * <p>Converts this {@link Timestamp} object to a {@link Instant}.
     */
    public Instant toInstant() {
        long epochSecond = millisecond / 1000;
        int milliOfSecond = (int) (millisecond % 1000);
        if (milliOfSecond < 0) {
            --epochSecond;
            milliOfSecond += 1000;
        }
        long nanoAdjustment = milliOfSecond * 1_000_000 + nanoOfMillisecond;
        return Instant.ofEpochSecond(epochSecond, nanoAdjustment);
    }

    /**
     * 转换为微秒.
     *
     * <p>Converts this {@link Timestamp} object to micros.
     */
    public long toMicros() {
        long micros = Math.multiplyExact(millisecond, MICROS_PER_MILLIS);
        return micros + nanoOfMillisecond / NANOS_PER_MICROS;
    }

    @Override
    public int compareTo(Timestamp that) {
        int cmp = Long.compare(this.millisecond, that.millisecond);
        if (cmp == 0) {
            cmp = this.nanoOfMillisecond - that.nanoOfMillisecond;
        }
        return cmp;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Timestamp)) {
            return false;
        }
        Timestamp that = (Timestamp) obj;
        return this.millisecond == that.millisecond
                && this.nanoOfMillisecond == that.nanoOfMillisecond;
    }

    @Override
    public String toString() {
        return toLocalDateTime().toString();
    }

    @Override
    public int hashCode() {
        int ret = (int) millisecond ^ (int) (millisecond >> 32);
        return 31 * ret + nanoOfMillisecond;
    }

    // ------------------------------------------------------------------------------------------
    // Constructor Utilities
    // ------------------------------------------------------------------------------------------

    /**
     * 从当前时间创建.
     *
     * <p>Creates an instance of {@link Timestamp} for now.
     */
    public static Timestamp now() {
        return fromLocalDateTime(LocalDateTime.now());
    }

    /**
     * 从毫秒值创建.
     *
     * <p>Creates an instance of {@link Timestamp} from milliseconds.
     *
     * <p>The nanos-of-millisecond field will be set to zero.
     *
     * @param milliseconds the number of milliseconds since {@code 1970-01-01 00:00:00}; a negative
     *     number is the number of milliseconds before {@code 1970-01-01 00:00:00}
     */
    public static Timestamp fromEpochMillis(long milliseconds) {
        return new Timestamp(milliseconds, 0);
    }

    /**
     * 从毫秒值、纳秒值创建.
     *
     * <p>Creates an instance of {@link Timestamp} from milliseconds and a nanos-of-millisecond.
     *
     * @param milliseconds the number of milliseconds since {@code 1970-01-01 00:00:00}; a negative
     *     number is the number of milliseconds before {@code 1970-01-01 00:00:00}
     * @param nanosOfMillisecond the nanoseconds within the millisecond, from 0 to 999,999
     */
    public static Timestamp fromEpochMillis(long milliseconds, int nanosOfMillisecond) {
        return new Timestamp(milliseconds, nanosOfMillisecond);
    }

    /**
     * 从 LocalDateTime 创建.
     *
     * <p>Creates an instance of {@link Timestamp} from an instance of {@link LocalDateTime}.
     *
     * @param dateTime an instance of {@link LocalDateTime}
     */
    public static Timestamp fromLocalDateTime(LocalDateTime dateTime) {
        long epochDay = dateTime.toLocalDate().toEpochDay();
        long nanoOfDay = dateTime.toLocalTime().toNanoOfDay();

        long millisecond = epochDay * MILLIS_PER_DAY + nanoOfDay / 1_000_000;
        int nanoOfMillisecond = (int) (nanoOfDay % 1_000_000);

        return new Timestamp(millisecond, nanoOfMillisecond);
    }

    /**
     * 从 {@link java.sql.Timestamp} 创建.
     *
     * <p>Creates an instance of {@link Timestamp} from an instance of {@link java.sql.Timestamp}.
     *
     * @param timestamp an instance of {@link java.sql.Timestamp}
     */
    public static Timestamp fromSQLTimestamp(java.sql.Timestamp timestamp) {
        return fromLocalDateTime(timestamp.toLocalDateTime());
    }

    /**
     * 从 {@link Instant} 创建.
     *
     * <p>Creates an instance of {@link Timestamp} from an instance of {@link Instant}.
     *
     * @param instant an instance of {@link Instant}
     */
    public static Timestamp fromInstant(Instant instant) {
        long epochSecond = instant.getEpochSecond();
        int nanoSecond = instant.getNano();

        long millisecond = epochSecond * 1_000 + nanoSecond / 1_000_000;
        int nanoOfMillisecond = nanoSecond % 1_000_000;

        return new Timestamp(millisecond, nanoOfMillisecond);
    }

    /**
     * 从微秒创建.
     *
     * <p>Creates an instance of {@link Timestamp} from micros.
     */
    public static Timestamp fromMicros(long micros) {
        long mills = Math.floorDiv(micros, MICROS_PER_MILLIS);
        long nanos = (micros - mills * MICROS_PER_MILLIS) * NANOS_PER_MICROS;
        return Timestamp.fromEpochMillis(mills, (int) nanos);
    }

    /**
     * 判断是否可以存储在毫秒的长整型中.
     *
     * <p>精度为 3 表示毫秒级的时间戳，如果 precision 小于等于 3，返回 true，表示可以存储在毫秒的长整型中. 如果 precision 大于 3，返回
     * false，表示时间戳精度太大，不能用毫秒的长整型进行存储.
     *
     * <p>Returns whether the timestamp data is small enough to be stored in a long of milliseconds.
     */
    public static boolean isCompact(int precision) {
        return precision <= 3;
    }
}
