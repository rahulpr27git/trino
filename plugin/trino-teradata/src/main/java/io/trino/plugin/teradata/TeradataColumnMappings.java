/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.teradata;

import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeType.TIME;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.Timestamps.roundDiv;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public final class TeradataColumnMappings
{
    private static final int MAX_LOCAL_DATE_TIME_PRECISION = 9;

    private TeradataColumnMappings() {}

    /**
     * @deprecated This method leads to incorrect result when the date value is before 1582 Oct 14.
     * If driver supports {@link LocalDate}, use {@link #dateColumnMappingUsingLocalDate} instead.
     */
    @Deprecated
    public static ColumnMapping dateColumnMappingUsingSqlDate()
    {
        return ColumnMapping.longMapping(
                DATE,
                dateReadFunctionUsingSqlDate(),
                dateWriteFunctionUsingSqlDate());
    }

    /**
     * @deprecated If driver supports {@link LocalDate}, use {@link #dateReadFunctionUsingLocalDate} instead.
     */
    @Deprecated
    public static LongReadFunction dateReadFunctionUsingSqlDate()
    {
        return (resultSet, columnIndex) -> {
            /*
             * JDBC returns a date using a timestamp at midnight in the JVM timezone, or earliest time after that if there was no midnight.
             * This works correctly for all dates and zones except when the missing local times 'gap' is 24h. I.e. this fails when JVM time
             * zone is Pacific/Apia and date to be returned is 2011-12-30.
             *
             * `return resultSet.getObject(columnIndex, LocalDate.class).toEpochDay()` avoids these problems but
             * is currently known not to work with Redshift (old Postgres connector) and SQL Server.
             */
            long localMillis = resultSet.getDate(columnIndex).getTime();
            // Convert it to a ~midnight in UTC.
            long utcMillis = ISOChronology.getInstance().getZone().getMillisKeepLocal(DateTimeZone.UTC, localMillis);
            // convert to days
            return MILLISECONDS.toDays(utcMillis);
        };
    }

    /**
     * @deprecated If driver supports {@link LocalDate}, use {@link #dateWriteFunctionUsingLocalDate} instead.
     */
    @Deprecated
    public static LongWriteFunction dateWriteFunctionUsingSqlDate()
    {
        return LongWriteFunction.of(Types.DATE, (statement, index, value) -> {
            // convert to midnight in default time zone
            long millis = DAYS.toMillis(value);
            statement.setDate(index, new Date(DateTimeZone.UTC.getMillisKeepLocal(DateTimeZone.getDefault(), millis)));
        });
    }

    public static ColumnMapping dateColumnMappingUsingLocalDate()
    {
        return ColumnMapping.longMapping(
                DATE,
                dateReadFunctionUsingLocalDate(),
                dateWriteFunctionUsingLocalDate());
    }

    public static LongReadFunction dateReadFunctionUsingLocalDate()
    {
        return new LongReadFunction() {
            @Override
            public boolean isNull(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                resultSet.getObject(columnIndex);
                return resultSet.wasNull();
            }

            @Override
            public long readLong(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                final Date date = (Date) resultSet.getObject(columnIndex);
                LocalDate value = date.toLocalDate();
                // Some drivers (e.g. MemSQL's) return null LocalDate even though the value isn't null
                if (value == null) {
                    throw new TrinoException(JDBC_ERROR, "Driver returned null LocalDate for a non-null value");
                }

                return value.toEpochDay();
            }
        };
    }

    public static LongWriteFunction dateWriteFunctionUsingLocalDate()
    {
        return LongWriteFunction.of(Types.DATE, (statement, index, value) -> statement.setObject(index, LocalDate.ofEpochDay(value)));
    }

    /**
     * @deprecated This method uses {@link Time} and the class cannot represent time value when JVM zone had
     * forward offset change (a 'gap') at given time on 1970-01-01. If driver only supports {@link LocalTime}, use
     * {@link #timeColumnMapping} instead.
     */
    @Deprecated
    public static ColumnMapping timeColumnMappingUsingSqlTime()
    {
        return ColumnMapping.longMapping(
                TIME,
                (resultSet, columnIndex) -> {
                    Time time = resultSet.getTime(columnIndex);
                    return (toLocalTime(time).toNanoOfDay() * PICOSECONDS_PER_NANOSECOND) % PICOSECONDS_PER_DAY;
                },
                timeWriteFunctionUsingSqlTime());
    }

    private static LocalTime toLocalTime(Time sqlTime)
    {
        // Time.toLocalTime() does not preserve second fraction
        return sqlTime.toLocalTime()
                .withNano(toIntExact(MILLISECONDS.toNanos(floorMod(sqlTime.getTime(), 1000L))));
    }

    /**
     * @deprecated This method uses {@link Time} and the class cannot represent time value when JVM zone had
     * forward offset change (a 'gap') at given time on 1970-01-01. If driver only supports {@link LocalTime}, use
     * {@link #timeWriteFunction} instead.
     */
    @Deprecated
    public static LongWriteFunction timeWriteFunctionUsingSqlTime()
    {
        return LongWriteFunction.of(Types.TIME, (statement, index, value) -> statement.setTime(index, toSqlTime(fromTrinoTime(value))));
    }

    private static Time toSqlTime(LocalTime localTime)
    {
        // Time.valueOf does not preserve second fraction
        return new Time(Time.valueOf(localTime).getTime() + NANOSECONDS.toMillis(localTime.getNano()));
    }

    public static ColumnMapping timeColumnMapping(TimeType timeType)
    {
        return ColumnMapping.longMapping(
                timeType,
                timeReadFunction(timeType),
                timeWriteFunction(timeType.getPrecision()));
    }

    public static LongReadFunction timeReadFunction(TimeType timeType)
    {
        requireNonNull(timeType, "timeType is null");
        checkArgument(timeType.getPrecision() <= 9, "Unsupported type precision: %s", timeType);
        return (resultSet, columnIndex) -> {
            final Time time = (Time) resultSet.getObject(columnIndex);
            long nanosOfDay = time.toLocalTime().toNanoOfDay();
            verify(nanosOfDay < NANOSECONDS_PER_DAY, "Invalid value of nanosOfDay: %s", nanosOfDay);
            long picosOfDay = nanosOfDay * PICOSECONDS_PER_NANOSECOND;
            long rounded = round(picosOfDay, 12 - timeType.getPrecision());
            if (rounded == PICOSECONDS_PER_DAY) {
                rounded = 0;
            }
            return rounded;
        };
    }

    public static LongWriteFunction timeWriteFunction(int precision)
    {
        checkArgument(precision <= 9, "Unsupported precision: %s", precision);

        return LongWriteFunction.of(Types.TIME, (statement, index, picosOfDay) -> {
            picosOfDay = round(picosOfDay, 12 - precision);
            if (picosOfDay == PICOSECONDS_PER_DAY) {
                picosOfDay = 0;
            }
            statement.setObject(index, fromTrinoTime(picosOfDay));
        });
    }

    /**
     * @deprecated This method uses {@link Timestamp} and the class cannot represent date-time value when JVM zone had
     * forward offset change (a 'gap'). This includes regular DST changes (e.g. Europe/Warsaw) and one-time policy changes
     * (Asia/Kathmandu's shift by 15 minutes on January 1, 1986, 00:00:00). This mapping also disables pushdown by default
     * to ensure correctness because rounding happens within Trino and won't apply to remote system.
     * If driver supports {@link LocalDateTime}, use {@link #timestampColumnMapping} instead.
     */
    @Deprecated
    public static ColumnMapping timestampColumnMappingUsingSqlTimestampWithRounding(TimestampType timestampType)
    {
        // TODO support higher precision
        checkArgument(timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", timestampType.getPrecision());
        return ColumnMapping.longMapping(
                timestampType,
                (resultSet, columnIndex) -> {
                    LocalDateTime localDateTime = resultSet.getTimestamp(columnIndex).toLocalDateTime();
                    int roundedNanos = toIntExact(round(localDateTime.getNano(), 9 - timestampType.getPrecision()));
                    LocalDateTime rounded = localDateTime
                            .withNano(0)
                            .plusNanos(roundedNanos);
                    return toTrinoTimestamp(timestampType, rounded);
                },
                timestampWriteFunctionUsingSqlTimestamp(timestampType),
                // NOTE: pushdown is disabled because the values stored in remote system might not match the values as
                // read by Trino due to rounding. This can lead to incorrect results if operations are pushed down to
                // the remote system.
                DISABLE_PUSHDOWN);
    }

    public static ColumnMapping timestampColumnMapping(TimestampType timestampType)
    {
        if (timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION) {
            return ColumnMapping.longMapping(
                    timestampType,
                    timestampReadFunction(timestampType),
                    timestampWriteFunction(timestampType));
        }
        checkArgument(timestampType.getPrecision() <= MAX_LOCAL_DATE_TIME_PRECISION, "Precision is out of range: %s", timestampType.getPrecision());
        return ColumnMapping.objectMapping(
                timestampType,
                longTimestampReadFunction(timestampType),
                longTimestampWriteFunction(timestampType, timestampType.getPrecision()));
    }

    public static LongReadFunction timestampReadFunction(TimestampType timestampType)
    {
        checkArgument(timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", timestampType.getPrecision());
        return (resultSet, columnIndex) -> {
            final Timestamp timestamp = (Timestamp) resultSet.getObject(columnIndex);
            return toTrinoTimestamp(timestampType, timestamp.toLocalDateTime());
        };
    }

    private static ObjectReadFunction longTimestampReadFunction(TimestampType timestampType)
    {
        checkArgument(timestampType.getPrecision() > TimestampType.MAX_SHORT_PRECISION && timestampType.getPrecision() <= MAX_LOCAL_DATE_TIME_PRECISION,
                "Precision is out of range: %s", timestampType.getPrecision());
        return ObjectReadFunction.of(
                LongTimestamp.class,
                (resultSet, columnIndex) -> toLongTrinoTimestamp(timestampType, resultSet.getObject(columnIndex, LocalDateTime.class)));
    }

    /**
     * @deprecated This method uses {@link Timestamp} and the class cannot represent date-time value when JVM zone had
     * forward offset change (a 'gap'). This includes regular DST changes (e.g. Europe/Warsaw) and one-time policy changes
     * (Asia/Kathmandu's shift by 15 minutes on January 1, 1986, 00:00:00). If driver only supports {@link LocalDateTime}, use
     * {@link #timestampWriteFunction} instead.
     */
    @Deprecated
    public static LongWriteFunction timestampWriteFunctionUsingSqlTimestamp(TimestampType timestampType)
    {
        checkArgument(timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", timestampType.getPrecision());
        return LongWriteFunction.of(Types.TIMESTAMP, (statement, index, value) -> statement.setTimestamp(index, Timestamp.valueOf(fromTrinoTimestamp(value))));
    }

    public static LongWriteFunction timestampWriteFunction(TimestampType timestampType)
    {
        checkArgument(timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", timestampType.getPrecision());
        return LongWriteFunction.of(Types.TIMESTAMP, (statement, index, value) -> statement.setObject(index, fromTrinoTimestamp(value)));
    }

    public static ObjectWriteFunction longTimestampWriteFunction(TimestampType timestampType, int roundToPrecision)
    {
        checkArgument(timestampType.getPrecision() > TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", timestampType.getPrecision());
        checkArgument(
                6 <= roundToPrecision && roundToPrecision <= 9 && roundToPrecision <= timestampType.getPrecision(),
                "Invalid roundToPrecision for %s: %s",
                timestampType,
                roundToPrecision);

        return ObjectWriteFunction.of(
                LongTimestamp.class,
                (statement, index, value) -> statement.setObject(index, fromLongTrinoTimestamp(value, roundToPrecision)));
    }

    public static long toTrinoTimestamp(TimestampType timestampType, LocalDateTime localDateTime)
    {
        long precision = timestampType.getPrecision();
        checkArgument(precision <= TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", precision);
        long epochMicros = localDateTime.toEpochSecond(UTC) * MICROSECONDS_PER_SECOND
                + localDateTime.getNano() / NANOSECONDS_PER_MICROSECOND;
        verify(
                epochMicros == round(epochMicros, TimestampType.MAX_SHORT_PRECISION - timestampType.getPrecision()),
                "Invalid value of epochMicros for precision %s: %s",
                precision,
                epochMicros);
        return epochMicros;
    }

    public static LongTimestamp toLongTrinoTimestamp(TimestampType timestampType, LocalDateTime localDateTime)
    {
        long precision = timestampType.getPrecision();
        checkArgument(precision > TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", precision);
        long epochMicros = localDateTime.toEpochSecond(UTC) * MICROSECONDS_PER_SECOND
                + localDateTime.getNano() / NANOSECONDS_PER_MICROSECOND;
        int picosOfMicro = (localDateTime.getNano() % NANOSECONDS_PER_MICROSECOND) * PICOSECONDS_PER_NANOSECOND;
        verify(
                picosOfMicro == round(picosOfMicro, TimestampType.MAX_PRECISION - timestampType.getPrecision()),
                "Invalid value of picosOfMicro for precision %s: %s",
                precision,
                picosOfMicro);
        return new LongTimestamp(epochMicros, picosOfMicro);
    }

    public static LocalDateTime fromTrinoTimestamp(long epochMicros)
    {
        long epochSecond = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
        int nanoFraction = floorMod(epochMicros, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
        Instant instant = Instant.ofEpochSecond(epochSecond, nanoFraction);
        return LocalDateTime.ofInstant(instant, UTC);
    }

    public static LocalDateTime fromLongTrinoTimestamp(LongTimestamp timestamp, int precision)
    {
        // The code below assumes precision is not less than microseconds and not more than picoseconds.
        checkArgument(6 <= precision && precision <= 9, "Unsupported precision: %s", precision);
        long epochSeconds = floorDiv(timestamp.getEpochMicros(), MICROSECONDS_PER_SECOND);
        int microsOfSecond = floorMod(timestamp.getEpochMicros(), MICROSECONDS_PER_SECOND);
        long picosOfMicro = round(timestamp.getPicosOfMicro(), TimestampType.MAX_PRECISION - precision);
        int nanosOfSecond = (microsOfSecond * NANOSECONDS_PER_MICROSECOND) + toIntExact(picosOfMicro / PICOSECONDS_PER_NANOSECOND);

        Instant instant = Instant.ofEpochSecond(epochSeconds, nanosOfSecond);
        return LocalDateTime.ofInstant(instant, UTC);
    }

    public static LocalTime fromTrinoTime(long value)
    {
        // value can round up to NANOSECONDS_PER_DAY, so we need to do % to keep it in the desired range
        return LocalTime.ofNanoOfDay(roundDiv(value, PICOSECONDS_PER_NANOSECOND) % NANOSECONDS_PER_DAY);
    }
}
