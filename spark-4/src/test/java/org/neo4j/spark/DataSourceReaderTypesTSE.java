/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
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
package org.neo4j.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.*;
import java.sql.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;

public class DataSourceReaderTypesTSE extends SparkConnectorScalaBaseTSE {

    @Test
    public void testReadNodeWithString() {
        String name = "Foobar";
        Dataset<Row> df = initTest("CREATE (p:Person {name: '" + name + "'})");

        assertEquals(name, df.select("name").collectAsList().get(0).getString(0));
    }

    @Test
    public void testReadNodeWithLong() {
        long age = 42L;
        Dataset<Row> df = initTest("CREATE (p:Person {age: " + age + "})");

        assertEquals(age, df.select("age").collectAsList().get(0).getLong(0));
    }

    @Test
    public void testReadNodeWithDouble() {
        double score = 4.2;
        Dataset<Row> df = initTest("CREATE (p:Person {score: " + score + "})");

        assertEquals(score, df.select("score").collectAsList().get(0).getDouble(0), 0);
    }

    @Test
    public void testReadNodeWithLocalTime() {
        Dataset<Row> df = initTest("CREATE (p:Person {aTime: localtime({hour:12, minute: 23, second: 0, millisecond: 294})})");

        GenericRowWithSchema result = df.select("aTime").collectAsList().get(0).getAs(0);

        assertEquals("local-time", result.get(0));
        assertEquals("12:23:00.294", result.get(1));
    }

    @Test
    public void testReadNodeWithTime() {
        TimeZone timezone = TimeZone.getDefault();
        Dataset<Row> df = initTest("CREATE (p:Person {aTime: time({hour:12, minute: 23, second: 0, millisecond: 294, timezone: '"+timezone.getID()+"'})})");

        GenericRowWithSchema result = df.select("aTime").collectAsList().get(0).getAs(0);

        LocalTime localTime = LocalTime.of(12, 23, 0, 294000000);
        OffsetTime expectedTime = OffsetTime.of(localTime, timezone.toZoneId().getRules().getOffset(Instant.now()));

        assertEquals("offset-time", result.get(0));
        assertEquals(expectedTime.toString(), result.get(1));
    }

    @Test
    public void testReadNodeWithLocalDateTime() {
        String localDateTime = "2007-12-03T10:15:30";
        Dataset<Row> df = initTest("CREATE (p:Person {aTime: localdatetime('"+localDateTime+"')})");

        Timestamp result = df.select("aTime").collectAsList().get(0).getTimestamp(0);
        assertEquals(Timestamp.from(LocalDateTime.parse(localDateTime).toInstant(ZoneOffset.UTC)), result);
    }

    @Test
    public void testReadNodeWithZonedDateTime() {
        String dateTime = "2015-06-24T12:50:35.556+01:00";
        Dataset<Row> df = initTest("CREATE (p:Person {aTime: datetime('"+dateTime+"')})");

        Timestamp result = df.select("aTime").collectAsList().get(0).getTimestamp(0);
        assertEquals(Timestamp.from(OffsetDateTime.parse(dateTime).toInstant()), result);
    }

    @Test
    public void testReadNodeWithPoint() {
        Dataset<Row> df = initTest("CREATE (p:Person {location: point({x: 12.12, y: 13.13})})");

        GenericRowWithSchema res = df.select("location").collectAsList().get(0).getAs(0);
        assertEquals("point-2d", res.get(0));
        assertEquals(7203, res.get(1));
        assertEquals(12.12, res.get(2));
        assertEquals(13.13, res.get(3));
    }

    @Test
    public void testReadNodeWithGeoPoint() {
        Dataset<Row> df = initTest("CREATE (p:Person {location: point({longitude: 12.12, latitude: 13.13})})");

        GenericRowWithSchema res = df.select("location").collectAsList().get(0).getAs(0);
        assertEquals("point-2d", res.get(0));
        assertEquals(4326, res.get(1));
        assertEquals(12.12, res.get(2));
        assertEquals(13.13, res.get(3));
    }

    @Test
    public void testReadNodeWithPoint3D() {
        Dataset<Row> df = initTest("CREATE (p:Person {location: point({x: 12.12, y: 13.13, z: 1})})");

        GenericRowWithSchema res = df.select("location").collectAsList().get(0).getAs(0);
        assertEquals("point-3d", res.get(0));
        assertEquals(9157, res.get(1));
        assertEquals(12.12, res.get(2));
        assertEquals(13.13, res.get(3));
        assertEquals(1.0, res.get(4));
    }

    @Test
    public void testReadNodeWithDate() {
        Dataset<Row> df = initTest("CREATE (p:Person {born: date('2009-10-10')})");

        Date res = df.select("born").collectAsList().get(0).getDate(0);
        assertEquals(Date.valueOf("2009-10-10"), res);
    }

    @Test
    public void testReadNodeWithDuration() {
        Dataset<Row> df = initTest("CREATE (p:Person {range: duration({days: 14, hours:16, minutes: 12})})");

        GenericRowWithSchema res = df.select("range").collectAsList().get(0).getAs(0);
        assertEquals("duration", res.get(0));
        assertEquals(0L, res.get(1));
        assertEquals(14L, res.get(2));
        assertEquals(58320L, res.get(3));
        assertEquals(0, res.get(4));
        assertEquals("P0M14DT58320S", res.get(5));
    }

    @Test
    public void testReadNodeWithStringArray() {
        Dataset<Row> df = initTest("CREATE (p:Person {names: ['John', 'Doe']})");

        List<String> res = df.select("names").collectAsList().get(0).getList(0);
        assertEquals("John", res.get(0));
        assertEquals("Doe", res.get(1));
    }

    @Test
    public void testReadNodeWithLongArray() {
        Dataset<Row> df = initTest("CREATE (p:Person {ages: [22, 23]})");

        List<Long> res = df.select("ages").collectAsList().get(0).getList(0);
        assertEquals(22L, res.get(0).longValue());
        assertEquals(23L, res.get(1).longValue());
    }

    @Test
    public void testReadNodeWithDoubleArray() {
        Dataset<Row> df = initTest("CREATE (p:Person {scores: [22.33, 44.55]})");

        List<Double> res = df.select("scores").collectAsList().get(0).getList(0);
        assertEquals(22.33, res.get(0), 0);
        assertEquals(44.55, res.get(1), 0);
    }

    @Test
    public void testReadNodeWithLocalTimeArray() {
        Dataset<Row> df = initTest("CREATE (p:Person {someTimes: [localtime({hour:12}), localtime({hour:1, minute: 3})]})");

        List<GenericRowWithSchema> res = df.select("someTimes").collectAsList().get(0).getList(0);
        assertEquals("local-time", res.get(0).get(0));
        assertEquals("12:00:00", res.get(0).get(1));
        assertEquals("local-time", res.get(1).get(0));
        assertEquals("01:03:00", res.get(1).get(1));
    }

    @Test
    public void testReadNodeWithBooleanArray() {
        Dataset<Row> df = initTest("CREATE (p:Person {bools: [true, false]})");

        List<Boolean> res = df.select("bools").collectAsList().get(0).getList(0);
        assertEquals(true, res.get(0));
        assertEquals(false, res.get(1));
    }

    @Test
    public void testReadNodeWithArrayDate() {
        Dataset<Row> df = initTest("CREATE (p:Person {dates: [date('2009-10-10'), date('2009-10-11')]})");

        List<Date> res = df.select("dates").collectAsList().get(0).getList(0);
        assertEquals(Date.valueOf("2009-10-10"), res.get(0));
        assertEquals(Date.valueOf("2009-10-11"), res.get(1));
    }

    @Test
    public void testReadNodeWithArrayZonedDateTime() {
        String datetime1 = "2015-06-24T12:50:35.556+01:00";
        String datetime2 = "2015-06-23T12:50:35.556+01:00";
        Dataset<Row> df = initTest("CREATE (p:Person {dates: [datetime('"+datetime1+"'), datetime('"+datetime2+"')]})");

        List<Timestamp> res = df.select("dates").collectAsList().get(0).getList(0);
        assertEquals(Timestamp.from(OffsetDateTime.parse(datetime1).toInstant()), res.get(0));
        assertEquals(Timestamp.from(OffsetDateTime.parse(datetime2).toInstant()), res.get(1));
    }

    @Test
    public void testReadNodeWithArrayDurations() {
        Dataset<Row> df = initTest("CREATE (p:Person {durations: [duration({months: 0.75}), duration({weeks: 2.5})]})");

        List<GenericRowWithSchema> res = df.select("durations").collectAsList().get(0).getList(0);
        assertEquals("duration", res.get(0).get(0));
        assertEquals(0L, res.get(0).get(1));
        assertEquals(22L, res.get(0).get(2));
        assertEquals(71509L, res.get(0).get(3));
        assertEquals(500000000, res.get(0).get(4));
        assertEquals("P0M22DT71509.500000000S", res.get(0).get(5));

        assertEquals("duration", res.get(1).get(0));
        assertEquals(0L, res.get(1).get(1));
        assertEquals(17L, res.get(1).get(2));
        assertEquals(43200L, res.get(1).get(3));
        assertEquals(0, res.get(1).get(4));
        assertEquals("P0M17DT43200S", res.get(1).get(5));
    }

    @Test
    public void testReadNodeWithPointArray() {
        Dataset<Row> df = initTest("CREATE (p:Person {locations: [point({x: 11, y: 33.111}), point({x: 22, y: 44.222})]})");

        List<GenericRowWithSchema> res = df.select("locations").collectAsList().get(0).getList(0);
        assertEquals("point-2d", res.get(0).get(0));
        assertEquals(7203, res.get(0).get(1));
        assertEquals(11.0, res.get(0).get(2));
        assertEquals(33.111, res.get(0).get(3));

        assertEquals("point-2d", res.get(1).get(0));
        assertEquals(7203, res.get(1).get(1));
        assertEquals(22.0, res.get(1).get(2));
        assertEquals(44.222, res.get(1).get(3));
    }

    @Test
    public void testReadNodeWithGeoPointArray() {
        Dataset<Row> df = initTest("CREATE (p:Person {locations: [point({longitude: 11, latitude: 33.111}), point({longitude: 22, latitude: 44.222})]})");

        List<GenericRowWithSchema> res = df.select("locations").collectAsList().get(0).getList(0);
        assertEquals("point-2d", res.get(0).get(0));
        assertEquals(4326, res.get(0).get(1));
        assertEquals(11.0, res.get(0).get(2));
        assertEquals(33.111, res.get(0).get(3));

        assertEquals("point-2d", res.get(1).get(0));
        assertEquals(4326, res.get(1).get(1));
        assertEquals(22.0, res.get(1).get(2));
        assertEquals(44.222, res.get(1).get(3));
    }

    @Test
    public void testReadNodeWithPoint3DArray() {
        Dataset<Row> df = initTest("CREATE (p:Person {locations: [point({x: 11, y: 33.111, z: 12}), point({x: 22, y: 44.222, z: 99.1})]})");

        List<GenericRowWithSchema> res = df.select("locations").collectAsList().get(0).getList(0);
        assertEquals("point-3d", res.get(0).get(0));
        assertEquals(9157, res.get(0).get(1));
        assertEquals(11.0, res.get(0).get(2));
        assertEquals(33.111, res.get(0).get(3));

        assertEquals("point-3d", res.get(1).get(0));
        assertEquals(9157, res.get(1).get(1));
        assertEquals(22.0, res.get(1).get(2));
        assertEquals(44.222, res.get(1).get(3));
    }

    @Test
    public void testReadNodeWithMap() {
        Dataset<Row> df = ss().read().format(DataSource.class.getName())
                .option("url", SparkConnectorScalaSuiteIT.server().getBoltUrl())
                .option("query", "RETURN {a: 1, b: '3'} AS map")
                .load();

        Map<Object, Object> map = df.select("map").collectAsList().get(0).getJavaMap(0);
        Map<String, String> expectedMap = new HashMap<>();
        expectedMap.put("a", "1");
        expectedMap.put("b", "3");

        assertEquals(expectedMap, map);
    }

    Dataset<Row> initTest(String query) {
        SparkConnectorScalaSuiteIT.session()
                .writeTransaction(transaction -> transaction.run(query).consume());

        return ss().read().format(DataSource.class.getName())
                .option("url", SparkConnectorScalaSuiteIT.server().getBoltUrl())
                .option("labels", "Person")
                .load();
    }
}
