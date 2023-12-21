package org.neo4j.spark.streaming

import org.apache.spark.sql.types.DataTypes
import org.junit.{Assert, Test}

class Neo4jOffsetTest {
    @Test
    def testCustomType(): Unit = {
      // given
      val json = """{"dataType": "string", "value": "foo"}"""
      // when
      val actual = Neo4jOffset.from(json)

      // then
      val expected = Neo4jOffset("foo", DataTypes.StringType)

      Assert.assertEquals(expected, actual)
      Assert.assertEquals("foo", actual.getValueAs[String])
    }

  @Test
  def testDefaultType(): Unit = {
    // given
    val json = """{"value": 1}"""
    // when
    val actual = Neo4jOffset.from(json)

    // then
    val expected = Neo4jOffset(1L)

    Assert.assertEquals(expected, actual)
    Assert.assertEquals(1L, actual.getValueAs[Long])
  }

}
