package org.neo4j.spark

import org.junit.Assert.assertEquals
import org.junit.Test

class VersionTest {

  @Test
  def parses_versions(): Unit = {
    assertEquals(Version(5, 26, 399), Version.parse("5.26.399"))
    assertEquals(Version(2025, 11, 0), Version.parse("2025.11.0-41865"))
  }
}
