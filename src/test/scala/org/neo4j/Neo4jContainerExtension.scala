package org.neo4j

import org.testcontainers.containers.Neo4jContainer

class Neo4jContainerExtension(imageName: String) extends Neo4jContainer[Neo4jContainerExtension](imageName)
