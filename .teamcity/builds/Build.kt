package builds

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.Project
import jetbrains.buildServer.configs.kotlin.sequential
import jetbrains.buildServer.configs.kotlin.toId

val DEFAULT_NEO4J_VERSION = Neo4jVersion.V_2025

class Build(
    name: String,
    forPullRequests: Boolean,
    javaVersions: List<JavaVersion>,
    scalaVersions: List<ScalaVersion>,
    pysparkVersions: List<PySparkVersion>,
    neo4jVersion: Neo4jVersion = DEFAULT_NEO4J_VERSION,
    customizeCompletion: BuildType.() -> Unit = {}
) :
    Project(
        {
          this.id(name.toId())
          this.name = name

          val complete = Empty("${name}-complete", "complete")

          val bts = sequential {
            if (forPullRequests)
                buildType(WhiteListCheck("${name}-whitelist-check", "white-list check"))
            if (forPullRequests) dependentBuildType(PRCheck("${name}-pr-check", "pr check"))

            parallel {
              javaVersions.cartesianProduct(scalaVersions).forEach { (java, scala) ->
                sequential {
                  val packaging =
                      Package(
                          "${name}-package-${java.version}-${scala.version}",
                          "package (${java.version}, ${scala.version})",
                          java,
                          scala,
                      )

                  dependentBuildType(
                      Maven(
                          "${name}-build-${java.version}-${scala.version}",
                          "build (${java.version}, ${scala.version})",
                          "test-compile",
                          java,
                          scala,
                          neo4jVersion,
                      ),
                  )
                  dependentBuildType(
                      Maven(
                          "${name}-unit-tests-${java.version}-${scala.version}",
                          "unit tests (${java.version}, ${scala.version})",
                          "test",
                          java,
                          scala,
                          neo4jVersion,
                      ),
                  )
                  dependentBuildType(
                      collectArtifacts(
                          packaging,
                      ),
                  )
                  parallel {
                    dependentBuildType(
                        JavaIntegrationTests(
                            "${name}-integration-tests-java-${java.version}-${scala.version}-${neo4jVersion.version}",
                            "java integration tests (${java.version}, ${scala.version}, ${neo4jVersion.version})",
                            java,
                            scala,
                            neo4jVersion,
                        ) {},
                    )

                    pysparkVersions
                        .filter { it.shouldTestWith(java, scala) }
                        .forEach { pyspark ->
                          pyspark.pythonVersions.forEach { python ->
                            PythonIntegrationTests(
                                "${name}-integration-tests-pyspark-${java.version}-${scala.version}-${python.version}-${pyspark.version}",
                                "pyspark integration tests (${java.version}, ${scala.version}, ${python.version}, ${pyspark.version})",
                                java,
                                python,
                                scala,
                                pyspark.version,
                                neo4jVersion,
                            ) {
                              dependencies {
                                artifacts(packaging) {
                                  artifactRules =
                                      """
                                    +:packages/*.jar => ./scripts/python
                                    """
                                          .trimIndent()
                                }
                              }
                            }
                          }
                        }
                  }
                }
              }
            }

            dependentBuildType(complete)
            if (!forPullRequests)
                dependentBuildType(Release("${name}-release", "release", DEFAULT_JAVA_VERSION))
          }

          bts.buildTypes().forEach {
            it.thisVcs()

            it.features {
              loginToECR()
              requireDiskSpace("5gb")
              enableCommitStatusPublisher()
              if (forPullRequests) enablePullRequests()
            }

            buildType(it)
          }

          complete.apply(customizeCompletion)
        },
    )
