package builds

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.Project
import jetbrains.buildServer.configs.kotlin.buildFeatures.notifications
import jetbrains.buildServer.configs.kotlin.sequential
import jetbrains.buildServer.configs.kotlin.toId

val DEFAULT_NEO4J_VERSION = Neo4jVersion.V_2025

class Build(
    name: String,
    forPullRequests: Boolean,
    javaVersions: Set<JavaVersion>,
    scalaVersions: Set<ScalaVersion>,
    pysparkVersions: Set<PySparkVersion>,
    neo4jVersions: Set<Neo4jVersion>,
    forCompatibility: Boolean = false,
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
                      ),
                  )

                  dependentBuildType(
                      collectArtifacts(
                          packaging,
                      ),
                  )

                  parallel {
                    neo4jVersions.forEach { neo4jVersion ->
                      dependentBuildType(
                          Maven(
                              "${name}-unit-tests-${java.version}-${scala.version}-${neo4jVersion.version}",
                              "unit tests (${java.version}, ${scala.version}, ${neo4jVersion.version})",
                              "test",
                              java,
                              scala,
                              neo4jVersion,
                          ),
                      )

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
                              dependentBuildType(
                                  PythonIntegrationTests(
                                      "${name}-integration-tests-pyspark-${java.version}-${scala.version}-${neo4jVersion.version}-${python.version}-${pyspark.sparkVersion.version}",
                                      "pyspark integration tests (${java.version}, ${scala.version}, ${neo4jVersion.version}, ${python.version}, ${pyspark.sparkVersion.version})",
                                      java,
                                      python,
                                      scala,
                                      pyspark.sparkVersion,
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
                                  },
                              )
                            }
                          }
                    }
                  }
                }
              }
            }

            dependentBuildType(complete)
            if (!forPullRequests && !forCompatibility)
                dependentBuildType(Release("${name}-release", "release", DEFAULT_JAVA_VERSION))
          }

          bts.buildTypes().forEach {
            it.thisVcs()

            it.features {
              loginToECR()
              requireDiskSpace("5gb")
              if (!forCompatibility) enableCommitStatusPublisher()
              if (forPullRequests) enablePullRequests()
            }

            buildType(it)
          }

          if (!forPullRequests) {
            complete.features {
              notifications {
                buildFailedToStart = true
                buildFailed = true
                firstFailureAfterSuccess = true
                firstSuccessAfterFailure = true
                buildProbablyHanging = true

                branchFilter = "+:main"

                notifierSettings = slackNotifier {
                  connection = SLACK_CONNECTION_ID
                  sendTo = SLACK_CHANNEL
                  messageFormat = simpleMessageFormat()
                }
              }
            }
          }

          complete.apply(customizeCompletion)
        },
    )
