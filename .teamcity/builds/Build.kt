package builds

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.Project
import jetbrains.buildServer.configs.kotlin.buildFeatures.notifications
import jetbrains.buildServer.configs.kotlin.sequential
import jetbrains.buildServer.configs.kotlin.toId

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
              javaVersions.cartesianProduct(scalaVersions, neo4jVersions).forEach {
                  (java, scala, neo4j) ->
                sequential {
                  val packaging =
                      Package(
                          "${name}-package-${java.version}-${scala.version}-${neo4j.version}",
                          "package (${java.version}, ${scala.version}, ${neo4j.version})",
                          java,
                          scala,
                      )

                  dependentBuildType(
                      Maven(
                          "${name}-build-${java.version}-${scala.version}-${neo4j.version}",
                          "build (${java.version}, ${scala.version}, ${neo4j.version})",
                          "test-compile",
                          java,
                          scala,
                      ),
                  )

                  dependentBuildType(
                      Maven(
                          "${name}-unit-tests-${java.version}-${scala.version}-${neo4j.version}",
                          "unit tests (${java.version}, ${scala.version}, ${neo4j.version})",
                          "test",
                          java,
                          scala,
                          neo4j,
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
                            "${name}-integration-tests-java-${java.version}-${scala.version}-${neo4j.version}",
                            "java integration tests (${java.version}, ${scala.version}, ${neo4j.version})",
                            java,
                            scala,
                            neo4j,
                        ) {},
                    )

                    pysparkVersions
                        .filter { it.shouldTestWith(java, scala) }
                        .forEach { pyspark ->
                          pyspark.pythonVersions.forEach { python ->
                            dependentBuildType(
                                PythonIntegrationTests(
                                    "${name}-integration-tests-pyspark-${java.version}-${scala.version}-${neo4j.version}-${python.version}-${pyspark.sparkVersion.version}",
                                    "pyspark integration tests (${java.version}, ${scala.version}, ${neo4j.version}, ${python.version}, ${pyspark.sparkVersion.version})",
                                    java,
                                    python,
                                    scala,
                                    pyspark.sparkVersion,
                                    neo4j,
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

          complete.features {
            notifications {
              branchFilter =
                  """
                  +:$DEFAULT_BRANCH
                  ${if (forPullRequests) "+:pull/*" else ""}
                  """
                      .trimIndent()

              queuedBuildRequiresApproval = forPullRequests
              buildFailedToStart = !forPullRequests
              buildFailed = !forPullRequests
              firstFailureAfterSuccess = !forPullRequests
              firstSuccessAfterFailure = !forPullRequests
              buildProbablyHanging = !forPullRequests

              notifierSettings = slackNotifier {
                connection = SLACK_CONNECTION_ID
                sendTo = SLACK_CHANNEL
                messageFormat = simpleMessageFormat()
              }
            }
          }

          complete.apply(customizeCompletion)
        },
    )
