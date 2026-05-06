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
              scalaVersions.forEach { scala ->
                dependentBuildType(
                    SemgrepCheck(
                        "${name}-semgrep-check-${scala.version}",
                        "semgrep check (${scala.version})",
                        scala))
              }

              javaVersions.cartesianProduct(scalaVersions, neo4jVersions).forEach {
                  (java, scala, neo4j) ->
                // Spark 4.x requires Java 17+ and Scala 2.13 only; skip incompatible combinations
                if (scala == ScalaVersion.V2_13 && java.version.toInt() < 17) return@forEach
                // -Dspark-4 targets Spark 4.0.x; -Dspark-4.1 targets Spark 4.1.x.
                // Each is a separate Maven flag; CI runs them sequentially as separate args.
                // For unit/IT tests and packaging, Scala 2.13 builds pass -Dspark-4 which
                // activates both 4.0.2 spark.version and 4.1 can be run with -Dspark-4.1 separately.
                val spark4Profiles = if (scala == ScalaVersion.V2_13) "-Dspark-4" else ""
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
                          sparkProfiles = spark4Profiles,
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
                          sparkProfiles = spark4Profiles,
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
                            sparkProfiles = spark4Profiles,
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
            it.thisVcs(if (forPullRequests) "pull/*" else DEFAULT_BRANCH)

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
              branchFilter = buildString {
                appendLine("+:$DEFAULT_BRANCH")
                appendLine("+:refs/heads/$DEFAULT_BRANCH")
                if (forPullRequests) {
                  appendLine("+:pull/*")
                  appendLine("+:refs/heads/pull/*")
                }
              }

              queuedBuildRequiresApproval = forPullRequests
              buildFailedToStart = !forPullRequests
              buildFailed = !forPullRequests
              buildFinishedSuccessfully = !forPullRequests
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
