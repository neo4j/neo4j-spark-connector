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
                    skipWhenDocsOnly(
                        SemgrepCheck(
                            "${name}-semgrep-check-${scala.version}",
                            "semgrep check (${scala.version})",
                            scala)))
              }

              javaVersions.cartesianProduct(scalaVersions, neo4jVersions).forEach {
                  (java, scala, neo4j) ->
                sequential {
                  // note that artifact dependencies are resolved before step conditions are
                  // evaluated, so for a docs-only change the python integration tests below still
                  // attempt to download the jar this build has not published
                  val packaging =
                      skipWhenDocsOnly(
                          Package(
                              "${name}-package-${java.version}-${scala.version}-${neo4j.version}",
                              "package (${java.version}, ${scala.version}, ${neo4j.version})",
                              java,
                              scala,
                          ),
                      )

                  dependentBuildType(
                      skipWhenDocsOnly(
                          Maven(
                              "${name}-build-${java.version}-${scala.version}-${neo4j.version}",
                              "build (${java.version}, ${scala.version}, ${neo4j.version})",
                              "test-compile",
                              java,
                              scala,
                          ),
                      ),
                  )

                  dependentBuildType(
                      skipWhenDocsOnly(
                          Maven(
                              "${name}-unit-tests-${java.version}-${scala.version}-${neo4j.version}",
                              "unit tests (${java.version}, ${scala.version}, ${neo4j.version})",
                              "test",
                              java,
                              scala,
                              neo4j,
                          ),
                      ),
                  )

                  dependentBuildType(
                      collectArtifacts(
                          packaging,
                      ),
                  )

                  parallel {
                    dependentBuildType(
                        skipWhenDocsOnly(
                            JavaIntegrationTests(
                                "${name}-integration-tests-java-${java.version}-${scala.version}-${neo4j.version}",
                                "java integration tests (${java.version}, ${scala.version}, ${neo4j.version})",
                                java,
                                scala,
                                neo4j,
                            ) {},
                        ),
                    )

                    pysparkVersions
                        .filter { it.shouldTestWith(java, scala) }
                        .forEach { pyspark ->
                          pyspark.pythonVersions.forEach { python ->
                            dependentBuildType(
                                skipWhenDocsOnly(
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
                                    ?:packages/*.jar => ./scripts/python
                                    """
                                                  .trimIndent()
                                        }
                                      }
                                    },
                                ),
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
              requireDiskSpace("6gb")
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
