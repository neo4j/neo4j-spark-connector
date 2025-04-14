import builds.Build
import builds.JavaVersion
import builds.Neo4jSparkConnectorVcs
import builds.Neo4jVersion
import builds.SLACK_CHANNEL
import builds.SLACK_CONNECTION_ID
import builds.ScalaVersion
import jetbrains.buildServer.configs.kotlin.Project
import jetbrains.buildServer.configs.kotlin.buildFeatures.notifications
import jetbrains.buildServer.configs.kotlin.project
import jetbrains.buildServer.configs.kotlin.triggers.schedule
import jetbrains.buildServer.configs.kotlin.triggers.vcs
import jetbrains.buildServer.configs.kotlin.version

version = "2025.03"

project {
  params {
    password("github-commit-status-token", "%github-token%")
    password("github-pull-request-token", "%github-token%")
  }

  vcsRoot(Neo4jSparkConnectorVcs)

  subProject(
      Build(
          name = "main",
          javaVersions =
              listOf(JavaVersion.V_8, JavaVersion.V_11, JavaVersion.V_17, JavaVersion.V_21),
          scalaVersions = listOf(ScalaVersion.V2_12, ScalaVersion.V2_13),
          neo4jVersion = Neo4jVersion.V_2025,
          forPullRequests = false) {
            triggers {
              vcs {
                this.branchFilter = "+:setup-ci"
                this.triggerRules =
                    """
              -:comment=^build.*release version.*:**
              -:comment=^build.*update version.*:**
              """
                        .trimIndent()
              }
            }
          })

  subProject(
      Build(
          name = "pull-request",
          javaVersions =
              listOf(JavaVersion.V_8, JavaVersion.V_11, JavaVersion.V_17, JavaVersion.V_21),
          scalaVersions = listOf(ScalaVersion.V2_12, ScalaVersion.V2_13),
          neo4jVersion = Neo4jVersion.V_2025,
          forPullRequests = true) {
            triggers { vcs { this.branchFilter = "+:pull/*" } }
          })

  subProject(
      Project {
        this.id("compatibility")
        name = "compatibility"

        Neo4jVersion.entries.forEach { neo4j ->
          subProject(
              Build(
                  name = "${neo4j.version}",
                  javaVersions =
                      listOf(JavaVersion.V_8, JavaVersion.V_11, JavaVersion.V_17, JavaVersion.V_21),
                  scalaVersions = listOf(ScalaVersion.V2_12, ScalaVersion.V2_13),
                  neo4jVersion = neo4j,
                  forPullRequests = false) {
                    triggers {
                      vcs { enabled = false }

                      schedule {
                        branchFilter = "+:5.0"
                        schedulingPolicy = daily {
                          hour = 8
                          minute = 0
                        }
                        triggerBuild = always()
                      }
                    }

                    features {
                      notifications {
                        buildFailedToStart = true
                        buildFailed = true
                        firstFailureAfterSuccess = true
                        firstSuccessAfterFailure = true
                        buildProbablyHanging = true

                        notifierSettings = slackNotifier {
                          connection = SLACK_CONNECTION_ID
                          sendTo = SLACK_CHANNEL
                          messageFormat = simpleMessageFormat()
                        }
                      }
                    }
                  })
        }
      })
}
