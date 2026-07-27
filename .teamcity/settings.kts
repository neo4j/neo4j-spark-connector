import builds.Build
import builds.DEFAULT_BRANCH
import builds.JavaVersion
import builds.Neo4jSparkConnectorVcs
import builds.Neo4jVersion
import builds.PySparkVersion
import builds.ScalaVersion
import jetbrains.buildServer.configs.kotlin.Project
import jetbrains.buildServer.configs.kotlin.failureConditions.BuildFailureOnText
import jetbrains.buildServer.configs.kotlin.failureConditions.failOnText
import jetbrains.buildServer.configs.kotlin.project
import jetbrains.buildServer.configs.kotlin.triggers.schedule
import jetbrains.buildServer.configs.kotlin.triggers.vcs
import jetbrains.buildServer.configs.kotlin.version

version = "2025.11"

project {
  params {
    text("default-spark-branch", DEFAULT_BRANCH)

    text("osssonatypeorg-username", "%publish-username%")
    password("osssonatypeorg-password", "%publish-password%")
    password("signing-key-passphrase", "%publish-signing-key-password%")
    password("github-commit-status-token", "%github-token%")
    password("github-pull-request-token", "%github-token%")
    password("semgrep-app-token", "%semgrep-token%")
  }

  vcsRoot(Neo4jSparkConnectorVcs)

  subProject(
      Build(
          name = "main",
          javaVersions = setOf(JavaVersion.V_17, JavaVersion.V_21),
          scalaVersions = setOf(ScalaVersion.V2_13),
          pysparkVersions = setOf(PySparkVersion.V4_0, PySparkVersion.V4_1),
          neo4jVersions = setOf(Neo4jVersion.V_5, Neo4jVersion.V_CALVER),
          forPullRequests = false,
      ) {
        triggers {
          vcs {
            this.branchFilter = buildString {
              appendLine("+:$DEFAULT_BRANCH")
              appendLine("+:refs/heads/$DEFAULT_BRANCH")
            }

            this.triggerRules =
                """
              -:comment=^build.*release version.*:**
              -:comment=^build.*update version.*:**
              -:docs/**
              -:.github/workflows/docs-*.yml
              """
                    .trimIndent()
          }
        }
      },
  )

  subProject(
      Build(
          name = "pull-request",
          javaVersions = setOf(JavaVersion.V_17),
          scalaVersions = setOf(ScalaVersion.V2_13),
          pysparkVersions = setOf(PySparkVersion.V4_1),
          neo4jVersions = setOf(Neo4jVersion.V_5, Neo4jVersion.V_CALVER),
          forPullRequests = true,
      ) {
        triggers {
          vcs {
            this.branchFilter = buildString {
              appendLine("+:pull/*")
              appendLine("+:refs/heads/pull/*")
            }

            this.triggerRules =
                """
              -:docs/**
              -:.github/workflows/docs-*.yml
              """
                    .trimIndent()
          }
        }

        // when a PR gets closed, TC falls back to main branch to run the pipeline, which we don't
        // want
        failureConditions {
          failOnText {
            conditionType = BuildFailureOnText.ConditionType.CONTAINS
            pattern = "which does not correspond to any branch monitored by the build VCS roots"
            failureMessage = "Error: The branch %teamcity.build.branch% does not exist"
            reverse = false
            stopBuildOnFailure = true
          }
        }
      },
  )

  subProject(
      Project {
        this.id("compatibility")
        name = "compatibility"

        Neo4jVersion.entries.minus(Neo4jVersion.V_NONE).forEach { neo4j ->
          subProject(
              Build(
                  name = neo4j.version,
                  javaVersions = setOf(JavaVersion.V_17, JavaVersion.V_21),
                  scalaVersions = setOf(ScalaVersion.V2_13),
                  pysparkVersions = setOf(PySparkVersion.V4_0, PySparkVersion.V4_1),
                  neo4jVersions = setOf(neo4j),
                  forPullRequests = false,
                  forCompatibility = true,
              ) {
                triggers {
                  vcs { enabled = false }

                  schedule {
                    branchFilter = buildString {
                      appendLine("+:$DEFAULT_BRANCH")
                      appendLine("+:refs/heads/$DEFAULT_BRANCH")
                    }
                    schedulingPolicy = daily {
                      hour = 6
                      minute = 0
                    }
                    triggerBuild = always()
                    withPendingChangesOnly = false
                    enforceCleanCheckout = true
                    enforceCleanCheckoutForDependencies = true
                  }
                }
              },
          )
        }
      },
  )
}
