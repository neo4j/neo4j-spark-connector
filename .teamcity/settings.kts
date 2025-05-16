import builds.Build
import builds.DEFAULT_BRANCH
import builds.JavaVersion
import builds.Neo4jSparkConnectorVcs
import builds.Neo4jVersion
import builds.PySparkVersion
import builds.ScalaVersion
import jetbrains.buildServer.configs.kotlin.Project
import jetbrains.buildServer.configs.kotlin.project
import jetbrains.buildServer.configs.kotlin.triggers.schedule
import jetbrains.buildServer.configs.kotlin.triggers.vcs
import jetbrains.buildServer.configs.kotlin.version

version = "2025.03"

project {
  params {
    text("osssonatypeorg-username", "%publish-username%")
    password("osssonatypeorg-password", "%publish-password%")
    password("signing-key-passphrase", "%publish-signing-key-password%")
    password("github-commit-status-token", "%github-token%")
    password("github-pull-request-token", "%github-token%")
  }

  vcsRoot(Neo4jSparkConnectorVcs)

  subProject(
      Build(
          name = "main",
          javaVersions =
              setOf(JavaVersion.V_8, JavaVersion.V_11, JavaVersion.V_17, JavaVersion.V_21),
          scalaVersions = setOf(ScalaVersion.V2_12, ScalaVersion.V2_13),
          pysparkVersions = setOf(PySparkVersion.V3_4, PySparkVersion.V3_5),
          neo4jVersions = setOf(Neo4jVersion.V_4_4, Neo4jVersion.V_5, Neo4jVersion.V_2025),
          forPullRequests = false,
      ) {
        triggers {
          vcs {
            this.branchFilter = "+:$DEFAULT_BRANCH"
            this.triggerRules =
                """
              -:comment=^build.*release version.*:**
              -:comment=^build.*update version.*:**
              """
                    .trimIndent()
          }
        }
      },
  )

  subProject(
      Build(
          name = "pull-request",
          javaVersions = setOf(JavaVersion.V_11, JavaVersion.V_17),
          scalaVersions = setOf(ScalaVersion.V2_12, ScalaVersion.V2_13),
          pysparkVersions = setOf(PySparkVersion.V3_5),
          neo4jVersions = setOf(Neo4jVersion.V_4_4, Neo4jVersion.V_5, Neo4jVersion.V_2025),
          forPullRequests = true,
      ) {
        triggers { vcs { this.branchFilter = "+:pull/*" } }
      },
  )

  subProject(
      Project {
        this.id("compatibility")
        name = "compatibility"

        Neo4jVersion.entries.minus(Neo4jVersion.V_NONE).forEach { neo4j ->
          subProject(
              Build(
                  name = "${neo4j.version}",
                  javaVersions =
                      setOf(JavaVersion.V_8, JavaVersion.V_11, JavaVersion.V_17, JavaVersion.V_21),
                  scalaVersions = setOf(ScalaVersion.V2_12, ScalaVersion.V2_13),
                  pysparkVersions = setOf(PySparkVersion.V3_4, PySparkVersion.V3_5),
                  neo4jVersions = setOf(neo4j),
                  forPullRequests = false,
                  forCompatibility = true,
              ) {
                triggers {
                  vcs { enabled = false }

                  schedule {
                    branchFilter = "+:$DEFAULT_BRANCH"
                    schedulingPolicy = daily {
                      hour = 7
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
