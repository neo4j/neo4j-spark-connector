package builds

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.toId

class JavaIntegrationTests(
    id: String,
    name: String,
    javaVersion: JavaVersion,
    scalaVersion: ScalaVersion,
    neo4jVersion: Neo4jVersion,
    init: BuildType.() -> Unit
) :
    BuildType(
        {
          this.id(id.toId())
          this.name = name

          init()

          artifactRules =
              """
              +:diagnostics => diagnostics.zip
              """
                  .trimIndent()

          params { text("env.NEO4J_TEST_IMAGE", neo4jVersion.dockerImage) }

          steps {
            if (neo4jVersion != Neo4jVersion.V_NONE) {
              pullImage(neo4jVersion)
            }

            runMaven(javaVersion) {
              this.goals = "verify"
              this.runnerArgs =
                  "$MAVEN_DEFAULT_ARGS -Djava.version=${javaVersion.version} -Dscala-${scalaVersion.version} -DskipUnitTests"
            }
          }

          features { buildCache(javaVersion, scalaVersion) }

          requirements { runOnLinux(LinuxSize.LARGE) }
        },
    )
