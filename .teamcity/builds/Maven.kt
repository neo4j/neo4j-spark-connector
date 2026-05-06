package builds

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.toId

open class Maven(
    id: String,
    name: String,
    goals: String,
    javaVersion: JavaVersion,
    scalaVersion: ScalaVersion,
    neo4jVersion: Neo4jVersion = Neo4jVersion.V_NONE,
    sparkProfiles: String = "",
    args: String? = null
) :
    BuildType(
        {
          this.id(id.toId())
          this.name = name

          params {
            text("env.JAVA_VERSION", javaVersion.version)
            text("env.NEO4J_TEST_IMAGE", neo4jVersion.dockerImage)
          }

          steps {
            if (neo4jVersion != Neo4jVersion.V_NONE) {
              pullImage(neo4jVersion)
            }

            runMaven(javaVersion) {
              this.goals = goals
              val profiles = if (sparkProfiles.isNotBlank()) " $sparkProfiles" else ""
              this.runnerArgs =
                  "$MAVEN_DEFAULT_ARGS -Djava.version=${javaVersion.version} -Dscala-${scalaVersion.version}$profiles ${args ?: ""}"
            }
          }

          features { buildCache(javaVersion, scalaVersion) }

          requirements { runOnLinux(LinuxSize.SMALL) }
        },
    )
