package builds

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildFeatures.buildCache
import jetbrains.buildServer.configs.kotlin.buildSteps.MavenBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.maven
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

            maven {
              this.goals = "verify"
              this.runnerArgs =
                  "$MAVEN_DEFAULT_ARGS -Djava.version=${javaVersion.version} -Dscala-${scalaVersion.version} -Dneo4j-${neo4jVersion.version} -DskipUnitTests"

              dockerImagePlatform = MavenBuildStep.ImagePlatform.Linux
              dockerImage = javaVersion.dockerImage
              dockerRunParameters = "--volume /var/run/docker.sock:/var/run/docker.sock"
            }
          }

          features {
            buildCache {
              this.name = "neo4j-spark-connector"
              publish = true
              use = true
              publishOnlyChanged = true
              rules = ".m2/repository"
            }
          }

          requirements { runOnLinux(LinuxSize.LARGE) }
        },
    )
