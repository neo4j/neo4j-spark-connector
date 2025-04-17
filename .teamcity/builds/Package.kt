package builds

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildFeatures.buildCache
import jetbrains.buildServer.configs.kotlin.buildSteps.ScriptBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.script
import jetbrains.buildServer.configs.kotlin.toId

class Package(
    id: String,
    name: String,
    javaVersion: JavaVersion,
    scalaVersion: ScalaVersion,
) :
    BuildType({
      this.id(id.toId())
      this.name = name

      params { text("env.JAVA_VERSION", javaVersion.version) }

      steps {
        script {
          scriptContent =
              """
                  ./maven-release.sh package ${scalaVersion.version}
              """
                  .trimIndent()

          dockerImagePlatform = ScriptBuildStep.ImagePlatform.Linux
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

      requirements { runOnLinux(LinuxSize.SMALL) }
    })
