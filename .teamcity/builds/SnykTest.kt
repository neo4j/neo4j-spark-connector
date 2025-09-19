package builds

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.ParameterDisplay
import jetbrains.buildServer.configs.kotlin.buildFeatures.buildCache
import jetbrains.buildServer.configs.kotlin.buildSteps.ScriptBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.script
import jetbrains.buildServer.configs.kotlin.toId

class SnykTest(id: String, name: String, snykProfile: SnykProfile) : BuildType(
    {
      this.id(id.toId())
      this.name = name

      params {
        password("env.SNYK_TOKEN", "%snyk-token%", display = ParameterDisplay.HIDDEN)
      }

      steps {
        script {
          scriptContent = """
            #!/bin/bash
            set -eux
            snyk test --severity-threshold=high --all-projects --policy-path=. -- ${snykProfile.mavenArgs}
          """.trimIndent()

          dockerImagePlatform = ScriptBuildStep.ImagePlatform.Linux
          dockerImage = snykProfile.dockerImage
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
    }
)
