package builds

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildFeatures.dockerSupport
import jetbrains.buildServer.configs.kotlin.buildSteps.ScriptBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.script
import jetbrains.buildServer.configs.kotlin.toId

class PRCheck(id: String, name: String) :
    BuildType({
      this.id(id.toId())
      this.name = name

      steps {
        script {
          scriptContent =
              """
                #!/bin/bash
                
                set -eu
                
                export DANGER_GITHUB_API_TOKEN=%github-pull-request-token%
                export PULL_REQUEST_URL=https://github.com/$GITHUB_OWNER/$GITHUB_REPOSITORY/%teamcity.build.branch%
                
                # process pull request
                npm ci
                npx danger ci --verbose --failOnErrors
            """
                  .trimIndent()

          dockerImage = "node:18.4"
          dockerImagePlatform = ScriptBuildStep.ImagePlatform.Linux
        }
      }

      features { dockerSupport {} }

      requirements { runOnLinux(LinuxSize.SMALL) }
    })
