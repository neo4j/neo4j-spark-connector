package builds

import jetbrains.buildServer.configs.kotlin.AbsoluteId
import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildSteps.script
import jetbrains.buildServer.configs.kotlin.toId

class WhiteListCheck(id: String, name: String) :
    BuildType({
      this.id(id.toId())
      this.name = name

      dependencies {
        artifacts(AbsoluteId("Tools_WhitelistCheck")) {
          buildRule = lastSuccessful()
          cleanDestination = true
          artifactRules = "whitelist-check.tar.gz!** => whitelist-check/"
        }
      }

      steps {
        script {
          scriptContent =
              """
                #!/bin/bash -eu
                
                BRANCH=%teamcity.pullRequest.source.branch%
                if [[ "${'$'}BRANCH" =~ dependabot/.* ]]; then
                  echo "Raised by dependabot, skipping the white list check"
                  exit 0
                fi
                
                echo "Checking committers on PR %teamcity.build.branch%"
                TOKEN="%github-pull-request-token%"
                
                # process pull request
                ./whitelist-check/bin/examine-pull-request $GITHUB_OWNER $GITHUB_REPOSITORY "${'$'}{TOKEN}" %teamcity.build.branch% whitelist-check/cla-database.csv
            """
                  .trimIndent()
          formatStderrAsError = true
        }
      }

      requirements { runOnLinux(LinuxSize.SMALL) }
    })
