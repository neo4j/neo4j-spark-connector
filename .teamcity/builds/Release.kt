package builds

import jetbrains.buildServer.configs.kotlin.AbsoluteId
import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.ParameterDisplay
import jetbrains.buildServer.configs.kotlin.buildSteps.ScriptBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.script
import jetbrains.buildServer.configs.kotlin.toId

private const val DRY_RUN = "dry-run"

class Release(id: String, name: String, javaVersion: JavaVersion) :
    BuildType(
        {
          this.id(id.toId())
          this.name = name

          templates(AbsoluteId("FetchSigningKey"))

          params {
            text(
                "releaseVersion",
                "",
                label = "Version to release",
                display = ParameterDisplay.PROMPT,
                allowEmpty = false,
            )
            text(
                "nextSnapshotVersion",
                "",
                label = "Next snapshot version",
                description = "Next snapshot version to set after release",
                display = ParameterDisplay.PROMPT,
                allowEmpty = false,
            )

            checkbox(
                DRY_RUN,
                "true",
                "Dry run?",
                description =
                    "Whether to perform a dry run where nothing is published and released",
                display = ParameterDisplay.PROMPT,
                checked = "true",
                unchecked = "false",
            )

            password("env.JRELEASER_GITHUB_TOKEN", "%github-pull-request-token%")

            text("env.JRELEASER_DRY_RUN", "%$DRY_RUN%")
            text("env.JRELEASER_PROJECT_VERSION", "%releaseVersion%")

            text("env.JRELEASER_ANNOUNCE_SLACK_ACTIVE", "NEVER")
            text("env.JRELEASER_ANNOUNCE_SLACK_TOKEN", "%slack-token%")
            text("env.JRELEASER_ANNOUNCE_SLACK_WEBHOOK", "%slack-webhook%")

            password("env.JRELEASER_GPG_PASSPHRASE", "%signing-key-passphrase%")

            text("env.JRELEASER_MAVENCENTRAL_USERNAME", "%publish-username%")
            password("env.JRELEASER_MAVENCENTRAL_TOKEN", "%publish-password%")
          }

          steps {
            setVersion("Set release version", "%releaseVersion%", javaVersion)

            commitAndPush(
                "Push release version",
                "build: release version %releaseVersion%",
                dryRunParameter = DRY_RUN,
            )

            script {
              scriptContent =
                  """
                #!/bin/bash
                
                set -eux
                
                apt-get update
                apt-get install --yes build-essential curl git unzip zip
                
                # Get the jreleaser downloader
                curl -sL https://raw.githubusercontent.com/jreleaser/release-action/refs/tags/2.4.2/get_jreleaser.java > get_jreleaser.java

                # Download JReleaser with version = 1.18.0
                java get_jreleaser.java 1.18.0

                if [ "%dry-run%" = "true" ]; then
                  echo "we are on a dry run, only performing upload to maven central"
                  export JRELEASER_MAVENCENTRAL_STAGE=UPLOAD
                  export JRELEASER_ANNOUNCE_SLACK_ACTIVE=NEVER
                else
                  echo "we will do a full deploy to maven central"
                  export JRELEASER_MAVENCENTRAL_STAGE=FULL
                  export JRELEASER_ANNOUNCE_SLACK_ACTIVE=ALWAYS
                fi
                
                # Execute JReleaser
                java -jar jreleaser-cli.jar assemble
                java -jar jreleaser-cli.jar full-release
              """
                      .trimIndent()

              dockerImagePlatform = ScriptBuildStep.ImagePlatform.Linux
              dockerImage = javaVersion.dockerImage
              dockerRunParameters =
                  "--volume /var/run/docker.sock:/var/run/docker.sock --volume %teamcity.build.checkoutDir%/signingkeysandbox:/root/.gnupg"
            }

            setVersion("Set next snapshot version", "%nextSnapshotVersion%", javaVersion)

            commitAndPush(
                "Push next snapshot version",
                "build: update version to %nextSnapshotVersion%",
                dryRunParameter = DRY_RUN,
            )
          }

          artifactRules =
              """
            +:artifacts => artifacts
            +:out/jreleaser => jreleaser
            """
                  .trimIndent()

          dependencies {
            artifacts(AbsoluteId("Tools_ReleaseTool")) {
              buildRule = lastSuccessful()
              artifactRules = "rt.jar => lib"
            }
          }

          requirements { runOnLinux(LinuxSize.SMALL) }
        },
    )
