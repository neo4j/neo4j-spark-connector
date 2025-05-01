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

            text("env.JRELEASER_DRY_RUN", "%$DRY_RUN%")
            text("env.JRELEASER_PROJECT_VERSION", "%releaseVersion%")
            text("env.JRELEASER_UPLOAD_S3_ACTIVE", "")

            text("env.JRELEASER_S3_ACTIVE", "NEVER")
            text("env.JRELEASER_S3_REGION", "%aws-s3-region%")
            text("env.JRELEASER_S3_BUCKET", "%aws-s3-bucket%")
            text("env.JRELEASER_S3_ACCESS_KEY_ID", "%aws-s3-access-key-id%")
            text("env.JRELEASER_S3_SECRET_KEY", "%aws-s3-secret-key%")
            text("env.JRELEASER_S3_PATH", "/")

            text("env.JRELEASER_ANNOUNCE_SLACK_ACTIVE", "NEVER")
            text("env.JRELEASER_ANNOUNCE_SLACK_TOKEN", "%slack-token%")
            text("env.JRELEASER_ANNOUNCE_SLACK_WEBHOOK", "%slack-webhook%")

            password("env.JRELEASER_GITHUB_TOKEN", "%github-pull-request-token%")
            password("env.OSSSONATYPEORG_USERNAME", "%osssonatypeorg-username%")
            password("env.OSSSONATYPEORG_PASSWORD", "%osssonatypeorg-password%")
            password("env.SIGNING_KEY_PASSPHRASE", "%signing-key-passphrase%")
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
                curl -sL https://git.io/get-jreleaser > get_jreleaser.java

                # Download JReleaser with version = 1.18.0
                java get_jreleaser.java 1.18.0

                # Execute JReleaser
                java -jar jreleaser-cli.jar assemble
                java -jar jreleaser-cli.jar full-release
              """
                      .trimIndent()

              dockerImagePlatform = ScriptBuildStep.ImagePlatform.Linux
              dockerImage = javaVersion.dockerImage
              dockerRunParameters = "--volume /var/run/docker.sock:/var/run/docker.sock"
            }

            setVersion("Set next snapshot version", "%nextSnapshotVersion%", javaVersion)

            commitAndPush(
                "Push next snapshot version",
                "build: update version to %nextSnapshotVersion%",
                dryRunParameter = DRY_RUN,
            )

            ScalaVersion.entries.forEach { scala ->
              publishToMavenCentral(
                  "Publish to Maven Central",
                  "org.neo4j",
                  "./artifacts/${scala.version}/maven-artifacts",
                  dryRunParameter = DRY_RUN,
              )
            }
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
