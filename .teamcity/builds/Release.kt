package builds

import jetbrains.buildServer.configs.kotlin.AbsoluteId
import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.ParameterDisplay
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
                allowEmpty = false)
            text(
                "nextSnapshotVersion",
                "",
                label = "Next snapshot version",
                description = "Next snapshot version to set after release",
                display = ParameterDisplay.PROMPT,
                allowEmpty = false)

            checkbox(
                DRY_RUN,
                "true",
                "Dry run?",
                description =
                    "Whether to perform a dry run where nothing is published and released",
                display = ParameterDisplay.PROMPT,
                checked = "true",
                unchecked = "false")

            text("env.JRELEASER_DRY_RUN", "%$DRY_RUN%")

            password("env.JRELEASER_GITHUB_TOKEN", "%github-pull-request-token%")
            password("env.OSSSONATYPEORG_USERNAME", "%osssonatypeorg-username%")
            password("env.OSSSONATYPEORG_PASSWORD", "%osssonatypeorg-password%")
            password("env.SIGNING_KEY_PASSPHRASE", "%signing-key-passphrase%")
          }

          steps {
            setVersion("Set release version", "%releaseVersion%", javaVersion)

            ScalaVersion.entries.forEach { scalaVersion ->
              script {
                this.name = "Build for Scala ${scalaVersion.name}"

                scriptContent =
                    """
                          ./maven-release.sh deploy ${scalaVersion.version} "default::default::file://%teamcity.build.checkoutDir%/maven-artifacts"
                      """
                        .trimIndent()
              }
            }

            commitAndPush(
                "Push release version",
                "build: release version %releaseVersion%",
                dryRunParameter = DRY_RUN)

            runMaven(javaVersion) {
              this.name = "Release to Github"
              goals = "jreleaser:full-release"
              runnerArgs = "$MAVEN_DEFAULT_ARGS -Djava.version=${javaVersion.version} -Prelease"
            }

            setVersion("Set next snapshot version", "%nextSnapshotVersion%", javaVersion)

            commitAndPush(
                "Push next snapshot version",
                "build: update version to %nextSnapshotVersion%",
                dryRunParameter = DRY_RUN)

            publishToMavenCentral("Publish to Maven Central", dryRunParameter = DRY_RUN)
          }

          requirements { runOnLinux(LinuxSize.SMALL) }
        },
    )
