package builds

import jetbrains.buildServer.configs.kotlin.BuildSteps
import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.ParameterDisplay
import jetbrains.buildServer.configs.kotlin.buildSteps.MavenBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.ScriptBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.script
import jetbrains.buildServer.configs.kotlin.toId

class Release(id: String, name: String, javaVersion: JavaVersion) :
    BuildType(
        {
          this.id(id.toId())
          this.name = name

          params {
            text(
                "releaseVersion",
                "",
                allowEmpty = false,
                display = ParameterDisplay.PROMPT,
                label = "Version to release",
            )
            text(
                "nextSnapshotVersion",
                "",
                allowEmpty = false,
                display = ParameterDisplay.PROMPT,
                label = "Version on the next snapshot after the release",
            )

            password("env.JRELEASER_GITHUB_TOKEN", "%github-pull-request-token%")
          }

          steps {
            setVersion("Set release version", "%releaseVersion%", javaVersion)

            commonMaven(javaVersion) {
              this.name = "Build versionalised package"
              goals = "package"
              runnerArgs = "$MAVEN_DEFAULT_ARGS -Djava.version=${javaVersion.version} -DskipTests"
            }

            commitAndPush("Push release version", "build: release version %releaseVersion%")

            commonMaven(javaVersion) {
              this.name = "Release to Github"
              goals = "jreleaser:full-release"
              runnerArgs =
                  "$MAVEN_DEFAULT_ARGS -Djava.version=${javaVersion.version} -Prelease -pl :packaging"
            }

            setVersion("Set next snapshot version", "%nextSnapshotVersion%", javaVersion)

            commitAndPush(
                "Push next snapshot version",
                "build: update version to %nextSnapshotVersion%",
            )
          }

          requirements { runOnLinux(LinuxSize.SMALL) }
        },
    )

fun BuildSteps.setVersion(name: String, version: String, javaVersion: JavaVersion): MavenBuildStep {
  return this.commonMaven(javaVersion) {
    this.name = name
    goals = "versions:set"
    runnerArgs =
        "$MAVEN_DEFAULT_ARGS -Djava.version=${javaVersion.version} -DnewVersion=$version -DgenerateBackupPoms=false"
  }
}

fun BuildSteps.commitAndPush(
    name: String,
    commitMessage: String,
    includeFiles: String = "\\*pom.xml"
): ScriptBuildStep {
  return this.script {
    this.name = name
    scriptContent =
        """
          #!/bin/bash -eu              
         
          git add $includeFiles
          git commit -m "$commitMessage"
          git push
        """
            .trimIndent()
  }
}
