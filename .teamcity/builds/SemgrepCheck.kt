package builds

import jetbrains.buildServer.configs.kotlin.buildSteps.ScriptBuildStep

class SemgrepCheck(id: String, name: String, scalaVersion: ScalaVersion) :
    Maven(
        id,
        name,
        "dependency:tree",
        JavaVersion.V_17,
        scalaVersion,
        Neo4jVersion.V_NONE,
        "-DoutputFile=maven_dep_tree.txt") {

  init {

    params.password("env.SEMGREP_APP_TOKEN", "%semgrep-app-token%")
    params.text("env.SEMGREP_REPO_NAME", FULL_GITHUB_REPOSITORY)
    params.text("env.SEMGREP_REPO_URL", GITHUB_URL)
    params.text("env.SEMGREP_BRANCH", "%teamcity.build.branch%")
    params.text("env.SEMGREP_JOB_URL", "%env.BUILD_URL%")
    params.text("env.SEMGREP_COMMIT", "%env.BUILD_VCS_NUMBER%")

    steps.step(
        ScriptBuildStep {
          scriptContent ="semgrep ci --no-git-ignore"
          dockerImagePlatform = ScriptBuildStep.ImagePlatform.Linux
          dockerImage = SEMGREP_DOCKER_IMAGE
          dockerRunParameters =
              "--volume /var/run/docker.sock:/var/run/docker.sock --volume %teamcity.build.checkoutDir%/signingkeysandbox:/root/.gnupg"
        })
  }
}
