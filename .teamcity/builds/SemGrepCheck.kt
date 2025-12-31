package builds

import jetbrains.buildServer.configs.kotlin.buildSteps.ScriptBuildStep

class SemGrepCheck(
    id: String,
    name: String,
    scalaVersion: ScalaVersion
): Maven(
    id,
    name,
    "dependency:tree",
    JavaVersion.V_17,
    scalaVersion,
    Neo4jVersion.V_NONE,
    "-DoutputFile=maven_dep_tree.txt"
) {

  init {

    params.password("env.SEMGREP_APP_TOKEN", "%semgrep-app-token%")

    steps.step(ScriptBuildStep {
      scriptContent="semgrep ci --no-git-ignore"
      dockerImagePlatform = ScriptBuildStep.ImagePlatform.Linux
      dockerImage = "semgrep/semgrep:1.146.0"
      dockerRunParameters =
          "--volume /var/run/docker.sock:/var/run/docker.sock --volume %teamcity.build.checkoutDir%/signingkeysandbox:/root/.gnupg"
    })
  }

}
