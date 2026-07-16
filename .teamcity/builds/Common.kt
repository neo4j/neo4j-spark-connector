package builds

import builds.Neo4jSparkConnectorVcs.branchSpec
import jetbrains.buildServer.configs.kotlin.BuildFeatures
import jetbrains.buildServer.configs.kotlin.BuildSteps
import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.CompoundStage
import jetbrains.buildServer.configs.kotlin.FailureAction
import jetbrains.buildServer.configs.kotlin.Requirements
import jetbrains.buildServer.configs.kotlin.ReuseBuilds
import jetbrains.buildServer.configs.kotlin.buildFeatures.PullRequests
import jetbrains.buildServer.configs.kotlin.buildFeatures.buildCache
import jetbrains.buildServer.configs.kotlin.buildFeatures.commitStatusPublisher
import jetbrains.buildServer.configs.kotlin.buildFeatures.dockerRegistryConnections
import jetbrains.buildServer.configs.kotlin.buildFeatures.freeDiskSpace
import jetbrains.buildServer.configs.kotlin.buildFeatures.pullRequests
import jetbrains.buildServer.configs.kotlin.buildSteps.DockerCommandStep
import jetbrains.buildServer.configs.kotlin.buildSteps.MavenBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.ScriptBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.dockerCommand
import jetbrains.buildServer.configs.kotlin.buildSteps.maven
import jetbrains.buildServer.configs.kotlin.buildSteps.script
import jetbrains.buildServer.configs.kotlin.vcs.GitVcsRoot

const val GITHUB_OWNER = "neo4j"
const val GITHUB_REPOSITORY = "neo4j-spark-connector"
const val DEFAULT_BRANCH = "6.0"
val MAVEN_DEFAULT_ARGS = buildString {
  append("--no-transfer-progress ")
  append("--batch-mode ")
  append("--threads 1C ")
  append("-Dmaven.repo.local=%teamcity.build.checkoutDir%/.m2/repository ")
  append("-Dmaven.wagon.http.retryHandler.class=standard ")
  append("-Dmaven.wagon.http.retryHandler.timeout=60 ")
  append("-Dmaven.wagon.http.retryHandler.count=3 ")
  append(
      "-Dmaven.wagon.http.retryHandler.nonRetryableClasses=java.io.InterruptedIOException,java.net.UnknownHostException,java.net.ConnectException ")
}
const val FULL_GITHUB_REPOSITORY = "$GITHUB_OWNER/$GITHUB_REPOSITORY"
const val GITHUB_URL = "https://github.com/$FULL_GITHUB_REPOSITORY"

const val NODE_DOCKER_IMAGE = "%ecr-registry-connectors%:node-24-latest"

const val SEMGREP_DOCKER_IMAGE = "%ecr-registry-connectors%:semgrep-latest"

val DEFAULT_JAVA_VERSION = JavaVersion.V_17

// Look into Root Project's settings -> Connections
const val SLACK_CONNECTION_ID = "PROJECT_EXT_83"
const val SLACK_CHANNEL = "#team-connectors-feed"

// Look into Root Project's settings -> Connections
const val ECR_CONNECTION_ID_ENG = "PROJECT_EXT_124"
const val ECR_CONNECTION_ID_BUILD = "PROJECT_EXT_107"
val DOCKER_REGISTRIES = sequenceOf(ECR_CONNECTION_ID_ENG, ECR_CONNECTION_ID_BUILD)

enum class LinuxSize(val value: String) {
  SMALL("small"),
  LARGE("large")
}

enum class JavaVersion(val version: String, val dockerImage: String) {
  V_17(version = "17", dockerImage = "%ecr-registry-connectors%:jdk-17-latest"),
  V_21(version = "21", dockerImage = "%ecr-registry-connectors%:jdk-21-latest"),
}

enum class ScalaVersion(val version: String) {
  V2_13(version = "2.13"),
}

enum class PythonVersion(val version: String) {
  V3_10(version = "3.10"),
  V3_11(version = "3.11"),
  V3_12(version = "3.12"),
  V3_13(version = "3.13"),
  V3_14(version = "3.14"),
}

enum class SparkVersion(val version: String) {
  V4_0(version = "4.0.3"),
  V4_1(version = "4.1.2"),
}

enum class PySparkVersion(
    val sparkVersion: SparkVersion,
    val scalaVersion: ScalaVersion,
    val javaVersions: Set<JavaVersion>,
    val pythonVersions: Set<PythonVersion>,
) {
  V4_0(
      SparkVersion.V4_0,
      ScalaVersion.V2_13,
      setOf(
          JavaVersion.V_17,
          JavaVersion.V_21,
      ),
      setOf(
          PythonVersion.V3_10,
          PythonVersion.V3_11,
          PythonVersion.V3_12,
          PythonVersion.V3_13,
          PythonVersion.V3_14,
      ),
  ),
  V4_1(
      SparkVersion.V4_1,
      ScalaVersion.V2_13,
      setOf(
          JavaVersion.V_17,
          JavaVersion.V_21,
      ),
      setOf(
          PythonVersion.V3_10,
          PythonVersion.V3_11,
          PythonVersion.V3_12,
          PythonVersion.V3_13,
          PythonVersion.V3_14,
      ),
  ),
}

fun PySparkVersion.shouldTestWith(javaVersion: JavaVersion, scalaVersion: ScalaVersion): Boolean =
    this.javaVersions.contains(javaVersion) && this.scalaVersion == scalaVersion

enum class Neo4jVersion(val version: String, val dockerImage: String) {
  V_NONE("", ""),
  V_5("5", "neo4j:5-enterprise"),
  V_5_DEV(
      "5-dev",
      "535893049302.dkr.ecr.eu-west-1.amazonaws.com/build-service/neo4j:5-enterprise-debian-nightly-bundle",
  ),
  V_CALVER("2026", "neo4j:2026-enterprise"),
  V_CALVER_DEV(
      "2026-dev",
      "535893049302.dkr.ecr.eu-west-1.amazonaws.com/build-service/neo4j:2026-enterprise-debian-nightly-bundle",
  ),
}

fun <S, T, Y> Iterable<S>.cartesianProduct(
    other1: Collection<T>,
    other2: Collection<Y>
): Iterable<Triple<S, T, Y>> =
    this.flatMap { s -> other1.map { t -> s to t } }
        .flatMap { (s, t) -> other2.map { y -> Triple(s, t, y) } }

object Neo4jSparkConnectorVcs :
    GitVcsRoot(
        {
          id("Connectors_Neo4jSparkConnector_Build")

          name = "neo4j-spark-connector"
          url = "git@github.com:neo4j/neo4j-spark-connector.git"
          branch = "refs/heads/$DEFAULT_BRANCH"
          branchSpec = "refs/heads/*"

          authMethod = defaultPrivateKey { userName = "git" }
        },
    )

fun Requirements.runOnLinux(size: LinuxSize = LinuxSize.SMALL) {
  startsWith("cloud.amazon.agent-name-prefix", "linux-${size.value}")
}

fun BuildType.thisVcs(forBranch: String) = vcs {
  root(Neo4jSparkConnectorVcs)

  branchSpec = buildString {
    appendLine("-:*")
    appendLine("+:$forBranch")
  }

  cleanCheckout = true
}

fun BuildFeatures.enableCommitStatusPublisher() = commitStatusPublisher {
  vcsRootExtId = Neo4jSparkConnectorVcs.id.toString()
  publisher = github {
    githubUrl = "https://api.github.com"
    authType = personalToken { token = "%github-commit-status-token%" }
  }
}

fun BuildFeatures.enablePullRequests() = pullRequests {
  vcsRootExtId = Neo4jSparkConnectorVcs.id.toString()
  provider = github {
    authType = token { token = "%github-pull-request-token%" }
    filterAuthorRole = PullRequests.GitHubRoleFilter.EVERYBODY
    filterTargetBranch = buildString {
      appendLine("+:$DEFAULT_BRANCH")
      appendLine("+:refs/heads/$DEFAULT_BRANCH")
    }
  }
}

fun BuildFeatures.requireDiskSpace(size: String = "3gb") = freeDiskSpace {
  requiredSpace = size
  failBuild = true
}

fun BuildFeatures.loginToECR() = dockerRegistryConnections {
  cleanupPushedImages = true
  loginToRegistry = on { dockerRegistryId = DOCKER_REGISTRIES.joinToString(",") }
}

fun BuildFeatures.buildCache(javaVersion: JavaVersion, scalaVersion: ScalaVersion) = buildCache {
  this.name =
      "neo4j-spark-connector-${DEFAULT_BRANCH}-${javaVersion.version}-${scalaVersion.version}"
  publish = true
  use = true
  publishOnlyChanged = true
  rules = ".m2/repository"
}

fun CompoundStage.dependentBuildType(bt: BuildType, reuse: ReuseBuilds = ReuseBuilds.SUCCESSFUL) =
    buildType(bt) {
      onDependencyCancel = FailureAction.CANCEL
      onDependencyFailure = FailureAction.FAIL_TO_START
      reuseBuilds = reuse
    }

fun collectArtifacts(buildType: BuildType): BuildType {
  buildType.artifactRules =
      """
        +:spark/target/neo4j-spark-connector-*.jar => packages
        +:spark/target/*.zip => packages
    """
          .trimIndent()

  return buildType
}

fun BuildSteps.runMaven(javaVersion: JavaVersion, init: MavenBuildStep.() -> Unit): MavenBuildStep {
  val maven =
      this.maven {
        dockerImagePlatform = MavenBuildStep.ImagePlatform.Linux
        dockerImage = javaVersion.dockerImage
        dockerRunParameters = "--volume /var/run/docker.sock:/var/run/docker.sock"

        localRepoScope = MavenBuildStep.RepositoryScope.MAVEN_DEFAULT
      }

  init(maven)
  return maven
}

fun BuildSteps.setVersion(name: String, version: String, javaVersion: JavaVersion): MavenBuildStep {
  return this.runMaven(javaVersion) {
    this.name = name
    goals = "versions:set"
    runnerArgs =
        "$MAVEN_DEFAULT_ARGS -Djava.version=${javaVersion.version} -DnewVersion=$version -DgenerateBackupPoms=false"
  }
}

fun BuildSteps.commitAndPush(
    name: String,
    commitMessage: String,
    includeFiles: String = "\\*pom.xml",
    dryRunParameter: String = "dry-run"
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

    conditions { doesNotMatch(dryRunParameter, "true") }
  }
}

fun BuildSteps.pullImage(version: Neo4jVersion): DockerCommandStep =
    this.dockerCommand {
      name = "pull neo4j test image"
      commandType = other {
        subCommand = "image"
        commandArgs = "pull ${version.dockerImage}"
      }
    }
