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
const val MAVEN_DEFAULT_ARGS =
    "--no-transfer-progress --batch-mode -Dmaven.repo.local=%teamcity.build.checkoutDir%/.m2/repository"

val DEFAULT_JAVA_VERSION = JavaVersion.V_17

// Look into Root Project's settings -> Connections
const val SLACK_CONNECTION_ID = "PROJECT_EXT_83"
const val SLACK_CHANNEL = "#team-connectors-feed"

// Look into Root Project's settings -> Connections
const val ECR_CONNECTION_ID = "PROJECT_EXT_124"

enum class LinuxSize(val value: String) {
  SMALL("small"),
  LARGE("large")
}

enum class JavaVersion(val version: String, val dockerImage: String) {
  V_17(version = "17", dockerImage = "eclipse-temurin:17-jdk"),
  V_21(version = "21", dockerImage = "eclipse-temurin:21-jdk"),
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

enum class SparkVersion(val short: String, val version: String) {
  V4_0_1(short = "4", version = "4.0.1"),
  V4_1_0(short = "4", version = "4.1.0"),
}

enum class PySparkVersion(
    val sparkVersion: SparkVersion,
    val scalaVersion: ScalaVersion,
    val javaVersions: Set<JavaVersion>,
    val pythonVersions: Set<PythonVersion>,
) {
  V4_0(
      SparkVersion.V4_0_1,
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
      SparkVersion.V4_1_0,
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
  V_4_4("4.4", "neo4j:4.4-enterprise"),
  V_4_4_DEV(
      "4.4-dev",
      "535893049302.dkr.ecr.eu-west-1.amazonaws.com/build-service/neo4j:4.4-enterprise-debian-nightly",
  ),
  V_5("5", "neo4j:5-enterprise"),
  V_5_DEV(
      "5-dev",
      "535893049302.dkr.ecr.eu-west-1.amazonaws.com/build-service/neo4j:5-enterprise-debian-nightly-bundle",
  ),
  V_2025("2025", "neo4j:2025-enterprise"),
  V_2025_DEV(
      "2025-dev",
      "535893049302.dkr.ecr.eu-west-1.amazonaws.com/build-service/neo4j:2025-enterprise-debian-nightly-bundle",
  ),
}

data class SnykProfile(
    val name: String,
    val mavenArgs: String,
    val dockerImage: String = "snyk/snyk:maven-3-jdk-17"
)

val SNYK_PROFILES =
    setOf(
        SnykProfile("scala-2-13", "-Pscala-2.13"),
    )

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
  loginToRegistry = on { dockerRegistryId = ECR_CONNECTION_ID }
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
        +:spark/target/*_for_spark_*.jar => packages
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
