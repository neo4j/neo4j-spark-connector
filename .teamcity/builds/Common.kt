package builds

import jetbrains.buildServer.configs.kotlin.BuildFeatures
import jetbrains.buildServer.configs.kotlin.BuildSteps
import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.CompoundStage
import jetbrains.buildServer.configs.kotlin.FailureAction
import jetbrains.buildServer.configs.kotlin.Requirements
import jetbrains.buildServer.configs.kotlin.buildFeatures.PullRequests
import jetbrains.buildServer.configs.kotlin.buildFeatures.commitStatusPublisher
import jetbrains.buildServer.configs.kotlin.buildFeatures.dockerRegistryConnections
import jetbrains.buildServer.configs.kotlin.buildFeatures.freeDiskSpace
import jetbrains.buildServer.configs.kotlin.buildFeatures.pullRequests
import jetbrains.buildServer.configs.kotlin.buildSteps.MavenBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.maven
import jetbrains.buildServer.configs.kotlin.vcs.GitVcsRoot

const val GITHUB_OWNER = "neo4j"
const val GITHUB_REPOSITORY = "neo4j-spark-connector"
const val MAVEN_DEFAULT_ARGS =
    "--no-transfer-progress --batch-mode -Dmaven.repo.local=%teamcity.build.checkoutDir%/.m2/repository"

val DEFAULT_JAVA_VERSION = JavaVersion.V_11

// Look into Root Project's settings -> Connections
const val SLACK_CONNECTION_ID = "PROJECT_EXT_83"
const val SLACK_CHANNEL = "#C05R4RURYLA" // #team-connectors-feed

// Look into Root Project's settings -> Connections
const val ECR_CONNECTION_ID = "PROJECT_EXT_124"

enum class LinuxSize(val value: String) {
  SMALL("small"),
  LARGE("large")
}

enum class JavaVersion(val version: String, val dockerImage: String) {
  V_8(version = "8", dockerImage = "eclipse-temurin:8-jdk"),
  V_11(version = "11", dockerImage = "eclipse-temurin:11-jdk"),
  V_17(version = "17", dockerImage = "eclipse-temurin:17-jdk"),
  V_21(version = "21", dockerImage = "eclipse-temurin:21-jdk"),
}

enum class ScalaVersion(val version: String) {
  V2_12(version = "2.12"),
  V2_13(version = "2.13"),
}

enum class Neo4jVersion(val version: String, val dockerImage: String) {
  V_4_4("4.4", "neo4j:4.4-enterprise"),
  V_4_4_DEV(
      "4.4-dev",
      "535893049302.dkr.ecr.eu-west-1.amazonaws.com/build-service/neo4j:4.4-enterprise-debian-nightly"),
  V_5("5", "neo4j:5-enterprise"),
  V_5_DEV(
      "5-dev",
      "535893049302.dkr.ecr.eu-west-1.amazonaws.com/build-service/neo4j:5-enterprise-debian-nightly"),
  V_2025("2025", "neo4j:2025-enterprise"),
  V_2025_DEV(
      "2025-dev",
      "535893049302.dkr.ecr.eu-west-1.amazonaws.com/build-service/neo4j:2025-enterprise-debian-nightly"),
}

fun <S, T> List<S>.cartesianProduct(other: List<T>): List<Pair<S, T>> =
    this.flatMap { s -> List(other.size) { s }.zip(other) }

object Neo4jSparkConnectorVcs :
    GitVcsRoot(
        {
          id("Connectors_Neo4jSparkConnector_Build")

          name = "neo4j-spark-connector"
          url = "git@github.com:neo4j/neo4j-spark-connector.git"
          branch = "refs/heads/setup-ci"
          branchSpec = "refs/heads/*"

          authMethod = defaultPrivateKey { userName = "git" }
        },
    )

fun Requirements.runOnLinux(size: LinuxSize = LinuxSize.SMALL) {
  startsWith("cloud.amazon.agent-name-prefix", "linux-${size.value}")
}

fun BuildType.thisVcs() = vcs {
  root(Neo4jSparkConnectorVcs)

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

fun CompoundStage.dependentBuildType(bt: BuildType) =
    buildType(bt) {
      onDependencyCancel = FailureAction.CANCEL
      onDependencyFailure = FailureAction.FAIL_TO_START
    }

fun collectArtifacts(buildType: BuildType): BuildType {
  buildType.artifactRules =
      """
        +:packaging/target/*.jar => packages
        +:packaging/target/*.zip => packages
    """
          .trimIndent()

  return buildType
}

fun BuildSteps.commonMaven(
    javaVersion: JavaVersion,
    init: MavenBuildStep.() -> Unit
): MavenBuildStep {
  val maven =
      this.maven {
        dockerImagePlatform = MavenBuildStep.ImagePlatform.Linux
        dockerImage = javaVersion.dockerImage
        dockerRunParameters = "--volume /var/run/docker.sock:/var/run/docker.sock"
      }

  init(maven)
  return maven
}
