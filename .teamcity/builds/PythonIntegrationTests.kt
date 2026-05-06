package builds

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildSteps.ScriptBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.script
import jetbrains.buildServer.configs.kotlin.toId

class PythonIntegrationTests(
    id: String,
    name: String,
    javaVersion: JavaVersion,
    pythonVersion: PythonVersion,
    scalaVersion: ScalaVersion,
    sparkVersion: SparkVersion,
    neo4jVersion: Neo4jVersion,
    init: BuildType.() -> Unit
) :
    BuildType(
        {
          this.id(id.toId())
          this.name = name

          init()

          artifactRules =
              """
              +:diagnostics => diagnostics.zip
              """
                  .trimIndent()

          params { text("env.NEO4J_TEST_IMAGE", neo4jVersion.dockerImage) }

          // Determine the test script and jar naming based on Spark major version
          val isSpark4 = sparkVersion.short == "4"
          val testScript = if (isSpark4) "test_spark4.py" else "test_spark.py"
          // Spark 4 artifact ID embeds _4_ to distinguish it from the spark-3 jar
          val jarNameExpr = if (isSpark4) {
            "neo4j-connector-apache-spark_4_\${scala_version}-\${project_version}_for_spark_${sparkVersion.short}.jar"
          } else {
            "neo4j-connector-apache-spark_\${scala_version}-\${project_version}_for_spark_${sparkVersion.short}.jar"
          }
          // Maven flag to activate the appropriate Spark 4.x sub-version
          val spark4MavenFlag = when (sparkVersion) {
            SparkVersion.V4_0_2 -> "-Dspark-4"
            SparkVersion.V4_1_1 -> "-Dspark-4.1"
            else -> ""
          }

          steps {
            if (neo4jVersion != Neo4jVersion.V_NONE) {
              pullImage(neo4jVersion)
            }

            script {
              scriptContent =
                  """
              #!/bin/bash -eu
              
              apt-get update
              apt-get install -o Acquire::Retries=10 --yes build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev curl git libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev
              curl -fsSL https://pyenv.run | bash
              
              export PYENV_ROOT="${'$'}HOME/.pyenv"
              export PATH="${'$'}PYENV_ROOT/bin:${'$'}PATH"
              eval "$(pyenv init - bash)"
              pyenv install ${pythonVersion.version}
              pyenv global ${pythonVersion.version}
                 
              python -m pip install --upgrade pip
              pip install pyspark==${sparkVersion.version} "testcontainers[neo4j]" six tzlocal==2.1 
              
              project_version="${'$'}(./mvnw help:evaluate -Dexpression="project.version" --quiet -DforceStdout)"
              scala_version="${scalaVersion.version}"
              jar_name="$jarNameExpr"
              # Build the connector jar for the target Spark version if not already built
              ./mvnw install -pl common,test-support,spark-4 $spark4MavenFlag -DskipTests --quiet ${'\$'}{MAVEN_DEFAULT_ARGS:-}
              cd ./scripts/python
              python $testScript "${'$'}{jar_name}" "${neo4jVersion.dockerImage}"
              """
                      .trimIndent()

              dockerImagePlatform = ScriptBuildStep.ImagePlatform.Linux
              dockerImage = javaVersion.dockerImage
              dockerRunParameters = "--volume /var/run/docker.sock:/var/run/docker.sock"
            }
          }

          requirements { runOnLinux(LinuxSize.SMALL) }
        },
    )
