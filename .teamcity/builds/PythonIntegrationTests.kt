package builds

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildFeatures.buildCache
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
              
              project_version="$(./mvnw help:evaluate -Dexpression="project.version" --quiet -DforceStdout)"
              jar_name="neo4j-connector-apache-spark_${scalaVersion.version}-${'$'}{project_version}_for_spark_${sparkVersion.short}.jar"
              cd ./scripts/python
              python test_spark.py "${'$'}{jar_name}" "${neo4jVersion.dockerImage}"
              """
                      .trimIndent()

              dockerImagePlatform = ScriptBuildStep.ImagePlatform.Linux
              dockerImage = javaVersion.dockerImage
              dockerRunParameters = "--volume /var/run/docker.sock:/var/run/docker.sock"
            }
          }

          features {
            buildCache {
              this.name = "neo4j-spark-connector"
              publish = true
              use = true
              publishOnlyChanged = true
              rules = ".m2/repository"
            }
          }

          requirements { runOnLinux(LinuxSize.SMALL) }
        },
    )
