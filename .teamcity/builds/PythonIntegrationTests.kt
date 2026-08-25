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

          steps {
            if (neo4jVersion != Neo4jVersion.V_NONE) {
              pullImage(neo4jVersion)
            }

            script {
              scriptContent =
                  """
              #!/bin/bash -eu
              mise use -g python@${pythonVersion.version}

              python -m pip install --upgrade pip

              # pipelines is a 4.1+ feature
              if [[ "${sparkVersion.version}" != 4.0.* ]]; then
                pip install pyspark[pipelines]==${sparkVersion.version} "testcontainers[neo4j]" six tzlocal==2.1
              else
                pip install pyspark==${sparkVersion.version} "testcontainers[neo4j]" six tzlocal==2.1
              fi

              project_version="$(./mvnw help:evaluate -Dexpression="project.version" --quiet -DforceStdout)"
              jar_name="neo4j-spark-connector-${'$'}{project_version}-s_${scalaVersion.version}.jar"

              cd ./scripts/python

              # common integration tests:
              python test_spark.py "${'$'}{jar_name}" "${neo4jVersion.dockerImage}"

              # spark declarative pipeline integration test:
              if [[ "${sparkVersion.version}" != 4.0.* ]]; then
                python pipeline/test_spark_declarative_pipeline.py "${'$'}{jar_name}" "${neo4jVersion.dockerImage}"
              fi
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
