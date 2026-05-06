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

          requirements { runOnLinux(LinuxSize.SMALL) }
        },
    )
