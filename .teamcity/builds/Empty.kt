package builds

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.toId

class Empty(id: String, name: String) :
    BuildType({
      this.id(id.toId())
      this.name = name

      requirements { runOnLinux(LinuxSize.SMALL) }
    })
