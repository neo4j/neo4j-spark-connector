'use strict';

const ospath = require('path')
const fs = require('fs')
const childProcess = require('child_process')

const express = require('express')
const Download = require('bestikk-download')
const download = new Download({})

const root = ospath.join(__dirname, '..')
const linkcheckVersion = 'v2.0.15%2B1'
const linkcheckBinaryPath = ospath.join(root, 'bin', 'linkcheck')

;(async () => {
  // download linkcheck binary
  if (fs.existsSync(linkcheckBinaryPath)) {
    console.log(`${ospath.relative(root, linkcheckBinaryPath)} already exists, skipping download`)
  } else {
    const platform = process.platform
    if (platform === 'darwin' || platform === 'linux') {
      let os
      if (platform === 'darwin') {
        os = 'mac'
      } else {
        os = platform
      }
      await download.getContentFromURL(`https://github.com/filiph/linkcheck/releases/download/${linkcheckVersion}/linkcheck-${os}-x64.exe`, linkcheckBinaryPath)
    } else {
      throw new Error(`The platform ${platform} is not supported!`)
    }
  }

  // chmod +x
  fs.chmodSync(linkcheckBinaryPath, 0o775)

  // use "fork" to spawn a new Node.js process otherwise it creates a deadlock
  const serverProcess = childProcess.fork(ospath.join(root, 'server.js'))
  serverProcess.on('message', async (msg) => {
    if (msg.event === 'started') {
      // server has started!
      try {
        console.log(`${ospath.relative(root, linkcheckBinaryPath)} --skip-file ignore-links.txt :${msg.port}`)
        const result = childProcess.execSync(`${linkcheckBinaryPath} --skip-file ignore-links.txt :${msg.port}`, { cwd: root, stdio: 'inherit' })
        // stop server and exit gracefully
        await new Promise((resolve, reject) => {
          serverProcess.on('message', (msg) => {
            if (msg.event === 'exiting') {
              resolve()
            }
          })
          serverProcess.send({ event: 'exit' })
        })
        process.exit(0)
      } catch (error) {
        if (error.stdout) {
          console.log(error.stdout.toString('utf8'))
        }
        if (error.stderr) {
          console.error(error.stderr.toString('utf8'))
        }
        await new Promise((resolve, reject) => {
          serverProcess.on('message', (msg) => {
            if (msg.event === 'exiting') {
              resolve()
            }
          })
          serverProcess.send({ event: 'exit' })
        })
        process.exit(1)
      }
    }
  })
})()

