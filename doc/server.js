'use strict'

const express = require('express')
const portfinder = require('portfinder')
const process = require('process')

process.on('message', (msg) => {
  if (msg.event === 'exit') {
    process.send({ event: 'exiting' })
    process.exit(0)
  }
})

;(async () => {
  portfinder.basePort = 8000
  portfinder.getPort((err, port) => {
    if (err) {
      reject(err)
    }
    const app = express()
    app.use(express.static('./build/site'))
    app.use('/static/assets', express.static('./build/site/developer/_'))
    app.get('/', (req, res) => res.redirect('/developer/spark'))
    app.listen(port, () => {
      console.log(`📘 http://localhost:${port}`)
      if (typeof process.send === 'function') {
        process.send({ event: 'started', port: port });
      }
    })
  })
})()
