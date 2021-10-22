const express = require('express')

const app = express()
app.use(express.static('./build/site'))

app.use('/static/assets', express.static('./build/site/developer/_'))

app.get('/', (req, res) => res.redirect('/developer/spark'))

app.listen(8000, () => console.log('📘 http://0.0.0.0:8000'))
