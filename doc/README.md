# Local Development

In order to locally preview the Neo4j Connector for Apache Spark documentation built with Antora do the following steps:

- open a terminal window and be sure to be at the root of the project
- run the following command: `cd doc`
- run the following command: `npm install && npm start`
- browse to [localhost:8000](http://localhost:8000)

Now everytime you change one of your `.adoc` files antora will rebuild everything,
and you just need to refresh your page on [localhost:8000](http://localhost:8000)

Note: you may need to configure your ad/tracker blocker (like Ghostery) to not block the load of remote documentation
assets. 
