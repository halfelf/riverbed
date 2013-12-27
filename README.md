riverbed
========

Webserver for storing/updating/starting storm topology for social data processing.


Control
---------

* Use different HTTP method sending request to `host:port/topology/:task-id` to query/start/update/stop topology.
* query -- `GET`
* generate and run -- `POST`
* update -- `PUT`
* stop and delete -- `DELETE`


Prerequisite
------------

* Mysql
* Mongo
* Rabbitmq
* JVM with Clojure

