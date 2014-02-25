riverbed
========

System to create/delete/update storm topology for [bestminr](http://bestminr.com).

Control
---------

* Consume controlling messages from a task queue.
* Each payload should be a string containing a topology id.
* Each message's metadata should include "type" field to indicate the operation.
* The value of "type" field should be one of the following: "new", "update" or "stop".


Prerequisite
------------

* Mysql
* Mongo
* Rabbitmq
* JVM
* leiningen

