<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Welcome to Apache Flume Spring Boot!

Apache Flume is a distributed, reliable, and available service for efficiently
collecting, aggregating, and moving large amounts of log-like data. It has a simple
and flexible architecture based on streaming data flows. It is robust and fault
tolerant with tunable reliability mechanisms and many failover and recovery
mechanisms. The system is centrally managed and allows for intelligent dynamic
management. It uses a simple extensible data model that allows for online
analytic application.

The Apache Flume Spring Boot module provides the minimal framework required to allow 
Apache Flume to be created and configured as a "normal" Spring Boot applications.

Apache Flume Spring Boot is open-sourced under the Apache Software Foundation License v2.0.

## Documentation

Documentation is included in the binary distribution under the docs directory.
In source form, it can be found in the flume-ng-doc directory.

In general, creating a Flume Spring Boot application requires creating a simple 
project that contains the creation of the Sources, Sinks, and Channels with
the Sinks encapsulated in SinkRunners and the Sources in SourceRunners and then
then providing the bootstrap.yml and application.yml files. The Java class 
containing the configuration must be named in the file:
META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports 
where each line in the file should contain the fully qualified class name of the 
class to be autoconfigured.Note that Spring recommends that classes listed for 
AutoConfiguration should NOT specify packages for component scanning. Instead, 
the specific classes should be specified with @Import or they could also 
be added the the AutoConfiguration import file.

The Flume 2.x guide and FAQ are available here:

* https://cwiki.apache.org/FLUME
* https://cwiki.apache.org/confluence/display/FLUME/Getting+Started

## Contact us!

* Mailing lists: https://cwiki.apache.org/confluence/display/FLUME/Mailing+Lists
* Slack channel #flume on https://the-asf.slack.com/

Bug and Issue tracker.

* https://github.com/apache/flume-spring-boot/issues

## Compiling Flume Spring Boot

Compiling Flume Spring Boot requires the following tools:

* Oracle Java JDK 17
* Apache Maven 3.x
