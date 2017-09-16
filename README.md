# Welcome!
This repository includes several examples of performing streaming computation with Flink and __storing result to C*__.

Any comments, feedback or _pull requests_ are welcome!

## Motivation
A short description of the motivation behind the creation and maintenance of the project. As this project was created, as of Flink version 1.4.0, there are plenty streaming examples and a few simple C* connector examples. However, there are currently no concrete examples that demonstrates on storing streaming results to C* using `flink-connector-cassandra` library.

## Included Examples
1. The streaming examples could be found at `org.apache.flink.streaming.connectors.cassandra.example.streaming`.
2. The associated CQL entity mapping is located at `org.apache.flink.streaming.connectors.cassandra.example.datamodel`

### Examples Overview
Here is the quick overview for each examples

| Name          | Computation Type | Data Type  |  CQL Entity   |
| ------------- |:----------------:| :---------:| :------------: |
| WikiAnalysis  | Streaming        | POJO       | WikiEditRecord |
| FileWordCount | Streaming        | Tuple      | WordCount      |

### Description

#### WikiAnalysis
`org.apache.flink.streaming.connectors.cassandra.example.streaming.pojo.wiki.WikiAnalysis`

An implementation to stream all edited record from Wikipedia to Flink and count the number of bytes that each user edits within a given window of time
This example utilizes POJO data type to perform the stream computation and store the result back to C* with CQL entity `WikiEditRecord`

#### FileWordCount
`org.apache.flink.streaming.connectors.cassandra.example.streaming.tuple.wordcount.FileWordCount`

A re-implementation of the "WordCount" program that computes a simple word occurrence histogram over text files in a streaming fashion.
This example utilizes Tuple data type to perform the stream computation and store the result back to C* with CQL entity `WordCount`

## Common Services
### Embedded Cassandra Service
A embedded C* service is provided that is convenient for testing and demonstration purposes. This embedded C* instance will live with JVM process and stores data at a directory created randomly upon bootstrap under `/tmp`.

## Library Dependency
Here are the foundemental libraries used in this project, and those information could also be found in `pom.xml`
1. [Apache Flink](https://github.com/apache/flink) - Version 1.4.0
2. [Apache Cassandra](https://github.com/apache/cassandra) - Version 2.2.5
3. [DataStax Java Driver](https://github.com/datastax/java-driver) - Version 3.0.0

## How to Build Examples
Type the following maven command in console shell
1. `mvn clean package` for an executable fat-jar, or
2. `mvn clean package -Pbuild-jar` for an executable uber jar with cleaner dependencies.

## Acknowledgement
This project is based on the [QuickStart Project from Apache Flink Guide](https://ci.apache.org/projects/flink/flink-docs-release-1.4/quickstart/setup_quickstart.html), in which most of libraries dependencies and Maven settings remain. In addition, most of the examples could also be found from [Apache Flink](https://github.com/apache/flink) project (without storing the result to C* sink, of course).


