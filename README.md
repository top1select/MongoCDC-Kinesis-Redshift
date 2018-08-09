# Mongo CDC to Redshift by Kinesis
MongoDB Change Data Capture(CDC) to Amazon Redshift using Kinesis.

MongoDB version 3.6 Java 8 AWS Kinesis Redshift

## Introduction

Change streams allow applications to access real-time data changes without the complexity and risk of tailing the oplog. Applications can use change streams to subscribe to all data changes on a single collection, a database, or an entire deployment, and immediately react to them. Because change streams use the aggregation framework, applications can also filter for specific changes or transform the notifications at will.

## Architecture
## Design and main features

### Mocking the MongoDB Java driver

Java unit testing and test driven development of data access objects that wrap the MongoDB Java driver.
