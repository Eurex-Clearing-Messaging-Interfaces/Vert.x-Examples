[![Build Status](https://travis-ci.org/Eurex-Clearing-Messaging-Interfaces/Vert.x-Examples.svg?branch=master)](https://travis-ci.org/Eurex-Clearing-Messaging-Interfaces/Vert.x-Examples) [![CircleCI](https://circleci.com/gh/Eurex-Clearing-Messaging-Interfaces/Vert.x-Examples.svg?style=svg)](https://circleci.com/gh/Eurex-Clearing-Messaging-Interfaces/Vert.x-Examples)

# Vert.x-Examples

These examples show how to connect to Eurex Clearing Messaging Interfaces using the Vert.x 3 toolkit. More information about Vert.x can be found on its [website](http://vertx.io/). The examples are designed as a simple REST based application. They use the vertx-proton extension to communicate with the AMQP broker using AMQP 1.0. More info about vertx-proton can be found on its [GitHub project](https://github.com/vert-x3/vertx-proton). The examples also uses HSQL database over JDBC interface to store the messages.

_**Note: The examples are currently based on the latest development version of Vert.x (3.3.0-SANPSHOT). This is because of the OpenSSL support which will be added only in 3.3.0 release and is not available in the latest stable release (3.2.1).**_

## Configuration

The application can be configured using a JSON file which specifies
- Connection details for the AMQP broker
- HTTP port where the REST interface should run
- URL where the database should run / how it should be configured

Example configuration file can be found in `./src/main/resources/config.json`.

## Application start

The application can be build using `mvn package` command and started as a regular Java application:
`https://github.com/vert-x3/vertx-proton`

After the start, the application will
1) Start the webserver
2) Initialize the JDBS connection to the SQL database and create the database structure
3) Connect to the AMQP broker using AMQP 1.0, SASL EXTERNAL and SSL Client Authentication

## Rest interface

The REST interface is used for a basic control of the application.

### Subscribing to messages

In order to receive any messages from the AMQP broker, you have to create a receiver. That can be done using following HTTP POST request sent to the path `/api/subscribe`:
```
curl -X POST http://localhost:8080/api/subscribe -d "{\"name\":\"broadcast.ABCFR_ABCFRALMMACC1.TradeConfirmationNCM\"}"
```

In the same way you can also subscribe to the response queue `response.ABCFR_ABCFRALMMACC1`:
```
curl -X POST http://localhost:8080/api/subscribe -d "{\"name\":\"response.ABCFR_ABCFRALMMACC1\"}"
```

### Sending requests

Request messages can be sent using HTTP POST request sent to /api/request. The body of the request is a JSON encoded map with the message details. For example:

```
curl -X POST http://localhost:8080/api/request -d "{\"correlationId\":\"myCorrId\",\"body\":\"Hello World\"}"
```

### Reading Messages

Messages which were received by the application are stored in the database. They can be obtained using HTTP GET request. Reqeust sent to `/api/messages` will return JSON formated list of all messages. Request sent to `/api/messages/<queueNeme>` will return only messages received from the given queue. And request sent to `/api/messages/<queueNeme>/<correlationID>` will return only messages received from the given queue matching the specific correlation ID.
```
curl -X GET http://localhost:8080/api/messages
curl -X GET http://localhost:8080/api/messages/response.ABCFR_ABCFRALMMACC1
curl -X GET http://localhost:8080/api/messages/response.ABCFR_ABCFRALMMACC1/myCorrId
```

## SSL

In order to connect to the Eurex brokers using SSL Client Authentication, the OpenSSL library has to be used by the Vert.x toolkit. With the Java SSL implementation, the SSL authentication does not work at this moment.

## Integration tests

The project is using Travis-CI to run its own integration tests. The tests are executed using Docker images which contain the AMQP broker with configuration corresponding to Eurex Clearing FIXML Interface as well as the Dispatch router. The details of the Travis-CI integration can be found in the .travis.yml file.

## Documentation

More details about Eurex Clearing Messaging Interfaces can be found on [Eurex Clearing website](http://www.eurexclearing.com/clearing-en/technology/eurex-release14/system-documentation/system-documentation/861464?frag=861450)
