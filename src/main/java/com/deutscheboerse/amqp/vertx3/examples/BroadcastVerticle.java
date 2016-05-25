package com.deutscheboerse.amqp.vertx3.examples;

import io.vertx.core.*;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.message.Message;

/**
 * Created by schojak on 23.5.16.
 */
public class BroadcastVerticle extends AbstractVerticle {
    final static private Logger LOG = LoggerFactory.getLogger(BroadcastVerticle.class);

    private JDBCClient jdbc;
    private ProtonClient proton;
    private ProtonConnection protonConnection;

    @Override
    public void start(Future<Void> fut) {
        jdbc = JDBCClient.createShared(vertx, new JsonObject().put("url", "jdbc:hsqldb:mem:db/code-examples").put("drive_class", "org.hsqldb.jdbcDriver"), "Code-Examples");
        proton = ProtonClient.create(vertx);

        startDb(
                (connection) -> initDb(
                        connection,
                        (nothing) -> startWebApp(
                                (http) -> completeWebServerStartup(
                                        http,
                                        (nothing2) -> startAmqp(fut),
                                        fut),
                                fut),
                        fut),
                fut);
    }

    @Override
    public void stop() throws Exception {
        jdbc.close();
        protonConnection.close();
    }

    private void startDb(Handler<AsyncResult<SQLConnection>> next, Future<Void> fut) {
        jdbc.getConnection(ar -> {
            if (ar.failed()) {
                fut.fail(ar.cause());
            } else {
                next.handle(Future.succeededFuture(ar.result()));
            }
        });
    }

    private void initDb(AsyncResult<SQLConnection> result, Handler<AsyncResult<Void>> next, Future<Void> fut) {
        if (result.failed()) {
            fut.fail(result.cause());
        } else {
            SQLConnection jdbcConnection = result.result();
            jdbcConnection.execute(
                    "CREATE TABLE IF NOT EXISTS Messages (id INTEGER IDENTITY, source VARCHAR(100), messageId VARCHAR(100), correlationId VARCHAR(100), subject VARCHAR(255), body LONGVARCHAR)",
                    ar -> {
                        if (ar.failed()) {
                            fut.fail(ar.cause());
                            jdbcConnection.close();
                            return;
                        }
                        jdbcConnection.query("SELECT * FROM Messages", select -> {
                            if (select.failed()) {
                                fut.fail(ar.cause());
                                jdbcConnection.close();
                                return;
                            }

                            next.handle(fut);
                            jdbcConnection.close();
                        });

                    });
        }
    }

    private void startWebApp(Handler<AsyncResult<HttpServer>> next, Future<Void> fut) {
        Router router = Router.router(vertx);

        router.route("/api/v1*").handler(BodyHandler.create());
        /*router.get("/api/v1/broadcast").handler(this::broadcast);
        router.get("/api/v1/receive/:queueName").handler(this::receive);
        router.post("/api/v1/requestResponse").handler(this::requestResponse);*/
        router.post("/api/v1/subscribe").handler(this::subscribe);
        router.get("/api/v1/messages").handler(this::messages);
        router.get("/api/v1/messages/:queueName").handler(this::messagesByQueue);
        router.get("/api/v1/messages/:queueName/:correlationId").handler(this::messagesByQueueWithCorrelationId);
        router.post("/api/v1/request").handler(this::request);

        int port = config().getInteger("http.port", 8080);
        LOG.info("Starting web server on port " + port);

        // Create the HTTP server and pass the "accept" method to the request handler.
        vertx.createHttpServer()
                .requestHandler(router::accept)
                .listen(
                        port,
                        next::handle
                );
    }

    private void completeWebServerStartup(AsyncResult<HttpServer> http, Handler<AsyncResult<Void>> next, Future<Void> fut) {
        if (http.succeeded()) {
            LOG.info("Webserver successfully started");
            next.handle(fut);
        } else {
            LOG.error("Webserver failed to start " + http.cause());
            fut.fail(http.cause());
        }
    }

    private void startAmqp(Future<Void> fut) {
        LOG.info("Opening connection to admin/admin@eclbgc01:20707");
        proton.connect("eclbgc01.xeop.de", 20707, "admin", "admin", connectResult -> {
            if (connectResult.succeeded()) {
                connectResult.result().setContainer("example-container/code-examples").openHandler(openResult -> {
                    if (openResult.succeeded()) {
                        protonConnection = openResult.result();
                        fut.complete();
                    }
                    else {
                        LOG.error("AMQP Connection faield ", openResult.cause());
                        fut.fail(openResult.cause());
                    }
                }).open();
            }
            else {
                LOG.error("AMQP Connection failed ", connectResult.cause());
                fut.fail(connectResult.cause());
            }
        });
    }

    private void subscribe(RoutingContext routingContext) {

        Queue q = Json.decodeValue(routingContext.getBodyAsString(), Queue.class);

        LOG.info("Received subscribe request for queue " + q.getName());

        ProtonReceiver protonReceiver = protonConnection.createReceiver(q.getName()).setPrefetch(1000).setAutoAccept(false).handler((delivery, msg) -> {
            storeBroadcast(q.getName(), msg, (r) -> {
                        if (r.succeeded())
                        {
                            delivery.settle();
                        }
                        else
                        {
                            delivery.disposition(new Released(), true);
                        }
                    }
            );
        }).openHandler(protonReceiverAsyncResult -> {
            if (protonReceiverAsyncResult.succeeded())
            {
                LOG.info("Subscribe to queue " + q.getName());
                routingContext.response().setStatusCode(200)
                        .end();
            }
            else
            {
                LOG.error("Failed to subscribe to queue " + q.getName() + ": " + protonReceiverAsyncResult.cause());
                routingContext.response().setStatusCode(500)
                        .end();
            }
        }).open();
    }

    private void storeBroadcast(String sourceQueue, Message msg, Handler<AsyncResult<Void>> next) {
        MyMessage myMsg = MyMessage.createFromProtonMessage(msg);

        String sql = "INSERT INTO Messages (source, messageId, correlationId, subject, body) VALUES ?, ?, ?, ?, ?";

        jdbc.getConnection(ar -> {
            SQLConnection connection = ar.result();
            connection.updateWithParams(sql,
                    new JsonArray().add(sourceQueue).add(myMsg.getMessageId()).add(myMsg.getCorrelationId()).add(myMsg.getSubject()).add(myMsg.getBody()),
                    (ar2) -> {
                        if (ar2.failed()) {
                            LOG.error("Failed to store message into DB " + ar2.cause());
                            next.handle(Future.failedFuture(ar2.cause()));
                            return;
                        }
                        next.handle(Future.succeededFuture());
                    });
        });
    }

    private void messages(RoutingContext routingContext) {
        LOG.info("Received messages request");

        jdbc.getConnection(ar -> {
            SQLConnection connection = ar.result();
            connection.query("SELECT * FROM Messages", result -> {
                routingContext.response()
                        .putHeader("content-type", "application/json; charset=utf-8")
                        .end(Json.encodePrettily(result.result().getRows()));
                connection.close();
            });
        });
    }

    private void messagesByQueue(RoutingContext routingContext) {
        LOG.info("Received broadcast request");

        jdbc.getConnection(ar -> {
            SQLConnection connection = ar.result();
            connection.queryWithParams("SELECT * FROM Messages WHERE source=?", new JsonArray().add(routingContext.request().getParam("queueName")), result -> {
                routingContext.response()
                        .putHeader("content-type", "application/json; charset=utf-8")
                        .end(Json.encodePrettily(result.result().getRows()));
                connection.close();
            });
        });
    }

    private void messagesByQueueWithCorrelationId(RoutingContext routingContext) {
        LOG.info("Received broadcast request");

        jdbc.getConnection(ar -> {
            SQLConnection connection = ar.result();
            connection.queryWithParams("SELECT * FROM Messages WHERE source=? AND correlationId=?", new JsonArray().add(routingContext.request().getParam("queueName")).add(routingContext.request().getParam("correlationId")), result -> {
                routingContext.response()
                        .putHeader("content-type", "application/json; charset=utf-8")
                        .end(Json.encodePrettily(result.result().getRows()));
                connection.close();
            });
        });
    }

    private void request(RoutingContext routingContext) {
        MyMessage m = Json.decodeValue(routingContext.getBodyAsString(), MyMessage.class);

        LOG.info("Received request request" + m.toString());

        protonConnection.createSender("request.ABCFR_ABCFRALMMACC1").openHandler(openResult -> {
            if (openResult.succeeded()) {
                ProtonSender sender = openResult.result();

                Message message = Message.Factory.create();
                message.setReplyTo("response/response.ABCFR_ABCFRALMMACC1");
                message.setBody(new AmqpValue(m.getBody()));
                message.setCorrelationId(m.getCorrelationId());

                sender.send(message, delivery -> {
                    LOG.info("Request message received by server: remote state=" + delivery.getRemoteState() + ", remotely settled=" + delivery.remotelySettled());
                    routingContext.response().setStatusCode(200)
                            .end();
                });

                sender.close();
            }
        }).open();
    }

    /*private void broadcast(RoutingContext routingContext) {
        LOG.info("Received messages request");

        jdbc.getConnection(ar -> {
            SQLConnection connection = ar.result();
            connection.query("SELECT * FROM Messages", result -> {
                routingContext.response()
                        .putHeader("content-type", "application/json; charset=utf-8")
                        .end(Json.encodePrettily(result.result().getRows()));
                connection.close();
            });
        });
    }

    private void receive(RoutingContext routingContext) {
        LOG.info("Received receive request");

        ProtonReceiver receiver = protonConnection.createReceiver(routingContext.request().getParam("queueName")).setAutoAccept(false).setPrefetch(0).open();
        receiver.handler((delivery, msg) -> {
            Section body = msg.getBody();
            String bodyStr = "";

            if (body instanceof AmqpValue) {
                bodyStr = ((AmqpValue)body).getValue().toString();
                LOG.info("Got message with AMQP Value " + ((AmqpValue) body).getValue().toString());
            }
            else if (body instanceof Data)
            {
                bodyStr = ((Data)body).getValue().toString();
                LOG.info("Got message with Data Value " + ((Data) body).getValue().toString());
            }
            else
            {
                bodyStr = body.toString();
                LOG.info("Got message without AMQP Value " + body.toString());
            }

            routingContext.response()
                    .putHeader("content-type", "application/json; charset=utf-8")
                    .end(Json.encodePrettily(bodyStr));

            delivery.settle();

            receiver.close();
        });

        receiver.flow(1);
    }

    private void requestResponse(RoutingContext routingContext) {
        LOG.info("Received requestResponse request");

        ProtonReceiver receiver = protonConnection.createReceiver("response.ABCFR_ABCFRALMMACC1").setAutoAccept(false).setPrefetch(0).open();
        receiver.handler((delivery, msg) -> {
            Section body = msg.getBody();
            String bodyStr = "";

            if (body instanceof AmqpValue) {
                bodyStr = ((AmqpValue)body).getValue().toString();
                LOG.info("Got message with AMQP Value " + ((AmqpValue) body).getValue().toString());
            }
            else if (body instanceof Data)
            {
                bodyStr = ((Data)body).getValue().toString();
                LOG.info("Got message with Data Value " + ((Data) body).getValue().toString());
            }
            else
            {
                bodyStr = body.toString();
                LOG.info("Got message without AMQP Value " + body.toString());
            }

            routingContext.response()
                    .putHeader("content-type", "application/json; charset=utf-8")
                    .end(Json.encodePrettily(bodyStr));

            delivery.settle();

            receiver.close();
        });

        protonConnection.createSender("request.ABCFR_ABCFRALMMACC1").openHandler(openResult -> {
            if (openResult.succeeded()) {
                ProtonSender sender = openResult.result();

                Message message = Message.Factory.create();
                message.setReplyTo("response/response.ABCFR_ABCFRALMMACC1");
                message.setBody(new AmqpValue("Hello World"));
                message.setCorrelationId(UUID.randomUUID().toString());

                sender.send(message, delivery -> {
                    LOG.info("Request message received by server: remote state=" + delivery.getRemoteState() + ", remotely settled=" + delivery.remotelySettled());
                });

                sender.close();
            }
        }).open();

        receiver.flow(1);
    }*/
}
