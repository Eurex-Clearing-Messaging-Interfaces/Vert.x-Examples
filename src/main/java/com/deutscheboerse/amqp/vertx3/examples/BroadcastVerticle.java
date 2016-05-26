package com.deutscheboerse.amqp.vertx3.examples;

import com.deutscheboerse.amqp.vertx3.examples.model.MessageModel;
import com.deutscheboerse.amqp.vertx3.examples.model.QueueModel;
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
        LOG.info("Connecting to JDBC database on URL " + config().getString("jdbc.url") + " with driver class " + config().getString("jdbc.driver_class"));
        jdbc = JDBCClient.createShared(vertx, new JsonObject().put("url", config().getString("jdbc.url")).put("driver_class", config().getString("jdbc.driver_class")), "Code-Examples");

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
        proton = ProtonClient.create(vertx);

        LOG.info("Opening connection to " + config().getString("amqp.username") + "/" + config().getString("amqp.password") + "@" + config().getString("amqp.hostname", "localhost") + ":" + config().getInteger("amqp.port", 5671));

        proton.connect(config().getString("amqp.hostname", "localhost"), config().getInteger("amqp.port", 5671), config().getString("amqp.username"), config().getString("amqp.password"), connectResult -> {
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

        QueueModel q = Json.decodeValue(routingContext.getBodyAsString(), QueueModel.class);

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
        MessageModel myMsg = MessageModel.createFromProtonMessage(msg);

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
        MessageModel m = Json.decodeValue(routingContext.getBodyAsString(), MessageModel.class);

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
}
