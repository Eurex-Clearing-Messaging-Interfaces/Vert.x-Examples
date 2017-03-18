package com.deutscheboerse.amqp.vertx3.examples;

import com.deutscheboerse.amqp.vertx3.examples.model.MessageModel;
import com.deutscheboerse.amqp.vertx3.examples.model.QueueModel;
import cz.scholz.aliaskeymanager.AliasProvider;
import io.vertx.core.*;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.*;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.proton.*;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.message.Message;

/**
 * Created by schojak on 23.5.16.
 */
public class CodeExampleVerticle extends AbstractVerticle {
    final static private Logger LOG = LoggerFactory.getLogger(CodeExampleVerticle.class);

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

    /*
    Connect to the DB based on the configuration
     */
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

    /*
    Initialize the database - create the table for messages using SQL
     */
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

    /*
    Start the Web server and configure the router
     */
    private void startWebApp(Handler<AsyncResult<HttpServer>> next, Future<Void> fut) {
        Router router = Router.router(vertx);

        router.route("/api/*").handler(BodyHandler.create());
        router.post("/api/subscribe").handler(this::subscribe);
        router.post("/api/request").handler(this::request);
        router.get("/api/messages").handler(this::messages);
        router.get("/api/messages/:queueName").handler(this::messagesByQueue);
        router.get("/api/messages/:queueName/:correlationId").handler(this::messagesByQueueWithCorrelationId);

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

    /*
    Check that the webserver is started
     */
    private void completeWebServerStartup(AsyncResult<HttpServer> http, Handler<AsyncResult<Void>> next, Future<Void> fut) {
        if (http.succeeded()) {
            LOG.info("Webserver successfully started");
            next.handle(fut);
        } else {
            LOG.error("Webserver failed to start " + http.cause());
            fut.fail(http.cause());
        }
    }

    /*
    Open AMQP connection to the broker using OpenSSL
     */
    private void startAmqp(Future<Void> fut) {
        proton = ProtonClient.create(vertx);

        AliasProvider.setAsDefault();

        LOG.info("Opening connection to " + config().getString("amqp.hostname", "localhost") + ":" + config().getInteger("amqp.port", 5671) + " with server certificate " + config().getString("amqp.serverCert") + ", client certificate " + config().getString("amqp.clientCert") + " and client key " +  config().getString("amqp.clientKey"));
        ProtonClientOptions options = new ProtonClientOptions().addEnabledSaslMechanism("EXTERNAL").setIdleTimeout(0).setSsl(true).setPemKeyCertOptions(new PemKeyCertOptions().setCertPath(config().getString("amqp.clientCert")).setKeyPath(config().getString("amqp.clientKey"))).setPemTrustOptions(new PemTrustOptions().addCertPath(config().getString("amqp.serverCert")));

        proton.connect(options, config().getString("amqp.hostname", "localhost"), config().getInteger("amqp.port", 5671), connectResult -> {
            if (connectResult.succeeded()) {
                connectResult.result().setContainer("example-container/code-examples").openHandler(openResult -> {
                    if (openResult.succeeded()) {
                        protonConnection = openResult.result();
                        fut.complete();
                    }
                    else {
                        LOG.error("AMQP Connection failed ", openResult.cause());
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

    /*
    HTTP request for subscribing to a queue - basically opens an AMQP receiver to given queue
     */
    private void subscribe(RoutingContext routingContext) {
        QueueModel q = Json.decodeValue(routingContext.getBodyAsString(), QueueModel.class);

        LOG.info("Received subscribe request for queue " + q.getName());

        ProtonReceiver protonReceiver = protonConnection.createReceiver(q.getName()).setPrefetch(1000).setAutoAccept(false).handler((delivery, msg) -> {
            LOG.info("Received a message from queue " + q.getName());
            storeBroadcast(q.getName(), msg, (r) -> {
                        if (r.succeeded())
                        {
                            LOG.info("Message has been stored - settling");
                            delivery.settle();
                        }
                        else
                        {
                            LOG.warn("Failed to store the message - releasing");
                            delivery.disposition(new Released(), true);
                        }
                    }
            );
        }).openHandler(protonReceiverAsyncResult -> {
            if (protonReceiverAsyncResult.succeeded())
            {
                LOG.info("Subscribed to queue " + q.getName());
                routingContext.response().setStatusCode(201)
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

    /*
    Support method for storing received messages in the SQL database
     */
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

    /*
    HTTP request to show all messages which we received
     */
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

    /*
    HTTP request to show messages only for a single queue
     */
    private void messagesByQueue(RoutingContext routingContext) {
        LOG.info("Received messages request by queue " + routingContext.request().getParam("queueName"));

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

    /*
    HTTP request to show messages from single queue which match specific correlation ID
     */
    private void messagesByQueueWithCorrelationId(RoutingContext routingContext) {
        LOG.info("Received messages request by queue " + routingContext.request().getParam("queueName") + " and correlation ID " + routingContext.request().getParam("correlationId"));

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

    /*
    HTTP request to send AMQP request messages to the broker. It opens the sender, sends a message and closes the sender.
     */
    private void request(RoutingContext routingContext) {
        MessageModel m = Json.decodeValue(routingContext.getBodyAsString(), MessageModel.class);

        LOG.info("Received request request " + m.toString());

        protonConnection.createSender("request.ABCFR_ABCFRALMMACC1").openHandler(openResult -> {
            if (openResult.succeeded()) {
                ProtonSender sender = openResult.result();

                Message message = Message.Factory.create();
                message.setReplyTo("response/response.ABCFR_ABCFRALMMACC1");
                message.setBody(new AmqpValue(m.getBody()));
                message.setCorrelationId(m.getCorrelationId());

                sender.send(message, delivery -> {
                    LOG.info("Request message received by server: remote state=" + delivery.getRemoteState() + ", remotely settled=" + delivery.remotelySettled());
                    routingContext.response().setStatusCode(201)
                            .end();
                });

                sender.close();
            }
        }).open();
    }
}
