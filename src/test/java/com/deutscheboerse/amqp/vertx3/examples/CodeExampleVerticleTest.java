package com.deutscheboerse.amqp.vertx3.examples;

import com.deutscheboerse.amqp.vertx3.examples.model.MessageModel;
import com.deutscheboerse.amqp.vertx3.examples.model.QueueModel;
import com.deutscheboerse.amqp.vertx3.examples.utils.AutoCloseableConnection;
import com.deutscheboerse.amqp.vertx3.examples.utils.Utils;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.jms.*;
import javax.naming.NamingException;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by schojak on 27.5.16.
 */
@RunWith(VertxUnitRunner.class)
public class CodeExampleVerticleTest {
    public static final String BROADCAST_QUEUE = "broadcast.ABCFR_ABCFRALMMACC1.TradeConfirmation";
    public static final String REQUEST_QUEUE = "request_be.ABCFR_ABCFRALMMACC1";
    public static final String RESPONSE_QUEUE = "response.ABCFR_ABCFRALMMACC1";

    protected Vertx vertx;
    protected int port;
    protected Utils brokerUtils = new Utils();

    @Before
    public void setUp(TestContext context) throws IOException {
        vertx = Vertx.vertx();

        ServerSocket socket = new ServerSocket(0);
        port = socket.getLocalPort();
        socket.close();

        DeploymentOptions options = new DeploymentOptions()
                .setConfig(getConfig());

        vertx.deployVerticle(CodeExampleVerticle.class.getName(), options, context.asyncAssertSuccess());
    }

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void checkRequestResponse(TestContext context) {
        // Start a parallel request responder
        ExecutorService executor = Executors.newFixedThreadPool(1);
        Future<Boolean> future = executor.submit(() -> {
            boolean success = true;
            try (AutoCloseableConnection connection = CodeExampleVerticleTest.this.brokerUtils.getAdminConnection(getConfig().getString("amqp.hostname"), Integer.toString(getConfig().getInteger("amqp.tcpPort"))))
            {
                connection.start();
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageConsumer requestConsumer = session.createConsumer(CodeExampleVerticleTest.this.brokerUtils.getQueue(REQUEST_QUEUE));
                Message requestMessage = requestConsumer.receive(10000);
                String receivedMessageText = ((TextMessage) requestMessage).getText();
                Message responseMessage = session.createTextMessage("Re: " + receivedMessageText);
                responseMessage.setJMSCorrelationID(requestMessage.getJMSCorrelationID());
                MessageProducer responseProducer = session.createProducer(CodeExampleVerticleTest.this.brokerUtils.getQueue(RESPONSE_QUEUE));
                responseProducer.send(responseMessage);
            }
            catch (JMSException | NamingException ex)
            {
                success = false;
            }
            return success;
        });

        // Do the actual test
        Async async = context.async();

        final String subscribePayload = Json.encodePrettily(new QueueModel(RESPONSE_QUEUE));

        MessageModel requestMessage = new MessageModel(UUID.randomUUID().toString(), "123", "request", "Hello World");
        final String requestPayload = Json.encodePrettily(requestMessage);

        vertx.createHttpClient().post(port, "localhost", "/api/subscribe")
                .putHeader("content-type", "application/json")
                .putHeader("content-length", Integer.toString(subscribePayload.length()))
                .handler(response -> {
                    context.assertEquals(response.statusCode(), 201);

                    vertx.createHttpClient().post(port, "localhost", "/api/request")
                            .putHeader("content-type", "application/json")
                            .putHeader("content-length", Integer.toString(requestPayload.length()))
                            .handler(response2 -> {
                                context.assertEquals(response2.statusCode(), 201);

                                try {
                                    Thread.sleep(1000);
                                }
                                catch (InterruptedException e)
                                {
                                    System.out.println(e);
                                }

                                vertx.createHttpClient().getNow(port, "localhost", "/api/messages/" + RESPONSE_QUEUE + "/" + requestMessage.getCorrelationId(), response3 -> {
                                    context.assertEquals(response3.statusCode(), 200);
                                    response3.bodyHandler(body -> {
                                        context.assertTrue(body.toString().contains("Re: Hello World"));
                                        async.complete();
                                    });
                                });
                            })
                            .write(requestPayload)
                            .end();
                })
                .write(subscribePayload)
                .end();

        // Shutdown the executor
        executor.shutdown();
    }

    @Test
    public void checkBroadcastReceiver(TestContext context) throws JMSException, NamingException, InterruptedException {
        // Send a message into the broadcast queue
        try (AutoCloseableConnection connection = this.brokerUtils.getAdminConnection(getConfig().getString("amqp.hostname"), Integer.toString(getConfig().getInteger("amqp.tcpPort"))))
        {
            connection.start();
            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageProducer broadcastProducer = session.createProducer(this.brokerUtils.getQueue(BROADCAST_QUEUE));
            Message broadcastMessage = session.createTextMessage("Hello World");
            broadcastProducer.send(broadcastMessage);
        }

        // Do the actual test
        Async async = context.async();
        final String payload = Json.encodePrettily(new QueueModel(BROADCAST_QUEUE));
        vertx.createHttpClient().post(port, "localhost", "/api/subscribe")
                .putHeader("content-type", "application/json")
                .putHeader("content-length", Integer.toString(payload.length()))
                .handler(response -> {
                    context.assertEquals(response.statusCode(), 201);

                    vertx.createHttpClient().getNow(port, "localhost", "/api/messages/" + BROADCAST_QUEUE, response2 -> {
                        context.assertEquals(response2.statusCode(), 200);
                        response2.bodyHandler(body -> {
                            context.assertTrue(body.toString().contains("Hello World"));

                            async.complete();
                        });
                    });
                })
                .write(payload)
                .end();
    }

    private JsonObject getConfig()
    {
        return new JsonObject()
                .put("http.port", port)
                .put("jdbc.url", "jdbc:hsqldb:mem:db/code-examples")
                .put("jdbc.driver_class", "org.hsqldb.jdbcDriver")
                .put("amqp.hostname", "ecag-fixml-dev1")
                .put("amqp.port", 35671)
                .put("amqp.tcpPort", 35672)
                .put("amqp.serverCert", "./src/main/resources/ecag-fixml-dev1.crt")
                .put("amqp.clientCert", "./src/main/resources/ABCFR_ABCFRALMMACC1.crt")
                .put("amqp.clientKey", "./src/main/resources/ABCFR_ABCFRALMMACC1.pem");
    }
}
