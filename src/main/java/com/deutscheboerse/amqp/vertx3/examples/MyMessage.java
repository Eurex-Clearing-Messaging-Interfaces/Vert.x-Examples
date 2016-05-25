package com.deutscheboerse.amqp.vertx3.examples;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;

/**
 * Created by schojak on 24.5.16.
 */
public class MyMessage {
    final static private Logger LOG = LoggerFactory.getLogger(BroadcastVerticle.class);

    private String messageId = null;
    private String correlationId = null;
    private String subject = null;
    private String body = null;

    public static MyMessage createFromProtonMessage(org.apache.qpid.proton.message.Message msg)
    {
        MyMessage newMsg = new MyMessage();
        newMsg.setCorrelationId(msg.getCorrelationId() != null ? msg.getCorrelationId().toString() : "");
        newMsg.setMessageId(msg.getMessageId() != null ? msg.getMessageId().toString() : "");
        newMsg.setSubject(msg.getSubject() != null ? msg.getSubject() : "");

        Section body = msg.getBody();

        if (body instanceof AmqpValue) {
            newMsg.setBody(((AmqpValue)body).getValue().toString());
            LOG.trace("MyMessage with AMQP Value " + ((AmqpValue) body).getValue().toString());
        }
        else if (body instanceof Data)
        {
            newMsg.setBody(((Data)body).getValue().toString());
            LOG.trace("MyMessage with Data Value " + ((Data) body).getValue().toString());
        }
        else
        {
            newMsg.setBody(body.toString());
            LOG.trace("MyMessage without AMQP Value " + body.toString());
        }

        return newMsg;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }
}
