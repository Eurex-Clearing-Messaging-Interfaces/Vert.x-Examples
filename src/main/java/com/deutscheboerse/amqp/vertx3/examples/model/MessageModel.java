package com.deutscheboerse.amqp.vertx3.examples.model;

import com.deutscheboerse.amqp.vertx3.examples.CodeExampleVerticle;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;

/**
 * Class used to map AMQP messages between AMQP broker, database and JSON
 */
public class MessageModel {
    final static private Logger LOG = LoggerFactory.getLogger(CodeExampleVerticle.class);

    private String messageId = null;
    private String correlationId = null;
    private String subject = null;
    private String body = null;

    public static MessageModel createFromProtonMessage(org.apache.qpid.proton.message.Message msg)
    {
        MessageModel newMsg = new MessageModel();
        newMsg.setCorrelationId(msg.getCorrelationId() != null ? msg.getCorrelationId().toString() : "");
        newMsg.setMessageId(msg.getMessageId() != null ? msg.getMessageId().toString() : "");
        newMsg.setSubject(msg.getSubject() != null ? msg.getSubject() : "");

        Section body = msg.getBody();

        if (body instanceof AmqpValue) {
            newMsg.setBody(((AmqpValue)body).getValue().toString());
            LOG.trace("MessageModel with AMQP Value " + ((AmqpValue) body).getValue().toString());
        }
        else if (body instanceof Data)
        {
            newMsg.setBody(((Data)body).getValue().toString());
            LOG.trace("MessageModel with Data Value " + ((Data) body).getValue().toString());
        }
        else
        {
            newMsg.setBody(body.toString());
            LOG.trace("MessageModel without AMQP Value " + body.toString());
        }

        return newMsg;
    }

    public MessageModel() {
        return;
    }

    public MessageModel(String messageId, String correlationId, String subject, String body) {
        this.messageId = messageId;
        this.correlationId = correlationId;
        this.subject = subject;
        this.body = body;
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
