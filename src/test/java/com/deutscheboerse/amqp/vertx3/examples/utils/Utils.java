package com.deutscheboerse.amqp.vertx3.examples.utils;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

public class Utils
{
    public AutoCloseableConnection getAdminConnection(String hostname, String port) throws JMSException, NamingException
    {
        return new ConnectionBuilder().hostname(hostname).port(port).build();
    }

    public Queue getQueue(String queueName) throws NamingException
    {
        Properties props = new Properties();
        props.setProperty("java.naming.factory.initial", "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
        props.setProperty("queue.queue", queueName);
        InitialContext ctx = new InitialContext(props);
        return (Queue) ctx.lookup("queue");
    }

    public Topic getTopic(String queueName) throws NamingException
    {
        Properties props = new Properties();
        props.setProperty("java.naming.factory.initial", "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
        props.setProperty("topic.topic", queueName);
        InitialContext ctx = new InitialContext(props);
        return (Topic) ctx.lookup("topic");
    }
}
