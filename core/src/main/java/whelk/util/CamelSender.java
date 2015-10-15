package whelk.util;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.io.Serializable;

/**
 A lightweight way of sending (object) messages to camel via ActiveMQ.
 Use like so:

 ---------------------------------------------------------------------
 CamelSender sender = new CamelSender(
 "tcp://localhost:61616?wireFormat.maxInactivityDuration=3600000",
 "test.queue"
 );

 // A message of some sort
 HashMap<String, String> complicatedMessage = new HashMap<>();
 complicatedMessage.put("key", "value");
 complicatedMessage.put("other key", "other value");

 sender.sendObjectMessage(complicatedMessage);

 sender.close();
 ---------------------------------------------------------------------
 */

public class CamelSender
{
    private Connection m_connection;
    private Session m_session;
    private MessageProducer m_producer;

    /**
     * @param connectionString Address string of activeMQ broker, for example: "tcp://localhost:61616"
     * @param queueName The name of the queue in which to place messages, for example "myqueue.queue"
     * @throws Exception
     */
    public CamelSender(String connectionString, String queueName) throws Exception
    {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(connectionString);
        m_connection = connectionFactory.createConnection();
        m_connection.start();

        /*
        Not using transaction mode. See: http://activemq.apache.org/how-do-transactions-work.html
         */
        m_session = m_connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = m_session.createQueue(queueName);
        m_producer = m_session.createProducer(destination);

        /*
        Use NON_PERSISTENT mode. Persistent mode means the ActiveMQ broker
        must write passing messages to disk, in order to survive crashes/restarts.
        See: http://activemq.apache.org/what-is-the-difference-between-persistent-and-non-persistent-delivery.html
         */
        m_producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
    }

    public void close() throws JMSException
    {
        m_session.close();
        m_connection.close();
    }

    public void sendObjectMessage(Serializable payload) throws JMSException
    {
        ObjectMessage message = m_session.createObjectMessage(payload);
        m_producer.send(message);
    }
}
