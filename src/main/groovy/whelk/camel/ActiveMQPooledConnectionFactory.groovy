package whelk.camel

import groovy.util.logging.Slf4j as Log

import java.io.File
import javax.jms.*

import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.pool.PooledConnectionFactory
import org.apache.camel.util.FileUtil

@Log
public final class ActiveMQPooledConnectionFactory {

    private ActiveMQPooledConnectionFactory() {
    }

    public static PooledConnectionFactory createPooledConnectionFactory(String brokerUrl=null, int maxConnections, int maxActive) {
        ConnectionFactory cf = createConnectionFactory(brokerUrl)
        PooledConnectionFactory pooled = new PooledConnectionFactory()
        pooled.setConnectionFactory(cf)
        pooled.setMaxConnections(maxConnections)
        pooled.setMaximumActiveSessionPerConnection(maxActive)
        return pooled
    }

    public static ConnectionFactory createConnectionFactory(String brokerUrl) {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl)
        connectionFactory.setCopyMessageOnSend(false)
        connectionFactory.setOptimizeAcknowledge(true)
        connectionFactory.setOptimizedMessageDispatch(true)
        connectionFactory.setUseAsyncSend(true)
        connectionFactory.setAlwaysSessionAsync(true)
        connectionFactory.setExceptionListener(
            new ExceptionListener() {
                public void onException(JMSException ex) {
                    log.error("ActiveMQ received exception: ${ex.message}", ex)
                    ex.printStackTrace()
                }
            }
        )
        return connectionFactory
    }
}
