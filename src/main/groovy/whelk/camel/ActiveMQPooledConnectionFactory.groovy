package whelk.camel

import java.io.File
import javax.jms.ConnectionFactory
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.pool.PooledConnectionFactory
import org.apache.camel.util.FileUtil

public final class ActiveMQPooledConnectionFactory {

    private ActiveMQPooledConnectionFactory() {
    }

    public static PooledConnectionFactory createPooledConnectionFactory(String brokerUrl=null) {
        ConnectionFactory cf = createConnectionFactory(brokerUrl)
        PooledConnectionFactory pooled = new PooledConnectionFactory()
        pooled.setConnectionFactory(cf)
        pooled.setMaxConnections(8)
        return pooled
    }

    public static ConnectionFactory createConnectionFactory(String brokerUrl) {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl)
        connectionFactory.setCopyMessageOnSend(false)
        connectionFactory.setOptimizeAcknowledge(true)
        connectionFactory.setOptimizedMessageDispatch(true)
        connectionFactory.setUseAsyncSend(true)
        connectionFactory.setAlwaysSessionAsync(true)
        return connectionFactory
    }
}
