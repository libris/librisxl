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

    public static ConnectionFactory createConnectionFactory() {
        return createConnectionFactory(null)
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

    public static ConnectionFactory createPersistentConnectionFactory() {
        return createPersistentConnectionFactory(null)
    }

    public static ConnectionFactory createPersistentConnectionFactory(String options) {
        // use an unique data directory in target
        String id = 1
        String dir = "target/activemq-data-" + id
        // remove dir so its empty on startup
        FileUtil.removeDir(new File(dir))
        String url = "vm://test-broker-" + id + "?broker.persistent=true&broker.useJmx=false&broker.dataDirectory=" + dir
        if (options != null) {
            url = url + "&" + options
        }
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url)
        // optimize AMQ to be as fast as possible so unit testing is quicker
        connectionFactory.setCopyMessageOnSend(false)
        connectionFactory.setOptimizeAcknowledge(true)
        connectionFactory.setOptimizedMessageDispatch(true)
        connectionFactory.setAlwaysSessionAsync(false)
        return connectionFactory
    }
}
