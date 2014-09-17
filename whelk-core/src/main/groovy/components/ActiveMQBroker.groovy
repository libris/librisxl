package se.kb.libris.whelks.component.support
import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.plugin.*

import org.apache.activemq.broker.BrokerFactory
import org.apache.activemq.broker.BrokerService

@Log
class ActiveMQBroker extends BasicPlugin {

    ActiveMQBroker() {
        log.info("Starting an ActiveMQ broker.")
        BrokerService broker = BrokerFactory.createBroker("broker:(tcp://localhost:61616)?persistent=false&useJmx=true", true)
    }
}

