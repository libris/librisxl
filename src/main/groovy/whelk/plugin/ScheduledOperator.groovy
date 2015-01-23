package whelk.plugin

import groovy.util.logging.Slf4j as Log

import java.util.concurrent.*

@Log
class ScheduledOperator extends BasicPlugin {

    String description = "Scheduled operator runner."

    ScheduledExecutorService ses

    Map configuration

    ScheduledOperator(Map settings) {
        this.configuration = settings
    }

    void bootstrap() {
        ses = Executors.newScheduledThreadPool(configuration.size())
        configuration.each { task, conf ->
            log.info("Setting up schedule for $task : $conf")
            def op = getPlugin(conf.operator)
            assert op
            conf.put("sinceFromWhelkState", true)
            log.info("setting params: $conf")
            op.setParameters(conf)
            log.info("settings: ${op.dataset}, ${op.serviceUrl}")
            ses.scheduleAtFixedRate(op, 3, conf.interval, TimeUnit.SECONDS)
            log.info("${op.id} should start in 3 seconds.")
        }
        log.info("Bootstrap finished.")
    }
}
