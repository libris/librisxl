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
            try {
                ses.scheduleWithFixedDelay(op, 30, conf.interval, TimeUnit.SECONDS)
                log.info("${op.id} will start in 30 seconds.")
            } catch (RejectedExecutionException ree) {
                log.error("execution failed", ree)
            }
        }
    }
}
