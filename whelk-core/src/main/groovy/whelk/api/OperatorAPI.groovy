package whelk.api

import groovy.util.logging.Slf4j as Log

import javax.servlet.http.*

import org.codehaus.jackson.map.*
import org.codehaus.jackson.map.SerializationConfig.Feature

import java.util.concurrent.*

import whelk.*
import whelk.exception.*
import whelk.importer.*
import whelk.plugin.*

import whelk.util.Tools

@Log
class OperatorAPI extends BasicAPI {

    def pathEnd = "_operations"
    def varPath = false

    String description = "Web based whelk operator"

    ObjectMapper mapper = new ObjectMapper()


    static final OPERATIONS_HTML_FILE = "operator.html"

    Map operators = [:]

    Map configurationSettings

    OperatorAPI(Map settings) {
        this.configurationSettings = settings
    }

    void bootstrap(String initString) {
        plugins.findAll { it instanceof AbstractOperator }.each { op ->
            operators[(op.id[0..-9])] = op
        }
    }

    void doHandle(HttpServletRequest request, HttpServletResponse response, List pathVars) {
        def req = new HashMap(request.parameterMap)
        req.putAll(configurationSettings)
        log.debug("req: $req")
        if (!isBusy() && operators.containsKey(req.operation?.first())) {
            log.debug("Starting ${req.operation.first()} operation")
            def op = operators[req.operation.first()]
            op.whelk = this.whelk
            op.setParameters(req)
            new Thread(op).start()
            response.setStatus(HttpServletResponse.SC_SEE_OTHER)
            response.setHeader("Location", request.getRequestURL().toString())
            return
        }
        if (isBusy() && operators.containsKey(req.cancel?.first())) {
            log.debug("Cancelling operation ${req.cancel.first()}")
            operators[req.cancel.first()].cancel()
        }

        if (isBusy() || req.status) {
            def opMap = [:]
            operators.each { k,v ->
                opMap[k] = v.status
            }
            // Make output pretty
            mapper.enable(Feature.INDENT_OUTPUT)
            def page = mapper.writeValueAsString(["operations":opMap])

            sendResponse(response, page, "application/json")
        } else {
            sendResponse(response, loadPage(), "text/html")
        }
    }

    boolean isBusy() {
        boolean busy = operators.values().any { it.operatorState == OperatorState.RUNNING }
        return busy
    }

    String loadPage() {
        try {
            return getClass().classLoader.getResourceAsStream(OPERATIONS_HTML_FILE).text
        } catch (NullPointerException npe) {
            throw new WhelkRuntimeException("Couldn't find file $OPERATIONS_HTML_FILE.")
        }
    }
}

/*
@Log
@Deprecated
class BenchmarkOperator extends AbstractOperator {

    String oid = "benchmark"
    Date since = null
    String fromStorage = null
    boolean withSerialization = false
    boolean showSpinner = false

    @Override
    void setParameters(Map parameters) {
        super.setParameters(parameters)
        if (parameters.get("since", []) && parameters.get("since", []).first().length() > 0) {
            this.since = Date.parse("yyyy-MM-dd'T'hh:mm:ss'Z'", parameters.get("since").first())
            log.info("Since: $since")
        }
        this.fromStorage = parameters.get("fromStorage", null)?.first()
        if (fromStorage == "") {
            fromStorage = null
        }
        this.showSpinner = parameters.get("showSpinner", false)
    }

    void doRunBenchmark(long startTime) {
        def storage = this.whelk.getStorage()
        log.info("Starting benchmark method on ${storage.id}")
        storage.benchmark()
    }

    void doRun(long startTime) {
        for (doc in whelk.loadAll(dataset, since, fromStorage)) {
            if (doc) {
                if (count == 0) {
                    println "First document received at ${new Date()}"
                }
                count++
                runningTime = System.currentTimeMillis() - startTime
                if (withSerialization) {
                    doc.getData()
                }
            }
            if (showSpinner) {
                def velocityMsg = "Current velocity: ${count/(runningTime/1000)}."
                Tools.printSpinner("Benchmarking from ${fromStorage ?: "primary storage"}. ${count} documents read sofar (${new Date()}). $velocityMsg", count)
            }
            if (cancelled) {
                break
            }
        }
        runningTime = System.currentTimeMillis() - startTime
        log.info("$count documents read. Total time elapsed: ${runningTime/1000} seconds.")
    }

    @Override
    Map getStatus() {
        def map = super.getStatus()
        if (hasRun) {
            map.get("lastrun",[:]).put("fromStorage", fromStorage)
            map.get("lastrun").put("withSerialization", withSerialization)
        } else if (operatorState == OperatorState.RUNNING) {
            map.put("fromStorage", fromStorage)
            map.put("withSerialization", withSerialization)
        }
        return map
    }
}
*/

abstract class AbstractOperator extends BasicPlugin implements Runnable {
    abstract String getOid()
    String dataset
    int count = 0
    long runningTime = 0
    OperatorState operatorState = OperatorState.IDLE
    Whelk whelk
    boolean hasRun = false
    boolean cancelled = false
    List<String> errorMessages

    void setParameters(Map parameters) {
        this.dataset = parameters.get("dataset", null)?.first()
        if (this.dataset?.length() == 0) {
            this.dataset = null
        }
    }

    @Override
    void run() {
        assert whelk
        try {
            log.debug("Starting operation")
            operatorState=OperatorState.RUNNING
            cancelled = false
            count = 0
            runningTime = 0
            errorMessages = []
            doRun(System.currentTimeMillis())
        } finally {
            operatorState=OperatorState.IDLE
            hasRun = true
        }
    }

    abstract void doRun(long startTime);

    Map getStatus() {
        double rt = (runningTime > 0 ? runningTime/1000 : 0.0)
        float velocity = 0.0
        if (rt > 0) {
            velocity = count/rt
        }
        if (operatorState == OperatorState.IDLE) {
            def map = ["state":operatorState]
            if (hasRun) {
                map.put("lastrun", ["dataset":dataset,"average_velocity":(velocity > 0 ? velocity : "unlimited"),"count":count,"runningTime":rt])
                if (errorMessages) {
                    map.get("lastrun").put("errors",errorMessages)
                }
                if (cancelled) {
                    map.get("lastrun").put("cancelled", true)
                }
            }
            return map
        } else {
            return ["state":operatorState,"dataset":dataset,"velocity":velocity,"count":count,"runningTime":rt, "errors":errorMessages]
        }
    }

    void cancel() {
        this.cancelled = true
    }
}


enum OperatorState {
    IDLE, RUNNING, FINISHING
}
