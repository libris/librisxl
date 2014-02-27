package se.kb.libris.whelks.api

import groovy.util.logging.Slf4j as Log

import org.restlet.*
import org.restlet.data.*
import org.restlet.resource.*
import org.restlet.representation.*

import org.codehaus.jackson.map.*

import se.kb.libris.whelks.Whelk

@Log
class OperatorRestlet extends BasicWhelkAPI implements RestAPI {

    def pathEnd = "_operations"
    def varPath = false

    String description = "Web based whelk operator"
    String id = "operations"
    ObjectMapper mapper = new ObjectMapper()

    static BenchmarkOperator benchmarkOperator = new BenchmarkOperator()

    void doHandle(Request request, Response response) {
        def req = request.getResourceRef().getQueryAsForm().getValuesMap()
        def operations = []
        if (req.operation == "benchmark" && benchmarkOperator.operatorState == "IDLE") {
            log.debug("Starting benchmark operation")
            benchmarkOperator.whelk = this.whelk
            benchmarkOperator.dataset = req.get("dataset", null)
            log.debug("Thread state: ${benchmarkOperator.currentThread().getState()} - ${benchmarkOperator.operatorState}")
            benchmarkOperator.start()
            response.setStatus(Status.REDIRECTION_SEE_OTHER, "Operation ${req.operation} started.")
            response.setLocationRef(request.getRootRef().toString()+"/"+pathEnd)
        } else if (benchmarkOperator.operatorState == "RUNNING") {
            log.info("Benchmarker already running.")
        }

        operations << benchmarkOperator

        def page = mapper.writeValueAsString(buildPage(operations))

        log.info("Page is $page")

        response.setEntity(page, MediaType.APPLICATION_JSON)
    }

    Map buildPage(operations) {
        def opMap = [:]
        operations.each { op ->
            opMap[op.oid] = op.status
        }
        return ["operations":opMap]
    }
}

enum OperatorState {
    IDLE, RUNNING
}

interface Operator {
    Map getStatus()
}

@Log
class BenchmarkOperator extends Thread implements Operator {

    String oid = "benchmark"
    String dataset
    float velocity = 0.0
    int count = 0
    long runningTime = 0
    String operatorState = OperatorState.IDLE
    Whelk whelk

    @Override
    void run() {
        assert whelk
        operatorState=OperatorState.RUNNING
        velocity = 0.0
        count = 0
        runningTime = 0
        long startTime = System.currentTimeMillis()
        def docs = []
        for (doc in whelk.loadAll(dataset)) {
            docs << doc
            if (++count % 1000 == 0) {
                // Update runningtime every 1000 docs
                runningTime = System.currentTimeMillis() - startTime
                velocity = (count/(runningTime/1000))

                log.debug("Retrieved ${docs.size()} documents from $whelk ... ($count total). Time elapsed: ${runningTime/1000}. Current velocity: $velocity documents / second.")
                docs = []
            }
        }
        runningTime = System.currentTimeMillis() - startTime
        log.debug("$count documents read. Total time elapsed: ${runningTime/1000} seconds.")
        operatorState = OperatorState.IDLE
    }

    Map getStatus() {
        long rt = (runningTime > 0 ? runningTime/1000 : 0)
        if (operatorState == "IDLE") {
            return ["state":operatorState, "lastrun": ["dataset":dataset,"velocity":velocity,"count":count,"runningTime":rt]]
        } else {
            return ["state":operatorState,"dataset":dataset,"velocity":velocity,"count":count,"runningTime":rt]
        }
    }
}
