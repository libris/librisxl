package se.kb.libris.whelks.api

import groovy.util.logging.Slf4j as Log

import javax.servlet.http.*

import org.codehaus.jackson.map.*
import org.codehaus.jackson.map.SerializationConfig.Feature

import java.util.concurrent.*

import se.kb.libris.whelks.*
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.importers.*
import se.kb.libris.whelks.plugin.*

import se.kb.libris.conch.Tools

@Log
class ImportOperator extends AbstractOperator {
    String oid = "import"

    String importerPlugin = null
    String serviceUrl = null
    int numToImport = -1
    String resumptionToken = null
    Date since = null
    boolean picky = true

    Importer importer = null

    long startTime
    int totalCount = 0

    @Override
    void setParameters(Map parameters) {
        super.setParameters(parameters)
        this.importerPlugin = parameters.get("importer", null)?.first()
        this.serviceUrl = parameters.get("url", null)?.first()
        this.numToImport = parameters.get("nums", [-1]).first() as int
        this.resumptionToken = parameters.get("resumptionToken", null)?.first()
        if (parameters.get("since", []).size() > 0) {
            def dateString = parameters.get("since")?.first()
            if (dateString.length() == 10) {
                this.since = Date.parse('yyyy-MM-dd', dateString)
            } else if (dateString.length() > 10) {
                this.since = Date.parse("yyyy-MM-dd'T'HH:mm:ss'Z'", dateString)
            }
        }
    }

    void doRun(long startTime) {
        this.startTime = startTime
        assert dataset
        if (importerPlugin) {
            importer = plugins.find { it instanceof Importer && it.id == importerPlugin }
        } else {
            def importers = plugins.find { it instanceof Importer }
            if (importers.size() > 1) {
                throw new WhelkRuntimeException("Multiple importers available for ${whelk.id}, you need to specify one with the 'importer' parameter.")
            } else {
                try {
                    importer = importers[0]
                } catch (IndexOutOfBoundsException e) { }
            }
        }
        if (!importer) {
            throw new WhelkRuntimeException("Couldn't find any importers working for ${whelk.id} or specified importer \"${importerPlugin}\" is unavailable.")
        }
        log.debug("Importer name: ${importer.getClass().getName()}")
        if (importer instanceof OaiPmhImporter || importer.getClass().getName() == "se.kb.libris.whelks.importers.OldOAIPMHImporter") {
            importer.serviceUrl = serviceUrl
            this.totalCount = 0
            for (ds in dataset.split(",")) {
                log.info("Import from OAIPMH ${ds}")
                totalCount = totalCount + importer.doImport(ds, resumptionToken, numToImport, true, picky, since)
                log.info("Count is now: $totalCount")
            }
        } else {
            if (!serviceUrl) {
                throw new WhelkRuntimeException("URL is required for import.")
            }
            count = importer.doImport(dataset, numToImport, true, picky, new URI(serviceUrl))
        }
        count = totalCount
        runningTime = System.currentTimeMillis() - startTime
        long elapsed = ((System.currentTimeMillis() - startTime) / 1000)
    }

    @Override
    Map getStatus() {
        if (runningTime == 0) {
            runningTime = System.currentTimeMillis() - startTime
        }
        if (operatorState == OperatorState.RUNNING) {
            count = (importer ? importer.recordCount : 0)
        }
        def status = super.getStatus()
        return status
    }

    @Override
    void cancel() {
        this.importer.cancel()
    }
}

