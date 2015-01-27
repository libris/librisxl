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
class ImportOperator extends AbstractOperator {
    String oid = "import"

    String importerPlugin = null
    String serviceUrl = null
    int numToImport = -1
    String resumptionToken = null
    Date since = null
    boolean picky = true

    boolean useWhelkState = false

    Importer importer = null

    int totalCount = 0
    int startAtId = 0

    ImportOperator(Map settings) {
        if (settings) {
            setParameters(settings)
        }
    }

    @Override
    void setParameters(Map parameters) {
        super.setParameters(parameters)
        this.importerPlugin = parameters.get("importer") ?: importerPlugin
        this.serviceUrl = parameters.get("url") ?: serviceUrl
        this.numToImport = parameters.get("nums", -1) as int ?: numToImport
        this.resumptionToken = parameters.get("resumptionToken") ?: resumptionToken
        if (parameters.get("sinceFromWhelkState")) {
            this.useWhelkState = true
        } else if (parameters.get("since")) {
            def dateString = parameters.get("since")
            if (dateString.length() == 10) {
                this.since = Date.parse('yyyy-MM-dd', dateString)
            } else if (dateString.length() > 10) {
                this.since = Date.parse("yyyy-MM-dd'T'HH:mm:ss'Z'", dateString)
            }
        }
        this.startAtId = parameters.get("startAt") as Integer ?: startAtId
    }

    void doRun() {
        assert dataset
        log.trace("Starting ${this.id}. Plugin: $importerPlugin, url: $serviceUrl, dataset: $dataset")
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
        log.trace("Using importer: ${importer.getClass().getName()}")
        if (importer instanceof OaiPmhImporter || importer.getClass().getName() == "whelk.importer.libris.OldOAIPMHImporter") {
            importer.serviceUrl = serviceUrl
            this.totalCount = 0
            for (ds in dataset.split(",")) {
                def whelkState = whelk.loadState()
                if (useWhelkState) {
                    String dString = whelkState.get("oaipmh", [:]).get("lastImport", [:]).get(ds)
                    if (dString) {
                        since = Date.parse("yyyy-MM-dd'T'HH:mm:ss'Z'", dString)
                    } else {
                        since = new Date()
                        def lastWeeksDate = since[Calendar.DATE] - 7
                        since.set(date: lastWeeksDate)
                        log.info("Whelk has no state for last import from $ds. Setting last week (${since})")
                    }
                }
                log.debug("Executing OAIPMH import for $ds since $since from $serviceUrl")
                Date dateStamp = new Date()
                int dsImportCount = importer.doImport(ds, resumptionToken, numToImport, true, picky, since)
                totalCount = totalCount + dsImportCount
                if (dsImportCount > 0) {
                    log.info("Imported $dsImportCount document for $ds.")
                } else {
                    log.debug("Imported $dsImportCount document for $ds.")
                }
                log.trace("Total count is now: $totalCount")
                updateState(whelkState, ds, dateStamp)
            }
        } else {
            if (!serviceUrl) {
                throw new WhelkRuntimeException("URL is required for import.")
            }
            try {
                importer.startAt = startAtId
            } catch (MissingMethodException mme) {
                log.trace("Importer has no startAt parameter.")
            }
            totalCount = importer.doImport(dataset, numToImport, true, picky, new URI(serviceUrl))

        }
        importer = null // Release
    }

    @groovy.transform.Synchronized
    void updateState(whelkState, dataset, dateStamp) {
        whelkState = whelk.loadState()
        whelkState.get("oaipmh", [:]).get("lastImport", [:]).put(dataset, dateStamp.format("yyyy-MM-dd'T'HH:mm:ss'Z'"))
        whelk.saveState(whelkState)

    }


    int getCount() { (importer ? importer.recordCount : totalCount) }

    @Override
    void cancel() {
        this.importer.cancel()
    }
}

