package whelk.component

import org.apache.commons.codec.binary.Base64

import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.*
import org.elasticsearch.common.transport.*
import org.elasticsearch.common.settings.*
import org.elasticsearch.action.delete.*
import org.elasticsearch.action.admin.indices.flush.*
import org.elasticsearch.action.admin.indices.alias.get.*

import whelk.exception.*

abstract class BasicElasticComponent extends BasicComponent implements ShapeComputer {

    BasicElasticComponent() {
        super()
        connectClient()
    }
    BasicElasticComponent(Map settings) {
        super()
    }


    /*
    String getRealIndexFor(String alias) {
        def aliases = performExecute(client.admin().cluster().prepareState()).state.metaData.aliases()
        log.trace("aliases: $aliases")
        def ri = null
        if (aliases.containsKey(alias)) {
            ri = aliases.get(alias)?.keys().iterator().next()
        }
        if (ri) {
            log.trace("ri: ${ri.value} (${ri.value.getClass().getName()})")
        }
        return (ri ? ri.value : alias)
    }
    */


    /*
    void index(final List<Map<String,String>> data) throws WhelkIndexException  {
        def breq = client.prepareBulk()
        for (entry in data) {
            breq.add(client.prepareIndex(entry['index'], entry['type'], entry['id']).setSource(entry['data'].getBytes("utf-8")))
        }
        def response = performExecute(breq)
        if (response.hasFailures()) {
            log.error "Bulk entry indexing has failures."
            def fails = []
            for (re in response.items) {
                if (re.failed) {
                    log.error "Fail message for id ${re.id}, type: ${re.type}, index: ${re.index}: ${re.failureMessage}"
                    try {
                        fails << fromElasticId(re.id)
                    } catch (Exception e1) {
                        log.error("TranslateIndexIdTo cast an exception", e1)
                        fails << "Failed translation for \"$re\""
                    }
                }
            }
            throw new WhelkIndexException("Failed to index entries. Reason: ${response.buildFailureMessage()}", new WhelkAddException(fails))
        } else {
            log.debug("Direct bulk request completed in ${response.tookInMillis} millseconds.")
        }
    }

    void index(byte[] data, Map params) throws WhelkIndexException  {
        try {
            def response = performExecute(client.prepareIndex(params['index'], params['type'], params['id']).setSource(data))
            log.debug("Raw byte indexer (${params.index}/${params.type}/${params.id}) indexed version: ${response.version}")
        } catch (Exception e) {
            throw new WhelkIndexException("Failed to index ${new String(data)} with params $params", e)
        }
    }

    void deleteEntry(String identifier, indexName, indexType) {
        def response = performExecute(client.prepareDelete(indexName, indexType, toElasticId(identifier)))
        log.debug("Deleted ${response.id} with type ${response.type} from ${response.index}. Document found: ${response.found}")
    }


    */

}
