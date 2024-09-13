package whelk.datatool.form

import groovy.util.logging.Log4j2 as Log
import whelk.component.SparqlQueryClient

@Log
class Selection {
    List<String> recordIds

    Selection(List<String> recordIds) {
        this.recordIds = recordIds
    }

    static Selection byForm(Map form, SparqlQueryClient sparql) {
        return new Selection(sparql.queryIdsByForm(form))
    }

    int size() {
        return recordIds.size()
    }

    boolean isEmpty() {
        return recordIds.isEmpty()
    }
}
