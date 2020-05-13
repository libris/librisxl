/**
 * Delete duplicate SPI bib records (if other holding than sigel S then just log).
 * Move identifiedBy, technicalNote and hasNote fields from bib to holding for sigel S.
 * Link the S holding to the correct bib record.
 *
 * See LXL-3155 for more information
 */

import java.util.concurrent.atomic.AtomicInteger

class Script {
    static PrintWriter otherHoldingReport
    static PrintWriter deleteReport
    static PrintWriter holdReport
    static PrintWriter failedReport
}

Script.otherHoldingReport = getReportWriter("has-other-holding")
Script.deleteReport = getReportWriter("deleted")
Script.holdReport = getReportWriter("hold-updated")
Script.failedReport = getReportWriter("failed")

TSV = 'SPI-5705-ids.tsv'
new File(scriptDir, TSV).readLines().tail().collect{ it.split("\\t") }.each { columns ->
    try {
        process(getUri(columns[0]).toString(), getUri(columns[1]).toString())
    }
    catch (Exception e) {
        println(e)
        e.printStackTrace()
        Script.failedReport.println("${columns[0]} ${columns[1]} ${e}")
    }
}

void process(String duplicateUri, String keepUri) {
    def identifiedBy
    def hasNote
    selectByIds([duplicateUri]) { bib ->
        identifiedBy = bib.graph[0].remove('identifiedBy')
        bib.graph[0].remove('technicalNote')
        hasNote = bib.graph[1].remove('hasNote')

        if (!hasOtherHolding(duplicateUri)) {
            Script.deleteReport.println(duplicateUri)
            bib.scheduleDelete()
        } else {
            Script.otherHoldingReport.println(duplicateUri)
            bib.scheduleSave()
        }

        boolean found = false
        selectBySqlWhere(whereSHolding(duplicateUri)) { hold ->
            StringBuilder msg = new StringBuilder().append(hold.doc.getURI()).append("\n")
            msg.append("itemOf: $duplicateUri -> $keepUri").append("\n")
            hold.graph[1]['itemOf']['@id'] = keepUri + '#it'
            add(msg, hold.graph[0], 'identifiedBy', identifiedBy)
            add(msg, hold.graph[0], 'cataloguersNote', ['Katalog 56-86, SPI20191219. S bestånd flyttat från SPI-dubblett maskinellt. Fel kan förekomma.'])
            add(msg, hold.graph[1], 'hasNote', hasNote)
            hold.scheduleSave()
            Script.holdReport.println(msg)
            found = true
        }
        if(!found) {
            Script.failedReport.println("No holding for S: $duplicateUri")
        }
    }
}

void add(msg, thing, field, value) {
    def list = (thing[field] ?: []).with {it instanceof List ? it : [it]}
    msg.append("${field}: ").append(list).append(" -> ")
    list.addAll(value instanceof List ? value : [value])
    msg.append(list).append("\n")
    thing[value] = list
}

String getUri(String id) {
    isLegacyId(id)
            ? baseUri.toString() + systemIdFromLegacy(id)
            : baseUri.toString() + id
}

String systemIdFromLegacy(String id) {
    String result = null
    selectByIds(['http://libris.kb.se/resource/bib/' + id]) { bib ->
        result = bib.doc.shortId
    }
    if (result == null) {
        throw new RuntimeException("Could not get id for legacy id: $id")
    }
    return result
}

boolean isLegacyId(String id) {
    id ==~ /[0-9]{1,13}/
}

String whereNotSHolding(bibId) {
    whereHolding(bibId, " <> 'https://libris.kb.se/library/S'")
}

String whereSHolding(bibId) {
    whereHolding(bibId, " = 'https://libris.kb.se/library/S'")
}

String whereHolding(bibId, sigelOp) {
    """
    id in ( 
        select l.id 
        from lddb__identifiers i, lddb__dependencies d, lddb l
        where i.iri = '${bibId}'
        and d.dependsonid = i.id
        and d.relation = 'itemOf' 
        and d.id = l.id
        and l.data#>>'{@graph,1,heldBy,@id}' $sigelOp
    )
    """.stripIndent()
}

boolean hasOtherHolding(String bibId) {
    AtomicInteger count = new AtomicInteger()
    selectBySqlWhere(whereNotSHolding(bibId), { hold ->
        count.incrementAndGet()
    })
    return count.intValue() > 0
}