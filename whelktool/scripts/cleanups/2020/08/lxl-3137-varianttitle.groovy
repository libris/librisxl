/**
 * Change non-imported 'DistinctiveTitle' and all 'marc:OtherTitle' to 'VariantTitle'
 *
 * See LXL-3137 for more information.
 */

import whelk.util.DocumentUtil

class Script {
    static PrintWriter report
}
Script.report = getReportWriter("report.txt")

selectByCollection('bib') { bib ->
    try {
        process(bib)
    }
    catch(Exception e) {
        System.err.println("${bib.doc.shortId} $e")
        e.printStackTrace()
    }
}

void process(bib) {
    def (record, thing) = bib.graph

    if (!thing['hasTitle']) {
        return
    }

    DocumentUtil.traverse(thing['hasTitle']) { value, path ->
        if (value instanceof Map && changeTitleType(value, record)) {
            Script.report.println("${bib.doc.shortId} hasTitle${path} ${value['@type']}")
            value['@type'] = 'VariantTitle'
            bib.scheduleSave()
        }

        DocumentUtil.NOP
    }
}

boolean changeTitleType(Map title, record) {
    (title['@type'] == 'DistinctiveTitle' && !isImported(record)) || title['@type'] == 'marc:OtherTitle'
}

boolean isImported(Map record) {
    record['descriptionUpgrader'] && record['identifiedBy']
}