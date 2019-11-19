/**
 *  - Remove https://id.kb.se/language/___
 *  - Change 'scc' -> 'srp'
 *  - Change 'scr' -> 'hrv'
 *
 *  See LXL-2737 for more info
 */

import whelk.util.DocumentUtil

PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")

selectByCollection('bib') { bib ->
    try {
        boolean modified = DocumentUtil.traverse(bib.doc.data, { value, path ->
            if (value instanceof Map) {
                if (value['@id'] == 'https://id.kb.se/language/___') {
                    return new DocumentUtil.Remove()
                }
                if (value['@id'] == 'https://id.kb.se/language/scc') {
                    return new DocumentUtil.Replace(['@id': 'https://id.kb.se/language/srp'])
                }
                if (value['@id'] == 'https://id.kb.se/language/scr') {
                    return new DocumentUtil.Replace(['@id': 'https://id.kb.se/language/hrv'])
                }
            }
        })

        if (modified) {
            println(bib.doc.data)
            scheduledForUpdate.println("${bib.doc.getURI()}")
            bib.scheduleSave()
        }
    }
    catch (Exception e) {
        println "failed ${bib.doc.getURI()} : ${e}"
        e.printStackTrace()
    }
}