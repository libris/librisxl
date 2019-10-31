/**
 *  Remove https://id.kb.se/language/___
 *
 *  See LXL-2737 for more info
 */

import datatool.util.DocumentUtil

PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")

selectByCollection('bib') { bib ->
    try {
        boolean modified = DocumentUtil.traverse(bib.doc.data, { value, path ->
            if (value instanceof Map && value['@id'] == 'https://id.kb.se/language/___') {
                return new DocumentUtil.Remove()
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