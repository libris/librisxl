/**
 *  Replace links to obsolete language codes with their replacements (if one-to-one).
 *
 *  See LXL-3078 for more info
 */

import whelk.util.DocumentUtil
import whelk.util.DocumentUtil.Replace

PrintWriter scheduledForUpdate = getReportWriter('scheduled-for-update')

queryDocs(['@type': ['Language']])
        .grep(this.&isReplacedByExactlyOne)
        .each { obsolete ->
            String oldId = obsolete['@id']
            String newId = obsolete['isReplacedBy']['@id']

            selectByIds(queryIds(['o': [oldId]]).collect()) { d ->
                boolean modified = DocumentUtil.findKey(d.doc.data, '@id') { value, path ->
                    if (value == oldId) {
                        new Replace(newId)
                    }
                }

                if (modified) {
                    String msg = "${d.doc.shortId} : $oldId --> $newId"
                    println(msg)
                    scheduledForUpdate.println(msg)
                    d.scheduleSave()
                }
            }
        }

boolean isReplacedByExactlyOne(thing) {
    thing['isReplacedBy'] instanceof Map || thing['isReplacedBy']?.size() == 1
}