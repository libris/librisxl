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
            String id = obsolete['@id']
            String replacedBy = obsolete['isReplacedBy']['@id']

            selectByIds(queryIds(['o': [id]]).collect()) { d ->
                boolean modified = DocumentUtil.findKey(d.doc.data, '@id') { value, path ->
                    if (value == id) {
                        new Replace(replacedBy)
                    }
                }

                if (modified) {
                    String msg = "${d.doc.shortId} : $id --> $replacedBy"
                    println(msg)
                    scheduledForUpdate.println(msg)
                    d.scheduleSave()
                }
            }
        }

boolean isReplacedByExactlyOne(thing) {
    thing['isReplacedBy'] instanceof Map || thing['isReplacedBy']?.size() == 1
}