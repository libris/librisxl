import whelk.util.DocumentUtil
import whelk.util.Statistics

Statistics s = new Statistics(5)
s.printOnShutdown()

selectByCollection('bib') { bib ->
    try {
        DocumentUtil.findKey(bib.doc.data, 'marc:mediaTerm') { String value, path ->
            if (value.contains(']')) {
                String mediaType = value.substring(0, value.indexOf(']'))
                String suffix = value.substring(value.indexOf(']') + 1)
                if (!suffix.isBlank()) {
                    String id = bib.doc.shortId
                    s.increment('ALL', suffix, id)
                    s.increment(mediaType, suffix, id)
                    s.increment('TOTAL', 'TOTAL')
                }
            }

        }
    }
    catch(Exception e) {
        println(e)
        e.printStackTrace()
    }
}