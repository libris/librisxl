package whelk.converter.marc

import java.util.regex.Matcher
import java.util.regex.Pattern
import org.codehaus.jackson.map.ObjectMapper

import whelk.JsonLd
import static whelk.JsonLd.asList
import static whelk.JsonLd.ID_KEY as ID
import static whelk.JsonLd.TYPE_KEY as TYPE

class NormalizeWorkTitlesStep extends MarcFramePostProcStepBase {

    void modify(Map record, Map thing) {
    }

    void unmodify(Map record, Map thing) {
        def work = thing.instanceOf

        if (!work) return

        asList(work.translationOf).each { original ->
            def originalTitle = asList(original.hasTitle).find { it[TYPE] == 'Title' }

            Set workLangIds = asList(work.language).collect { it[ID] }
            if (originalTitle instanceof Map) {
                boolean noWorkLangOrNotSameLang =
                    workLangIds.size() == 0
                    || !asList(original.language).every { it[ID] in workLangIds }
                if (noWorkLangOrNotSameLang) {
                    markToIgnore(work.hasTitle)
                    if (!work.hasTitle) work.hasTitle = []
                    work.hasTitle << copyForRevert(originalTitle)
                }
            }
        }

        if (!work.contribution?.find { it[TYPE] == 'PrimaryContribution' }
            && !work.expressionOf
            && work.hasTitle
        ) {
            def workTitle = work.hasTitle?.find { it[TYPE] == 'Title' }
            if (workTitle instanceof Map) {
                def copiedTitle = copyForRevert(workTitle)
                markToIgnore(workTitle)
                work.expressionOf = [
                    (TYPE): 'Work',
                    hasTitle: [ copiedTitle ]
                ]
            }
        }
    }

    void markToIgnore(item) {
        asList(item).each {
            it._revertedBy = 'NormalizeWorkTitlesStep'
        }
    }

    Map copyForRevert(Map item) {
        item = item.clone()
        item._revertOnly = true
        return item
    }
}
