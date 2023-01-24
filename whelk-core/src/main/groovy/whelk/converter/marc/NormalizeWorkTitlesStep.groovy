package whelk.converter.marc

import whelk.filter.LanguageLinker
import static whelk.JsonLd.asList
import static whelk.JsonLd.ID_KEY as ID
import static whelk.JsonLd.TYPE_KEY as TYPE

class NormalizeWorkTitlesStep extends MarcFramePostProcStepBase {

    List<String> titleProps = ['hasTitle', 'musicKey', 'musicMedium', 'version', 'marc:version', 'legalDate', 'originDate']

    LanguageLinker langLinker = getLangLinker()

    void modify(Map record, Map thing) {
//        traverse(thing, null, true)
    }

    void unmodify(Map record, Map thing) {
        traverse(thing)
    }

    void traverse(o, String via = null, convert = false) {
        asList(o).each {
            if (it instanceof Map) {
                if (convert) {
                    shuffleTitles(it)
                } else if (!('_revertOnly' in it)) {
                    useOriginalTitle(it, via)
                }
                it.each { k, v ->
                    traverse(v, k, convert)
                }
            }
        }
    }

    void useOriginalTitle(Map work, String via = null) {
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
                    if (original['legalDate'] && !work['legalDate']) {
                        work['legalDate'] = original['legalDate']
                    }
                }
            }
        }

        if (via == 'instanceOf'
                && !work.contribution?.find { it[TYPE] == 'PrimaryContribution' }
                && !work.expressionOf
//                && (!work.expressionOf || work.expressionOf[ID]) // Only if work.expressionOf[ID].inCollection == https://id.kb.se/term/uniformWorkTitle, should never be the case
                && work.hasTitle
        ) {
            def workTitle = work.hasTitle?.find { it[TYPE] == 'Title' && !('_revertedBy' in it) }
            if (workTitle instanceof Map) {
                def copiedTitle = copyForRevert(workTitle)
                markToIgnore(workTitle)
                work.expressionOf = [
                        (TYPE)     : 'Work',
                        hasTitle   : [copiedTitle],
                        _revertOnly: true
                ]
                (titleProps - 'hasTitle').each {
                    if (work[it]) {
                        work.expressionOf[it] = work[it]
                    }
                }
            }
        }
    }

    void markToIgnore(o) {
        asList(o).each {
            it._revertedBy = 'NormalizeWorkTitlesStep'
        }
    }

    Map copyForRevert(Map item) {
        item = item.clone()
        item._revertOnly = true
        return item
    }

    //TODO: Implement here or in marcframe?
    void normalizePunctuationOnRevert(Object title) {

    }

    void normalizePunctuationOnConvert(Object title) {

    }

    void shuffleTitles(Map work) {
        if (hasOkExpressionOf(work)) {
            // try match existing linked hub first?
            if (tryMove(work['expressionOf'], work, titleProps)) {
                work.remove('expressionOf')
            }
        }
        def isMusic = { it[TYPE] in ['Music', 'NotatedMusic'] }
        if (!isMusic(work) && asList(work['translationOf']).size() == 1) {
            tryMove(work, work['translationOf'], ['hasTitle', 'legalDate'])
        }
    }

    boolean tryMove(Object from, Object target, Collection properties) {
        from = asList(from)[0]
        target = asList(target)[0]

        def moveThese = properties ? from.keySet().intersect(properties) : from.keySet()
        def conflictingProps = moveThese.intersect(target.keySet())

        if (conflictingProps && conflictingProps.any { from[it] != target[it] }) {
            return false
        }

        from.removeAll { k, v ->
            if (k in moveThese) {
                target[k] = v
                return true
            }
            return false
        }

        return true
    }

    boolean hasOkExpressionOf(Map work) {
        if (asList(work['expressionOf']).size() != 1) {
            return false
        }
        def expressionOf = asList(work['expressionOf'])[0]
        if (!expressionOf['hasTitle']) {
            return false
        }
        langLinker.linkAll(work)
        def workLang = asList(work['language'])
        def trlOf = asList(work['translationOf'])[0]
        def trlOfLang = trlOf ? asList(trlOf['language']) : []
        langLinker.linkLanguages(expressionOf, workLang + trlOfLang)
        def exprOfLang = asList(expressionOf['language'])
        return (workLang + trlOfLang).containsAll(exprOfLang)
    }

    LanguageLinker getLangLinker() {
        //TODO
    }
}
