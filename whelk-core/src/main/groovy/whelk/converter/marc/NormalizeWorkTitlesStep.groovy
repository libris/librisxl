package whelk.converter.marc

import whelk.filter.LanguageLinker
import static whelk.JsonLd.asList
import static whelk.JsonLd.ID_KEY as ID
import static whelk.JsonLd.TYPE_KEY as TYPE

class NormalizeWorkTitlesStep extends MarcFramePostProcStepBase {

    List<String> titleRelatedProps = ['hasTitle', 'musicKey', 'musicMedium', 'version', 'marc:version', 'marc:fieldref', 'legalDate', 'originDate']

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
                    (titleRelatedProps - 'hasTitle').each {
                        if (original[it]) {
                            work[it] = original[it]
                        }
                    }
                }
            }
        }

        if (
        via == 'instanceOf'
                && !work.contribution?.find { it[TYPE] == 'PrimaryContribution' }
                && !work.expressionOf
//                && (!work.expressionOf || work.expressionOf[ID]) TODO: Only if work.expressionOf[ID].inCollection == https://id.kb.se/term/uniformWorkTitle
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
                (titleRelatedProps - 'hasTitle').each {
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
        if (isOkTranslation(work)) {
            tryMoveTitle(work, work.translationOf)
        } else {
            def target = isMusic(work) ? work : (work.translationOf ?: work)
            if (isOkExpressionOf(work.expressionOf, target) && tryMoveTitle(work.expressionOf, target)) {
                work.remove('expressionOf')
            }
        }
    }

    boolean tryMoveTitle(Object from, Object target) {
        from = asList(from)[0]
        target = asList(target)[0]

        //TODO: Not sure that all properties should be moved to translationOf, needs further analysis
        def moveThese = from.keySet().intersect(titleRelatedProps)
        if (!moveThese.contains('hasTitle') || moveThese.intersect(target.keySet())) {
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

    boolean isOkTranslation(Map work) {
        !work.expressionOf
                && !isMusic(work)
                && work.translationOf
                && asList(work.translationOf).size() == 1
    }

    boolean isOkExpressionOf(Object expressionOf, Object target) {
        expressionOf
                && asList(expressionOf).size() == 1
                && asList(target).size() == 1
                && compatibleLangs(asList(expressionOf)[0], asList(target)[0])
    }

    boolean isMusic(Map work) {
        work[TYPE] in ['Music', 'NotatedMusic']
    }

    boolean compatibleLangs(Map expressionOf, Map target) {
//        moveLanguagesFromTitle(expressionOf) TODO: Necessary?
        langLinker.linkLanguages(target)
        langLinker.linkLanguages(expressionOf, asList(target.language))
        return asList(target.language).containsAll(expressionOf.language)
    }

    LanguageLinker getLangLinker() {
        //TODO
    }
}
