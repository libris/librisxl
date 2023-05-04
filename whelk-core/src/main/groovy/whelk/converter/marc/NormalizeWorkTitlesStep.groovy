package whelk.converter.marc

import whelk.filter.LanguageLinker

import static whelk.JsonLd.WORK_KEY
import static whelk.JsonLd.asList
import static whelk.JsonLd.ID_KEY as ID
import static whelk.JsonLd.TYPE_KEY as TYPE

class NormalizeWorkTitlesStep extends MarcFramePostProcStepBase {

    private static final String EXPRESSION_OF = 'expressionOf'
    private static final String TRANSLATION_OF = 'translationOf'
    private static final String LANGUAGE = 'language'
    private static final String HAS_TITLE = 'hasTitle'
    private static final String TITLE = 'Title'
    private static final String LEGAL_DATE = 'legalDate'
    private static final String CONTRIBUTION = 'contribution'
    private static final String HAS_PART = 'hasPart'

    private static final List<String> titleProps = [HAS_TITLE, 'musicKey', 'musicMedium', 'version', LEGAL_DATE, 'originDate', 'marc:arrangedStatementForMusic']

    boolean requiresResources = true
    LanguageLinker langLinker

    void modify(Map record, Map thing) {
        def work = thing[WORK_KEY]
        if (work instanceof Map) {
            langLinker?.linkAll(work)
            moveExpressionOfTitle(work)
            moveTranslationOfIntoParts(work)
            traverse(work, moveOriginalTitleToTranslationOf)
        }
    }

    void unmodify(Map record, Map thing) {
        def work = thing[WORK_KEY]
        if (work instanceof Map) {
            traverse(work, moveOriginalTitleToTranslation)
            titleToExpressionOfIfNoPrimaryContribution(work)
        }
    }

    void traverse(o, Closure handleTitles) {
        asList(o).each {
            if (it instanceof Map && !('_revertOnly' in it)) {
                handleTitles(it)
                it.each { k, v ->
                    traverse(v, handleTitles)
                }
            }
        }
    }

    void moveExpressionOfTitle(Map work) {
        if (hasOkExpressionOf(work)) {
            if (tryMove(work[EXPRESSION_OF], work, titleProps)) {
                work.remove(EXPRESSION_OF)
            }
        }
    }

    void moveTranslationOfIntoParts(Map work) {
        if (!work[HAS_PART] || !work[TRANSLATION_OF]) {
            return
        }
        def origLangs = asList(work[TRANSLATION_OF]).collect { it[LANGUAGE] }.flatten().unique()
        def workLangs = work[LANGUAGE]

        def safeToMove = workLangs.size() == 1
                && origLangs.size() == 1
                && work[HAS_PART].every { p ->
            p[LANGUAGE] == workLangs
                    && p[LANGUAGE] != origLangs
                    && p[HAS_TITLE]
                    && !p[TRANSLATION_OF]
        }

        if (safeToMove) {
            work[HAS_PART].each { p ->
                p[TRANSLATION_OF] = work[TRANSLATION_OF]
            }
            if (!work[HAS_TITLE]) {
                work.remove(TRANSLATION_OF)
            }
        }
    }

    def moveOriginalTitleToTranslationOf = { Map work ->
        def isMusic = work[TYPE] in ['Music', 'NotatedMusic']
        def hasTranslator = asList(work[CONTRIBUTION]).any { Map c ->
            asList(c.role).any { Map r ->
                r.code == 'trl' || r[ID] == 'https://id.kb.se/relator/translator'
            }
        }

        if (!isMusic) {
            if (!work[TRANSLATION_OF] && hasTranslator && work[HAS_TITLE]) {
                work[TRANSLATION_OF] = [(TYPE): 'Work']
            }
            if (asList(work[TRANSLATION_OF]).size() == 1) {
                tryMove(work, work[TRANSLATION_OF], [HAS_TITLE, LEGAL_DATE])
            }
        }
    }

    def moveOriginalTitleToTranslation = { Map work ->
        asList(work[TRANSLATION_OF]).each { original ->
            def originalTitle = asList(original[HAS_TITLE]).find { it[TYPE] == TITLE }
            if (originalTitle instanceof Map) {
                markToIgnore(work[HAS_TITLE])
                if (!work[HAS_TITLE]) work[HAS_TITLE] = []
                work[HAS_TITLE] << copyForRevert(originalTitle)
                original.remove(HAS_TITLE)
                if (original[LEGAL_DATE] && !work[LEGAL_DATE]) {
                    work[LEGAL_DATE] = original[LEGAL_DATE]
                }
            }
        }
    }

    void titleToExpressionOfIfNoPrimaryContribution(Map work) {
        if (!work[CONTRIBUTION]?.find { it[TYPE] == 'PrimaryContribution' }
                && !work[EXPRESSION_OF]
                && work[HAS_TITLE]
        ) {
            def workTitle = work[HAS_TITLE]?.find { it[TYPE] == TITLE && !('_revertedBy' in it) }
            if (workTitle instanceof Map) {
                def copiedTitle = copyForRevert(workTitle)
                markToIgnore(workTitle)
                work[EXPRESSION_OF] = [
                        (TYPE)     : 'Work',
                        (HAS_TITLE): [copiedTitle],
                        _revertOnly: true
                ]
                (titleProps - HAS_TITLE).each {
                    if (work[it]) {
                        work[EXPRESSION_OF][it] = work[it]
                    }
                }
                if (work[TRANSLATION_OF]) {
                    work[EXPRESSION_OF][LANGUAGE] = work[LANGUAGE]
                }
            }
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

    boolean hasOkExpressionOf(Map work) {
        if (asList(work[EXPRESSION_OF]).size() != 1) {
            return false
        }
        def expressionOf = asList(work[EXPRESSION_OF])[0]
        if (!expressionOf[HAS_TITLE]) {
            return false
        }
        def workLang = asList(work[LANGUAGE])
        def trlOf = asList(work[TRANSLATION_OF])[0]
        def trlOfLang = trlOf ? asList(trlOf[LANGUAGE]) : []
        langLinker?.linkLanguages(expressionOf, workLang + trlOfLang)
        def exprOfLang = asList(expressionOf[LANGUAGE])
        return exprOfLang.every { l -> (workLang + trlOfLang).any { l.subMap(['@id'] + langLinker?.fields).intersect(it) } }
    }
}
