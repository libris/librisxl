package mergeworks


import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.datatool.DocumentItem

import static Util.asList
import mergeworks.Util.Relator

class Doc {
    public static final String SAOGF_SKÖN = 'https://id.kb.se/term/saogf/Sk%C3%B6nlitteratur'
    public static final List MARC_FICTION = [
            'https://id.kb.se/marc/FictionNotFurtherSpecified',
            'https://id.kb.se/marc/Drama',
            'https://id.kb.se/marc/Essay',
            'https://id.kb.se/marc/Novel',
            'https://id.kb.se/marc/HumorSatiresEtc',
            'https://id.kb.se/marc/Letter',
            'https://id.kb.se/marc/ShortStory',
            'https://id.kb.se/marc/MixedForms',
            'https://id.kb.se/marc/Poetry',
    ]
    public static final List MARC_NOT_FICTION = [
            'https://id.kb.se/marc/NotFictionNotFurtherSpecified',
            'https://id.kb.se/marc/Biography'
    ]
    public static final List DRAMA_GF = [
            'https://id.kb.se/term/saogf/Dramatik',
            'https://id.kb.se/marc/Drama'
    ]


    Whelk whelk
    Document document
    DocumentItem docItem

    Map instanceData
    Map workData

    List<String> flatInstanceTitle

    DisplayDoc display

    Doc(DocumentItem docItem) {
        this.whelk = docItem.whelk
        this.document = docItem.doc
        this.docItem = docItem
        setData()
    }

    void setData() {
        if (mainEntity()['instanceOf']) {
            instanceData = mainEntity()
            workData = instanceData['instanceOf']
        } else {
            workData = mainEntity()
        }
    }

    DisplayDoc getView() {
        if (!display) {
            display = new DisplayDoc(this)
        }

        return display
    }

    Map record() {
        document.data['@graph'][0]
    }

    Map mainEntity() {
        document.data['@graph'][1]
    }

    String shortId() {
        document.shortId
    }

    String thingIri() {
        document.getThingIdentifiers().first()
    }

    String encodingLevel() {
        return record()['encodingLevel'] ?: ''
    }

    String workIri() {
        workData['@id']
    }

    List<Map> workTitle() {
        asList(workData['hasTitle'])
    }

    List<Map> instanceTitle() {
        asList(instanceData?.hasTitle)
    }

    List<String> flatInstanceTitle() {
        if (!flatInstanceTitle) {
            flatInstanceTitle = Util.getFlatTitle(instanceTitle())
        }

        return flatInstanceTitle
    }

    String workType() {
        workData['@type']
    }

    String instanceType() {
        instanceData?.'@type'
    }

    List<Map> translationOf() {
        asList(workData['translationOf'])
    }

    List<Map> contribution() {
        asList(workData['contribution'])
    }

    List<Map> classification() {
        asList(workData['classification'])
    }

    List<Map> genreForm() {
        asList(workData['genreForm'])
    }

    List<Map> publication() {
        asList(instanceData?.publication)
    }

    List<Map> identifiedBy() {
        asList(instanceData?.identifiedBy)
    }

    List<Map> extent() {
        asList(instanceData?.extent)
    }

    List<Map> reproductionOf() {
        asList(instanceData?.reproductionOf)
    }

    String editionStatement() {
        instanceData?.editionStatement
    }

    String responsibilityStatement() {
        instanceData?.responsibilityStatement
    }

    int numPages() {
        String extent = Util.getPathSafe(extent(), [0, 'label', 0]) ?: Util.getPathSafe(extent(), [0, 'label'], '')
        return numPages(extent)
    }

    // TODO: improve parsing https://metadatabyran.kb.se/beskrivning/materialtyper-arbetsfloden/tryckta-monografier/omfang-for-tryckta-monografier
    static int numPages(String extentLabel) {
        def l = extentLabel.replace('onumrerade', '')
        def matcher = l =~ /(\d+)(?=[, \[\]0-9]*[sp])/
        List<Integer> pages = []
        while (matcher.find()) {
            pages << Integer.parseInt(matcher.group(1))
        }
        pages ? pages.max() : -1
    }

    boolean hasGenericTitle() {
        Util.hasGenericTitle(instanceTitle())
    }

    boolean isMonograph() {
        instanceData?.issuanceType == 'Monograph'
    }

    boolean isManuscript() {
        instanceType() == 'Manuscript' || [['@id': 'https://id.kb.se/term/saogf/Manuskript'], ['@id': 'https://id.kb.se/term/saogf/Handskrifter']].intersect(genreForm())
    }

    boolean isInSb17Bibliography() {
        asList(record()['bibliography']).contains(['@id': 'https://libris.kb.se/library/SB17'])
    }

    boolean isMaybeAggregate() {
        hasPart()
                || classification().any { it.inScheme?.code =~ /[Kk]ssb/ && it.code?.contains('(s)') }
                || !contribution().any { it['@type'] == 'PrimaryContribution' }
                || hasRelationshipWithContribution()
    }

    boolean hasPart() {
        workData['hasPart'] || instanceData['hasTitle'].findAll { it['@type'] == 'Title' }.any {
            it.hasPart?.size() > 1 || it.hasPart?.any { p -> asList(p.partName).size() > 1 || asList(p.partNumber).size() > 1 }
        }
    }

    boolean hasRelationshipWithContribution() {
        asList(workData['relationship']).any { r ->
            asList(r['entity']).any { e ->
                e.containsKey('contribution')
            }
        }
    }

    boolean isFiction() {
        isMarcFiction() || isSaogfFiction() || isSabFiction()
    }

    boolean isMarcFiction() {
        genreForm().any { it['@id'] in MARC_FICTION }
    }

    boolean isMarcNotFiction() {
        genreForm().any { it['@id'] in MARC_NOT_FICTION }
    }

    boolean isSaogfFiction() {
        genreForm().any { whelk.relations.isImpliedBy(SAOGF_SKÖN, it['@id'] ?: '') }
    }

    boolean isSabFiction() {
        classification().any { it.inScheme?.code =~ /[Kk]ssb/ && it.code =~ /^(H|uH|ufH|ugH)/ }
    }

    boolean isNotFiction() {
        // A lot of fiction has marc/NotFictionNotFurtherSpecified but then classification is usually empty
        isMarcNotFiction() && (!classification().isEmpty() && !isSabFiction())
    }

    boolean isText() {
        workData['@type'] == 'Text'
    }

    boolean isAnonymousTranslation() {
        translationOf() && !hasAnyRole([Relator.TRANSLATOR, Relator.EDITOR, Relator.ADAPTER])
    }

    boolean hasAnyRole(List<Relator> relators) {
        contribution().any {
            asList(it['role']).intersect(relators.collect { [(JsonLd.ID_KEY): it.iri] })
        }
    }

    boolean isDrama() {
        isSabDrama() || isGfDrama()
    }

    boolean isSabDrama() {
        classification().any { it.code?.contains('Hc.02') || it.code?.contains('Hce.02') }
    }

    boolean isGfDrama() {
        asList(genreForm()).any { it['@id'] in DRAMA_GF }
    }

    boolean isTactile() {
        asList(workData['contentType']).contains(['@id': 'https://id.kb.se/term/rda/TactileText'])
                || asList(instanceData?.carrierType).any { it['@id'] in ['https://id.kb.se/marc/Braille', 'https://id.kb.se/marc/TacMaterialType-b'] }
    }

    boolean isThesis() {
        genreForm().any { it == ['@id': 'https://id.kb.se/marc/Thesis'] }
    }

    boolean hasDistinguishingEdition() {
        (instanceData?.editionStatement ?: '').toString().toLowerCase().contains("förk")
    }

    void addComparisonProps() {
        if (hasDistinguishingEdition()) {
            workData['_editionStatement'] = instanceData['editionStatement']
        }
        workData['_numPages'] = numPages()
    }

    void removeComparisonProps() {
        workData.remove('_editionStatement')
        workData.remove('_numPages')
    }
}