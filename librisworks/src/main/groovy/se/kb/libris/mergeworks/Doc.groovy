package se.kb.libris.mergeworks

import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.datatool.DocumentItem
import whelk.util.DocumentUtil

import static Util.asList
import static Util.Relator

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
    boolean existsInStorage = true

    Map instanceData
    Map workData

    List<String> flatInstanceTitle
    List<String> flatWorkTitle

    DisplayDoc display

    Doc(DocumentItem docItem) {
        this.whelk = docItem.whelk
        this.document = docItem.doc
        this.existsInStorage = docItem.existsInStorage
        setDocItemIfNew(docItem)
        setData()
    }

    Doc(Whelk whelk, Document document) {
        this.whelk = whelk
        this.document = document
        setData()
    }

    void setDocItemIfNew(DocumentItem docItem) {
        if (!existsInStorage) {
            this.docItem = docItem
        }
    }

    void setData() {
        if (mainEntity()['instanceOf']) {
            instanceData = mainEntity()
            workData = asList(instanceData['instanceOf']).find()
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

    String recordIri() {
        document.getCompleteSystemId()
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

    List<String> flatWorkTitle() {
        if (!flatWorkTitle) {
            flatWorkTitle = Util.getFlatTitle(workTitle())
        }

        return flatWorkTitle
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

    List<Map> intendedAudience() {
        asList(workData['intendedAudience'])
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

    Map primaryContributor() {
        contribution().findResult { it['@type'] == 'PrimaryContribution' ? asList(it.agent).find() : null }
    }

    String editionStatement() {
        instanceData?.editionStatement
    }

    String responsibilityStatement() {
        instanceData?.responsibilityStatement
    }

    String physicalDetailsNote() {
        instanceData?.physicalDetailsNote
    }

    int numPages() {
        String extent = DocumentUtil.getAtPath(extent(), [0, 'label', 0]) ?: DocumentUtil.getAtPath(extent(), [0, 'label'], '')
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
        Util.hasGenericTitle(instanceTitle()) || Util.hasGenericTitle(workTitle())
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
                || !contribution().any { it['@type'] == 'PrimaryContribution' && it['agent'] }
                || hasRelationshipWithContribution()
    }

    boolean intendedForMarcPreAdolescent() {
        intendedAudience().contains(['@id': 'https://id.kb.se/marc/PreAdolescent'])
    }

    boolean hasPart() {
        workData['hasPart'] || instanceData['hasTitle'].findAll { it['@type'] == 'Title' }.any {
            it.hasPart?.size() > 1
                    || it.hasPart?.any { p -> asList(p.partName).size() > 1
                    || asList(p.partNumber).size() > 1 }
                    // space+semicolon indicates an aggregate if it is not preceded by a slash
                    // aggregate: Måsen ; Onkel Vanja ; Körsbärsträdgården
                    // not aggregate: En visa för de döda / Patrick Dunne ; översättning: Hans Lindeberg
                    || [it.mainTitle, it.titleRemainder, it.subtitle].findAll().toString() =~ /(?<!\/.+)(\s+;)/
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
        genreForm().any { it['@id'] == SAOGF_SKÖN || whelk.relations.isImpliedBy(SAOGF_SKÖN, it['@id'] ?: '') }
    }

    boolean isSabFiction() {
        classification().any { it.inScheme?.code =~ /[Kk]ssb/ && it.code =~ /^(H|h|uH|ufH|ugH)/ }
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

    boolean isNotRegularText() {
        Set barnGfs = [
                'https://id.kb.se/term/barngf/Mekaniska%20b%C3%B6cker',
                'https://id.kb.se/term/barngf/Pop-up-b%C3%B6cker',
                'https://id.kb.se/term/barngf/TAKK',
                'https://id.kb.se/term/barngf/Taktila%20b%C3%B6cker',
                'https://id.kb.se/term/barngf/Taktila%20illustrationer',
                'https://id.kb.se/term/barngf/Teckenspr%C3%A5k',
                'https://id.kb.se/term/barngf/Tecken%20som%20st%C3%B6d%20till%20talet',
                'https://id.kb.se/term/barngf/Bliss%20%28symbolspr%C3%A5k%29'
        ] as Set

        def saogfTactile = 'https://id.kb.se/term/saogf/Taktila%20verk'

        asList(workData['contentType']).contains(['@id': 'https://id.kb.se/term/rda/TactileText'])
                || asList(instanceData?.carrierType).any { it['@id'] in ['https://id.kb.se/marc/Braille', 'https://id.kb.se/marc/TacMaterialType-b'] }
                || genreForm().any {it['@id'] in barnGfs || it['@id'] == saogfTactile ||  whelk.relations.isImpliedBy(saogfTactile, it['@id']) }
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

    void sortContribution() {
        // PrimaryContribution first
        contribution()?.sort {it['@type'] != 'PrimaryContribution' }
    }
}