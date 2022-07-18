package datatool.scripts.mergeworks

import se.kb.libris.Normalizers
import whelk.Document
import whelk.JsonLd
import whelk.Whelk

import static datatool.scripts.mergeworks.Util.asList

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
    Document doc
    Map work
    Map framed
    List<String> titles

    //FIXME
    Document ogDoc

    Doc(Whelk whelk, Document doc) {
        this.whelk = whelk
        this.doc = doc
        this.ogDoc = doc.clone()
    }

    Map getWork() {
        if (!work) {
            work = getWork(whelk, doc)
        }

        return work
    }

    static Map getWork(Whelk whelk, Document d) {
        Map work = Normalizers.getWork(whelk.jsonld, d)
        if (!work) {
            throw new NoWorkException(d.shortId)
        }
        work = new HashMap<>(work)

        //TODO 'marc:fieldref'

        work.remove('@id')
        return work
    }

    Map getMainEntity() {
        return doc.data['@graph'][1]
    }

    boolean isInstance() {
        return getMainEntity().containsKey('instanceOf')
    }

    List<String> getTitleVariants() {
        if (!titles) {
            titles = Util.getTitleVariants(getMainEntity()['hasTitle'])
        }

        return titles
    }

    boolean hasGenericTitle() {
        Util.hasGenericTitle(getMainEntity()['hasTitle'])
    }

    private static String displayTitle(Map thing) {
        thing['hasTitle'].collect { it['@type'] + ": " + it['flatTitle'] }.join(', ')
    }

    String mainEntityDisplayTitle() {
        displayTitle(['hasTitle': Util.flatTitles(getMainEntity()['hasTitle'])])
    }

    String link() {
        String base = Document.getBASE_URI().toString()
        String kat = "katalogisering/"
        String id = doc.shortId
        return base + kat + id
    }

    boolean isMonograph() {
        getMainEntity()['issuanceType'] == 'Monograph'
    }

    boolean hasPart() {
        getWork()['hasPart'] != null
    }

    String encodingLevel() {
        return doc.data['@graph'][0]['encodingLevel'] ?: ''
    }

    int numPages() {
        String extent = Util.getPathSafe(getMainEntity(), ['extent', 0, 'label', 0]) ?: Util.getPathSafe(getMainEntity(), ['extent', 0, 'label'], '')
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

    // TODO...
    String getDisplayText(String field) {
        if (field == 'contribution') {
            return contributorStrings().join("<br>")
        } else if (field == 'classification') {
            return classificationStrings().join("<br>")
        } else if (field == 'instance title') {
            return isInstance() ? (getMainEntity()['hasTitle'] ?: '') : ''
        } else if (field == 'work title') {
            // To load hasTitle from linked work in instanceOf we can use getFramed()
            // However we then need to handle that getFramed() loads linked instances in hasTitle.source
            // Prefer getMainEntity() for now
            return isInstance() ? (getMainEntity()['instanceOf']['hasTitle'] ?: '') : (getMainEntity()['hasTitle'] ?: '')
        } else if (field == 'instance type') {
            return isInstance() ? getMainEntity()['@type'] : ''
        } else if (field == 'editionStatement') {
            return getMainEntity()['editionStatement'] ?: ''
        } else if (field == 'responsibilityStatement') {
            return getMainEntity()['responsibilityStatement'] ?: ''
        } else if (field == 'encodingLevel') {
            return encodingLevel()
        } else if (field == 'publication') {
            return chipString(getMainEntity()['publication'] ?: [])
        } else if (field == 'identifiedBy') {
            return chipString(getMainEntity()['identifiedBy'] ?: [])
        } else if (field == 'extent') {
            return chipString(getMainEntity()['extent'] ?: [])
        } else if (field == 'reproductionOf') {
            return reproductionOfLink()
        } else {
            return chipString(getWork().getOrDefault(field, []))
        }
    }

    protected String chipString(def thing) {
        Util.chipString(thing, whelk)
    }

    String tooltip(String string, String tooltip) {
        """<abbr title="${tooltip}">${string}</abbr>"""
    }

    private String reproductionOfLink() {
        def shortId = Util.getPathSafe(getMainEntity(), ['reproductionOf', '@id'])
                ?.tokenize("/#")
                ?.dropRight(1)
                ?.last() ?: ''

        return "<a href=\"#$shortId\">$shortId</a>"
    }

    private List classificationStrings() {
        List path = isInstance() ? ['instanceOf', 'classification'] : ['classification']
        List<Map> classification = Util.getPathSafe(getFramed(), path, [])
        classification.collect() { c ->
            StringBuilder s = new StringBuilder()
            s.append(flatMaybeLinked(c['inScheme'], ['code', 'version']).with { it.isEmpty() ? it : it + ': ' })
            s.append(flatMaybeLinked(c, ['code']))
            return s.toString()
        }
    }

    private List contributorStrings() {
        List path = isInstance() ? ['instanceOf', 'contribution'] : ['contribution']
        List contribution = Util.getPathSafe(getFramed(), path, [])

        return contribution.collect { Map c ->
            contributionStr(c)
        }
    }

    protected Map getFramed() {
        if (!framed) {
            if (isInstance()) {
                framed = JsonLd.frame(doc.getThingIdentifiers().first(), whelk.loadEmbellished(doc.shortId).data)
            } else {
                Document copy = doc.clone()
                whelk.embellish(copy)
                framed = JsonLd.frame(doc.getThingIdentifiers().first(), copy.data)
            }
        }

        return framed
    }

    private String contributionStr(Map contribution) {
        StringBuilder s = new StringBuilder()

        if (contribution['@type'] == 'PrimaryContribution') {
            s.append('<b>')
        }

        s.append(flatMaybeLinked(contribution['role'], ['code', 'label']).with { it.isEmpty() ? it : it + ': ' })
        s.append(flatMaybeLinked(contribution['agent'], ['givenName', 'familyName', 'lifeSpan', 'name']))

        if (contribution['@type'] == 'PrimaryContribution') {
            s.append('</b>')
        }

        return s.toString()
    }

    static String flatten(Object o, List order, String mapSeparator = ': ') {
        if (o instanceof String) {
            return o
        }
        if (o instanceof List) {
            return o
                    .collect { flatten(it, order) }
                    .join(' || ')
        }
        if (o instanceof Map) {
            return order
                    .findResults { ((Map) o).get(it) }
                    .collect { flatten(it, order) }
                    .join(mapSeparator)
        }

        throw new RuntimeException(String.format("unexpected type: %s for %s", o.class.getName(), o))
    }

    private String flatMaybeLinked(Object thing, List order) {
        if (!thing)
            return ''

        if (thing instanceof List) {
            return thing.collect { flatMaybeLinked(it, order) }.join(' | ')
        }
        String s = flatten(thing, order, ', ')

        thing['@id']
                ? """<a href="${thing['@id']}">$s</a>"""
                : s
    }

    boolean isFiction() {
        isMarcFiction() || isSaogfFiction() || isSabFiction()
    }

    boolean isMarcFiction() {
        (getWork()['genreForm'] ?: []).any { it['@id'] in MARC_FICTION }
    }

    boolean isMarcNotFiction() {
        (getWork()['genreForm'] ?: []).any { it['@id'] in MARC_NOT_FICTION }
    }

    boolean isSaogfFiction() {
        (getWork()['genreForm'] ?: []).any { whelk.relations.isImpliedBy(SAOGF_SKÖN, it['@id'] ?: '') }
    }

    boolean isSabFiction() {
        classificationStrings().any { it.contains('kssb') && it.contains(': H') }
    }

    boolean isNotFiction() {
        // A lot of fiction has marc/NotFictionNotFurtherSpecified but then classification is usually empty
        isMarcNotFiction() && (!classificationStrings().isEmpty() && !isSabFiction())
    }

    boolean isText() {
        getWork()['@type'] == 'Text'
    }

    boolean isTranslationWithoutTranslator() {
        isTranslation() && !hasTranslator()
    }

    boolean isTranslation() {
        getWork()['translationOf']
    }

    boolean isSabDrama() {
        classificationStrings().any { it.contains(': Hc.02') || it.contains(': Hce.02') }
    }

    boolean isGfDrama() {
        asList(getWork()['genreForm']).any { it['@id'] in DRAMA_GF }
    }

    boolean isDrama() {
        isSabDrama() || isGfDrama()
    }

    boolean hasRole(String relatorIri) {
        asList(getWork()['contribution']).any {
            asList(it['role']).contains(['@id': relatorIri])
        }
    }

    boolean hasTranslator() {
        hasRole('https://id.kb.se/relator/translator')
    }

    boolean hasDistinguishingEdition() {
        (getMainEntity()['editionStatement'] ?: '').toString().toLowerCase().contains("förk")
    }

    boolean hasRelationshipWithContribution() {
        asList(getWork()['relationship']).any { r ->
            asList(r['entity']).any { e ->
                e.containsKey('contribution')
            }
        }
    }

    void addComparisonProps() {
        if (hasDistinguishingEdition()) {
            addToWork('editionStatement')
        }
        getWork()['_numPages'] = numPages()
    }

    void moveSummaryToInstance() {
        if (getWork()['summary']) {
            getMainEntity()['summary'] = asList(getMainEntity()['summary']) + asList(getWork()['summary'])
            getWork().remove('summary')
        }
    }

    void addToWork(String field) {
        getWork()[field] = getMainEntity()[field]
    }

    void removeComparisonProps() {
        getWork().remove('editionStatement')
        getWork().remove('_numPages')
    }
}