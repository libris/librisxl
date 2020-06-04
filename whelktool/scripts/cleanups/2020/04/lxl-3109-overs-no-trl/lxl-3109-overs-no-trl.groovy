/**
 * Try to add 'contribution' with role translator where it is missing by parsing it from 'responsibilityStatement'.
 *
 * Process documents that have
 * - marc:ItemIsOrIncludesATranslation
 * - no contribution with role https://id.kb.se/relator/translator
 * - "övers..." somewhere in responsibilityStatement
 *
 * First build a list of linkable translators by finding all persons in auth that have a unique name and either
 * - 'översättare' in the description
 * - five or more contributions with role https://id.kb.se/relator/translator
 *
 * For each name listed as 'översättare' in responsibilityStatement
 * a) check if it's already in contributions without role translator
 * b) see if there is a linkable agent
 * c) create a local entity for the agent
 *
 * There is a small risk of false positives here, i.e. linking to the wrong agent. 
 *
 * The concept of "name" in this script is specific to what is found in this
 * subset of Libris data and not any general handling of names...
 *
 * Could (should?) be generalized to handle e.g. illustrator, foreword, etc...
 *
 * See LXL-3109 for more info
 */

import org.apache.commons.lang3.StringUtils
import whelk.util.DocumentUtil
import whelk.util.Statistics
import whelk.util.Unicode

import java.util.concurrent.ConcurrentLinkedQueue

// workaround for weird scoping in groovy scripts
class Script {
    static final int MIN_CONTRIBUTIONS = 5
    static final boolean GENERATE_BLANK_AGENTS = true

    static final List<String> BIOGRAPHICAL_FIELDS = [
            'description',
            'fieldOfActivity',
            'hasOccupation',
            'marc:titlesAndOtherWordsAssociatedWithAName'
    ]

    static final stats = new Statistics().printOnShutdown()
}

TRL = 'https://id.kb.se/relator/translator'
FORMS = buildForms()
testParser()

scheduledForUpdate = getReportWriter("scheduled-for-update")

translators = loadTranslators()


File bibIDsFile = new File(scriptDir, 'overs-bibs.txt')
if (bibIDsFile.exists()) {
    selectByIds( bibIDsFile.readLines() ) { bib ->
        try {
            process(bib)
        }
        catch(Exception e) {
            e.printStackTrace()
        }
    }
} else {
    selectByCollection('bib')  { bib ->
        try {
            process(bib)
        }
        catch(Exception e) {
            e.printStackTrace()
        }
    }
}

void process(bib) {
    def work = getWork(bib)

    if(!work || !work.contribution || work.hasPart) {
        return
    }

    String responsibilityStatement = bib.graph[1].responsibilityStatement ?: ""

    if(work['marc:languageNote'] == "marc:ItemIsOrIncludesATranslation"
            && noTranslator(work.contribution ?: [])
            && responsibilityStatement.contains('övers')
    ) {
        tryAddTranslators(bib, work, responsibilityStatement)
    }
}

void tryAddTranslators(bib, Map work, String responsibilityStatement) {
    List<String> respTrlNames = parseTranslatorNames(responsibilityStatement)
    List<String> contributorNames = work['contribution']['agent'].collect{ getNames(it) }.flatten()

    String msgBase = "${bib.doc.getURI()}\t$responsibilityStatement\t-->\t"
    String msg = ""
    respTrlNames.each { name ->
        if (name in contributorNames) {
            addRoleToContribution(work.contribution, name, TRL)
            msg += "$msgBase add role to existing: $name\n"
            Script.stats.increment('names', 'add role to existing')
        }
        else if (translators.containsKey(name)) {
            def id = translators[name]
            work.contribution << ['@type': 'Contribution', agent: ['@id': id + '#it'], role: [['@id': TRL]]]
            msg += "$msgBase add link to: $name\t$id\n"
            Script.stats.increment('names', 'link to auth')
            Script.stats.increment('z - link to auth', name)
        }
        else if (Script.GENERATE_BLANK_AGENTS && tryMakeBlankContribution(name)) {
            work.contribution << tryMakeBlankContribution(name)
            msg += "$msgBase add blank node: $name\n"
            Script.stats.increment('names', 'add blank node')
            Script.stats.increment('x - blank names', name)
        }
        else {
            Script.stats.increment('names', 'unhandled')
            Script.stats.increment('y - unhandled names', name)
        }
    }

    if (!msg.isEmpty()) {
        scheduledForUpdate.println(msg)
        bib.scheduleSave()
    }

    Script.stats.increment('tot', 'docs')
    Script.stats.increment('names per doc', respTrlNames.size())
}

Map tryMakeBlankContribution(String name) {
    // For now, only handle the simple case "Firstname Lastname"
    def simpleName = /\p{javaUpperCase}\p{javaLowerCase}[^. ]*/
    (name =~ /($simpleName) ($simpleName)/).with {
        if (matches()) {
            [
                    '@type': 'Contribution',
                    agent  : ['@type': 'Person', givenName: group(1), familyName: group(2)],
                    role   : [['@id': TRL]]
            ]
        }
    }
}

List parseTranslatorNames(String responsibilityStatement) {
    responsibilityStatement
            .split(';')
            .grep{ it.contains('övers') }
            .collect(Unicode.&trimNoise)
            .collect(StringUtils.&normalizeSpace)
            .findResults { getNamesPart(it) }
            .collect{ splitPersons(it) }
            .flatten()
}

List<String> splitPersons(String names) {
    def name = /\p{javaUpperCase}\S+/
    def m = names =~ /(?<given1>$name) och (?<given2>$name) (?<family>$name)/ // e.g. Brita och Alf Agdler
    if (m.matches()) {
        def name1 = "${m.group('given1')} ${m.group('family')}".toString()
        def name2 = "${m.group('given2')} ${m.group('family')}".toString()
        return [name1, name2]
    }

    return names.split("och|,|&").collect{ it.trim() }
}

String getNamesPart(String respPart) {
    def result = FORMS.findResult { (respPart =~ it).with { matches() ? group(1).trim() : null } }

    if (!result) {
        println('unhandled:' + respPart)
    }

    return result
}

boolean noTranslator(def contribution) {
    boolean hasTranslator = false
    DocumentUtil.findKey(contribution, '@id') { value, path ->
        if (value == TRL) {
            hasTranslator = true
        }
        DocumentUtil.NOP
    }

    return !hasTranslator
}

void addRoleToContribution(List contributions, String agentName, String id) {
    contributions.each { contribution ->
        if (agentName in getNames(contribution['agent'])) {
            contribution.role = (contribution.role ?: []) << ['@id': id]
        }
    }
}

List<String> getNames(Map person) {
    person['@id']
            ? getNamesLocal(loadThing(person['@id']))
            : getNamesLocal(person)
}

List<String> getNamesLocal(Map person) {
    ([person] + (person.hasVariant ?: []))
            .collect{ getName(it)}
            .grep()
            .collectMany { [it, it.replaceAll('-', ' ')] }
            .unique()
}

String getName(Map person) {
    if (person['@type'] == 'Person' && person.givenName && person.familyName) {
        "${person.givenName} ${person.familyName}"
    }
}

Map loadThing(def id) {
    def thing = [:]
    selectByIds([id]) { t ->
        thing = t.graph[1]
    }
    return thing
}

List buildForms() {
    // These cover most of what is found in the data, but not everything
    // Could replace this with a list of prefixes because names are almost always last

    // Some of these could be more compact, but try to keep them a bit readable
    
    def nameWord = /(?:al-)?(?:\p{javaUpperCase}\S*|och|&|van|von|der|de|la|af|f.)/
    def names = /(?:\s*$nameWord|,|, )+/

    def nocase = '(?i)'
    def aukt = /(?:aukt. |auktor. |auktoris. |auktoriserad )/
    def bemynd = /(?:bemynd. |bemynd |bemyndigad )/
    def svensk = /(?:svensk )/
    def i = /(?:i )/
    def translation = /övers(?:att|ättning|\.|a)?/
    def type = /(?:${aukt}|${bemynd}|${svensk})/
    def from = /(?: från (?:det )?\S+(?: orig.| originalet)?)/
    def to = /(?: till svenska)/
    def by = /(?: av | av:| af | :|:)/

    return [
            ~/$nocase$i?$type*$translation$from?$to?$by($names)/,
            ~/$nocase$i?$type*$translation${by}($names)/,
            ~/översättningen är utförd av ($names)/,
            ~/övers. är utförd av ($names)/,
            ~/översättningar: ($names)/,
            ~/översättningar av ($names)/,
            ~/översättare: ($names)/,
            ~/översättning \.\.\. ($names)/,
            ~/$nocase${i}?${type}?${translation}${from}?${to}? ($names)/,
    ]
}

void testParser() {
    // Examples of different forms encountered
    [
            'Aukt. övers. av Anna Bagge'                                                     : ['Anna Bagge'],
            'auktor. övers. av Louis Renner och Ture Nerman'                                 : ['Louis Renner', 'Ture Nerman'],
            'aukt. övers. av Oscar Ljungström'                                               : ['Oscar Ljungström'],
            'auktoriserad översättning av C. G. Segerstråle'                                 : ['C. G. Segerstråle'],
            'auktoris. övers. av Karl August Hagberg'                                        : ['Karl August Hagberg'],
            'auktor. övers. från det engelska orig. av Allan Grandin'                        : ['Allan Grandin'],
            'Bemynd. övers. av Siri Thorngren-Olin'                                          : ['Siri Thorngren-Olin'],
            'Bemynd. övers. från engelskan av Arne Lönnbeck'                                 : ['Arne Lönnbeck'],
            'Bemyndigad övers. från engelska originalet av Erik Karlholm'                    : ['Erik Karlholm'],
            'bemynd. övers. från engelskan av Sven Hallström'                                : ['Sven Hallström'],
            'Bemynd. övers. av Karin Jensen f. Lidforss'                                     : ['Karin Jensen f. Lidforss'],
            'Bemynd övers. av Vera von Kræmer'                                               : ['Vera von Kræmer'],
            'svensk översättning: Anna Marolt'                                               : ['Anna Marolt'],
            'i svensk översättning av Erik Åhrberg'                                          : ['Erik Åhrberg'],
            'i översättning av Boo Cassel'                                                   : ['Boo Cassel'],
            'övers av Lars Erik Sundberg'                                                    : ['Lars Erik Sundberg'],
            'övers från holländska Betsy van Spiegel'                                        : ['Betsy van Spiegel'],
            'övers. Astrid Borger'                                                           : ['Astrid Borger'],
            'övers. av Agneta Hebbe'                                                         : ['Agneta Hebbe'],
            'övers. av Bo Kärre och Magnus Faxén'                                            : ['Bo Kärre', 'Magnus Faxén'],
            'övers. av Brita och Alf Agdler'                                                 : ['Brita Agdler', 'Alf Agdler'],
            'övers. från engelskan av Axel Palmgren'                                         : ['Axel Palmgren'],
            'övers. från kanadensiska originalet av Karin Färnström'                         : ['Karin Färnström'],
            'övers. från spanskan: Anders Hallström'                                         : ['Anders Hallström'],
            'översättning från franskan af Louise Fraenckel'                                 : ['Louise Fraenckel'],
            'övers.: Carl Braunerhielm'                                                      : ['Carl Braunerhielm'],
            'övers.: Carl-Erik Lindgren, Gustaf-Adolf Mannberg och Pär Rådström'             : ['Carl-Erik Lindgren', 'Gustaf-Adolf Mannberg', 'Pär Rådström'],
            'övers.: Elisabeth och Sven Johansson'                                           : ['Elisabeth Johansson', 'Sven Johansson'],
            'översatt av Kerstin Törngren'                                                   : ['Kerstin Törngren'],
            'översatt av Sabina Cleman & Charlotta Cleman'                                   : ['Sabina Cleman', 'Charlotta Cleman'],
            'översatt från danska av Nils Kalén'                                             : ['Nils Kalén'],
            'översättare: Björn Lundqvist'                                                   : ['Björn Lundqvist'],
            'översättning Torsten M. Nilsson'                                                : ['Torsten M. Nilsson'],
            'översättning av Aida Törnell'                                                   : ['Aida Törnell'],
            'översättning till svenska av Mats-Peter Sundström'                              : ['Mats-Peter Sundström'],
            'översättning till svenska: Stanley Almqvist'                                    : ['Stanley Almqvist'],
            'översättning: Aina Larsson'                                                     : ['Aina Larsson'],
            'översättning: Amelie Björck, Patricia Lorenzoni och Maria Åsard'                : ['Amelie Björck', 'Patricia Lorenzoni', 'Maria Åsard'],
            'översättningen är utförd av Gustaf Lundgren'                                    : ['Gustaf Lundgren'],

            // trickier cases that we don't handle for now. Don't want to touch them so should return empty
            'övers. från kinesiska och komment. av Kuma-san'                                 : [],
            'översatt av Lars-Olov Stråle i samarbete med Claes Andersson och Conny Cronholm': [],
            'översättning av Anton Karlgren, översedd av Sven Vallmark'                      : [],
            'övers. från italienska och förord av Eva Wennerström-Hartmann'                  : [],
            'övers. och bearb.: Henrik Wranér'                                               : [],
            'i översättning från kinesiska och med efterskrift av Göran Sommardal'           : [],
            'översättning och bearbetning av Jan Åhlander'                                   : [],
    ].each { k, v ->
        assert parseTranslatorNames(k) == v
    }
}


/*
 * ====================================================================================================================
 */
Map loadTranslators() {
    File translatorsFile = new File(scriptDir, 'translators.txt')
    def translators = [:]

    if (!translatorsFile.exists()) {
        buildTranslatorList(translatorsFile)
    }

    translatorsFile.splitEachLine('\t') { id, name ->
        translators[name] = id
    }

    return translators
}

void buildTranslatorList(File file) {
    System.err.println("Building list of agents...")

    UniqueMap allAgentNames = new UniqueMap()
    Queue translators = new ConcurrentLinkedQueue()

    selectByCollection('auth') { auth ->
        Map person = auth.graph[1]

        if (!person['@type'] == 'Person') {
            return
        }

        String id = auth.doc.getURI()
        List names = getNames(person)

        names.each {
            allAgentNames.put(it, id)
        }

        if(isTranslator(person)) {
            names.each {
                translators << [name: it, id: id]
            }
        }
    }

    translators.each {
        if(!allAgentNames.isUnique(it.name)) {
            it.name = "# NOT UNIQUE: ${it.name}"
        }
    }

    file.withPrintWriter { writer ->
        translators.collect().sort { a, b -> a.name <=> b.name}.each {
            writer.println("${it.id}\t${it.name}")
        }
    }
}


boolean isTranslator(Map person) {
    boolean isTranslator = false
    DocumentUtil.traverse(person) { value, path ->
        if (value && value instanceof String && value.toLowerCase().contains('översättare')) {
            if (path.first() in Script.BIOGRAPHICAL_FIELDS) {
                isTranslator = true
            }
            DocumentUtil.NOP
        }
    }
    return isTranslator || (TRL in getContributionRoles(person['@id']))
}

List getContributionRoles(String iri) {
    def where = """
        id in (
            select d.id
            from lddb__dependencies d, lddb__identifiers i
            where i.iri = '$iri'
            and i.id = d.dependsonid
            and d.relation = 'agent'
        )""".stripIndent()

    Queue roles = new ConcurrentLinkedQueue()
    selectBySqlWhere(where, silent: true) { bib ->
        def work = getWork(bib)
        work?.contribution.each {
            if (it['agent'] && it['role'] && (withoutHash(it['agent']['@id']) == withoutHash(iri))) {
                roles << it.role['@id']
            }
        }
    }

    roles.collect().flatten().grep().countBy {it}
            .findAll { role, count -> count >= Script.MIN_CONTRIBUTIONS}
            .collect {role, count -> role}
}

String withoutHash(String uri) {
    (uri && uri.contains('#'))
            ? uri.substring(0, uri.indexOf('#'))
            : uri
}

class UniqueMap {
    Map map = [:]
    Map<String, List> ambiguousIdentifiers = [:]

    synchronized void put(String from, String to) {
        from = from.toLowerCase()
        if (ambiguousIdentifiers.containsKey(from)) {
            ambiguousIdentifiers[from] << to
        } else if (map.containsKey(from) && map.get(from) != to) {
            ambiguousIdentifiers[from] = [to, map.remove(from)]
        } else {
            map[from] = to
        }
    }

    boolean isUnique(String id) {
        !ambiguousIdentifiers.containsKey(id.toLowerCase())
    }

    String get(String key) {
        map.get(key)
    }
}

/*
 * ====================================================================================================================
 */

Map getWork(def bib) {
    def (record, thing, work) = bib.graph
    if (thing && isInstanceOf(thing, 'Work')) {
        return thing
    }
    else if(thing && thing['instanceOf'] && isInstanceOf(thing['instanceOf'], 'Work')) {
        return thing['instanceOf']
    }
    else if (work && isInstanceOf(work, 'Work')) {
        return work
    }
    return null
}