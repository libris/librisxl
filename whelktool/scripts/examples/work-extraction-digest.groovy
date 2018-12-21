import org.codehaus.jackson.map.ObjectMapper

def mapper = new ObjectMapper()

format = new java.text.DecimalFormat().&format

void counter(int num) {
    print "${String.format('%12s', format(num))}\r"
}

List<String> makeTitleKeys(Map title) {
    if (!title?.mainTitle) {
        return []
    }
    def titleKeys = [ title.mainTitle ]
    if (title.subtitle)
        titleKeys.add(0, "$title.mainTitle $title.subtitle".toString())
    else if (title.titleRemainder)
        titleKeys << "$title.mainTitle $title.titleRemainder".toString()

    def titlePart = title.hasPart?.collect { [it.partNumber, it.partNname].findAll()?.join(' ') }?.join("")
    if (titlePart)
        titleKeys.add(0, "${titleKeys[-1]} $titlePart".toString())

    return titleKeys
}

String shrinkKey(String s) {
    s = s.toLowerCase()
    def chars = new LinkedHashSet(s.findAll {
            'bcdfghjklmnpqrstvwxz0123456789'.indexOf(it) > -1
        })
    return chars.size() < 3 ? s : chars.join()
}

Map<String, List<String>> index = [:]

Map aliases = [:]

new File(System.properties.extractFile).eachWithIndex { line, i ->
    def row = mapper.readValue(line, Map)

    def crekeys = []

    row.creator.each {
        if (it.bYear)
            crekeys << it.name + ' ' + it.bYear
        if (it.name)
            crekeys << it.name
    }
    if (!crekeys)
        return

    // TODO: condition the use on whether creator has role? or if all contribs lack role?
    row.contribs.each {
        if (it.role == 'author' || it.role == 'creator') {
            if (it.bYear) crekeys << it.name + ' ' + it.bYear
            crekeys << it.name
        }
    }
    // TODO: also make one key with all contribs; if that matches, that's an upper-class match!
    // (plus one sorted, that's pretty upper as well)

    def titleGroups = [[]]
    row.titles.each {
        if (it == '^') titleGroups << []
        else titleGroups[-1] << it
    }
    def tkeys = []
    tkeys += row.hasTitle.collect { makeTitleKeys(it) }.flatten().collect { shrinkKey(it) }
    if (row.instanceOf?.hasTitle)
        tkeys += row.instanceOf.hasTitle.collect { makeTitleKeys(it) }.flatten().collect { shrinkKey(it) }
    if (row.instanceOf?.expressionOf?.hasTitle)
        tkeys += row.instanceOf.expressionOf.hasTitle.collect { makeTitleKeys(it) }.flatten().collect { shrinkKey(it) }

    if (tkeys[0].size() < 2)
        return

    def obj = [
        makeTitleKeys(row.hasTitle[0])[0],
        row.creator[0].name,
        row.record,
        row.instanceType,
        row.encLevel,
        //row.genreForm
    ]

    Set<String> keySet = new HashSet<>()
    tkeys.take(3).collect { shrinkKey(it) }.unique().eachWithIndex { tkey, ti ->
        crekeys.take(3).collect { shrinkKey(it) }.unique().eachWithIndex { crekey, ci ->
            String key = "C $tkey $crekey"
            index.get(key, []) << obj
            keySet << key
            aliases[key] = keySet
            if (row.publYear) {
                String matchclass = ti == 0 && ci == 0 ? 'A' : 'B'
                String primeKey = "$matchclass $tkey $row.publYear $crekey"
                index.get(primeKey, []) << obj
                keySet << primeKey
                aliases[primeKey] = keySet
            }
            counter index.size()
        }
    }
}

// TODO: prune redundant lower-class matches where an upper-class match is found(?)
// (or at least note which high-levels are on a low-level match, only consider adding a high work on e.g. preliminary data in a low group (and discard low-only groups; or mark entire work there as "dubious?))
def iter = index.iterator()
iter.each {
    if (it.value.size() < 2) {
        iter.remove()
    } else if (aliases[it.key].any { a -> a != it.key && index[a] == it.value }) {
        iter.remove()
    } else {
        return
    }
    counter index.size()
}

println()

def jsonWriter = mapper.writerWithDefaultPrettyPrinter()
jsonWriter.writeValue(new File(System.properties.resultFile ?: '/tmp/work.json'), index)
