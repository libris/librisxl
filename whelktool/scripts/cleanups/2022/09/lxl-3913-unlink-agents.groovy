// Remove any links (contributions) to the listed records.

Set badAuthIDs = [
        "53hlt8kp5swj700",
        "zcr7x820wh3nsszg",
        "jgvz5wj22slrnfq",
        "hftx5b9109jr70t",
        "rp3584g92s7sftz",
        "r42bsxm6p5kzxfmb",
        "sq47ch5b5mzxpwv",
        "sq47ff6b3hctcww",
        "42gksr6n3064169",
        "mkz2bc150ws5vp0",
        "c9psxdww3m4g1wm",
        "zw9djxwh1qb17kr",
        "b8nrzclv3b40c5k",
        "dbqtzzmx2wzmhq4",
        "97mq02bt3x8kp59",
        "31fjps8m4cxxh4t",
        "vs69hv4d3x2frg1",
        "0xbd8xmj5mx9p79",
        "khwzsvc335hbf0g",
        "ljx194t4087wblf",
        "ljx14nq40d15b9d",
        "31fjng6m195sp2f",
        "97mqtj9t3j06v6c",
        "bqlt0l9b8ng2zjwz",
        "v61b0qjlsz7kx55j",
        "bmcrhjsj8zv7p1h4",
        "q22jtchnnrx9s3pg",
        "53hkf25p015s2zs",
        "42gkng5n0zlwxp5",
        "31fjn1gm5853bpr",
        "ljx01184543708h",
        "64jlgctq5gsj2ls",
        "53hlvs7p3shfcqx",
        "5k0ckm4r3t6mdq96",
        "z8cng026wjksnklw",
        "rp3551b939f7xhk",
        "khw04l535sq2k52",
        "1zcf8g9k4c7rf5l",
        "sq468wjb5dhp9sl",
        "gdsvzjv004srxmx",
        "xv8bf5hg3j1t2j9",
        "pm1357r73603fvf",
        "sq47cpsb4bpcq0s",
        "m1wc1nn4knqrtw44",
        "42gkpfhn4811ssr",
        "75kmmhwr1f1rrgz",
        "qn25c5h83lg2vp3",
        "31fjr4nm3tfmtfl",
        "hftx3s5118rltqs",
        "8mmh5lx560zs6m5w",
        "v9w8vtp6stmsmrm5",
        "b8nqmcmv505kwvm",
        "mkz256550bg2gzj"
]

String where = """
        collection = 'bib' AND deleted = false AND
        id in( select id from lddb__dependencies where dependsonid in ( '£' ) )
""".replace("£", String.join("', '", badAuthIDs))

selectBySqlWhere(where) { data ->
    //String initial = data.doc.getDataAsString()
    List titles = asList(data.graph[1]?.hasTitle).collect {
        it?.mainTitle
    }

    boolean changed = fixAllElements(data.graph, badAuthIDs, titles)
    if (changed) {
        //System.err.println("result:\n" +initial + "\nchanged into:\n"+ data.doc.getDataAsString() + "\n\n")
        data.scheduleSave()
    }
}

private boolean fixAllElements(element, badAuthIDs, titles) {
    boolean changed = false

    if (element instanceof List) {
        element.forEach { listMember ->
            changed |= fixAllElements(listMember, badAuthIDs, titles)
        }
    } else if (element instanceof Map) {
        changed |= fixElement(element, badAuthIDs, titles)
        element.keySet().forEach { key ->
            changed |= fixAllElements(element[key], badAuthIDs, titles)
        }
    }

    return changed
}

private boolean fixElement(Map element, badAuthIDs, titles) {
    if (element == null ||
            element["@id"] == null ||
            asList(element["@id"])[0] == null ||
            !asList(element["@id"])[0].endsWith("#it") ||
            element.keySet().size() != 1)
        return false

    boolean changed = false
    def linkedUri = element["@id"]

    String agentSystemId = linkedUri.substring(linkedUri.lastIndexOf("/")+1, linkedUri.length()-3) // Trim off base uri and "#it"
    if (badAuthIDs.contains(agentSystemId)) {
        selectByIds([linkedUri]) { linkedAgent ->

            // Is the instance "source consulted" by this particular agent ?
            boolean linkIsSourceConsulted = false
            asList(linkedAgent.graph[0]?.sourceConsulted).forEach { sc ->
                for (String title : titles) {
                    if (sc?.label.contains(title))
                        linkIsSourceConsulted = true
                }
            }

            // Otherwise replace the linked agent with just a local entity with name
            if (!linkIsSourceConsulted) {
                element.clear()
                for (property in ["@type", "familyName", "givenName"]) {
                    if (linkedAgent.graph[1][property] != null)
                        element.put(property, linkedAgent.graph[1][property])
                }
                changed = true
            }
        }
    }
    return changed
}

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}
