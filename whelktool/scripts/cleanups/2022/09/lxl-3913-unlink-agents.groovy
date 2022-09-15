// Remove any links (contributions) to the listed records.

String where = """
        collection = 'bib' AND deleted = false AND
        id in(
        select id from lddb__dependencies where dependsonid in (
        '53hlt8kp5swj700',
        'zcr7x820wh3nsszg',
        'jgvz5wj22slrnfq',
        'hftx5b9109jr70t',
        'rp3584g92s7sftz',
        'r42bsxm6p5kzxfmb',
        'sq47ch5b5mzxpwv',
        'sq47ff6b3hctcww',
        '42gksr6n3064169',
        'mkz2bc150ws5vp0',
        'c9psxdww3m4g1wm',
        'zw9djxwh1qb17kr',
        'b8nrzclv3b40c5k',
        'dbqtzzmx2wzmhq4',
        '97mq02bt3x8kp59',
        '31fjps8m4cxxh4t',
        'vs69hv4d3x2frg1',
        '0xbd8xmj5mx9p79',
        'khwzsvc335hbf0g',
        'ljx194t4087wblf',
        'ljx14nq40d15b9d',
        '31fjng6m195sp2f',
        '97mqtj9t3j06v6c',
        'bqlt0l9b8ng2zjwz',
        'v61b0qjlsz7kx55j',
        'bmcrhjsj8zv7p1h4',
        'q22jtchnnrx9s3pg',
        '53hkf25p015s2zs',
        '42gkng5n0zlwxp5',
        '31fjn1gm5853bpr',
        'ljx01184543708h',
        '64jlgctq5gsj2ls',
        '53hlvs7p3shfcqx',
        '5k0ckm4r3t6mdq96',
        'z8cng026wjksnklw',
        'rp3551b939f7xhk',
        'khw04l535sq2k52'
        ) )
"""

selectBySqlWhere(where) { data ->
    //String initial = data.doc.getDataAsString()
    boolean changed = false;
    data.graph[1].instanceOf?.contribution?.forEach { contribution ->
        def agent = contribution.agent
        if (agent != null && asList(agent["@id"])[0] != null) {
            def agentUri = asList(agent["@id"])[0]

            if (isABadLink(agentUri)) {
                selectByIds([agentUri]) { linkedAgent ->
                    contribution.remove("agent")
                    contribution.put("agent", [:])
                    for (property in ["@type", "familyName", "givenName"]) {
                        if (linkedAgent.graph[1][property] != null)
                            contribution.agent.put(property, linkedAgent.graph[1][property])
                    }
                    changed = true
                }
            }
        }
    }

    if (data.graph[1].instanceOf?.contribution != null && data.graph[1].instanceOf?.contribution.isEmpty())
        data.graph[1].instanceOf.remove("contribution")

    if (changed) {
        //System.err.println("result:\n" +initial + "\nchanged into:\n"+ data.doc.getDataAsString() + "\n\n")
        data.scheduleSave()
    }
}

boolean isABadLink(String candidate) {
    String s = candidate.substring(0, candidate.length()-3) // Trim off the #it
    return s.endsWith("53hlt8kp5swj700") ||
            s.endsWith("zcr7x820wh3nsszg") ||
            s.endsWith("jgvz5wj22slrnfq") ||
            s.endsWith("hftx5b9109jr70t") ||
            s.endsWith("rp3584g92s7sftz") ||
            s.endsWith("r42bsxm6p5kzxfmb") ||
            s.endsWith("sq47ch5b5mzxpwv") ||
            s.endsWith("sq47ff6b3hctcww") ||
            s.endsWith("42gksr6n3064169") ||
            s.endsWith("mkz2bc150ws5vp0") ||
            s.endsWith("c9psxdww3m4g1wm") ||
            s.endsWith("zw9djxwh1qb17kr") ||
            s.endsWith("b8nrzclv3b40c5k") ||
            s.endsWith("dbqtzzmx2wzmhq4") ||
            s.endsWith("97mq02bt3x8kp59") ||
            s.endsWith("31fjps8m4cxxh4t") ||
            s.endsWith("vs69hv4d3x2frg1") ||
            s.endsWith("0xbd8xmj5mx9p79") ||
            s.endsWith("khwzsvc335hbf0g") ||
            s.endsWith("ljx194t4087wblf") ||
            s.endsWith("ljx14nq40d15b9d") ||
            s.endsWith("31fjng6m195sp2f") ||
            s.endsWith("97mqtj9t3j06v6c") ||
            s.endsWith("bqlt0l9b8ng2zjwz") ||
            s.endsWith("v61b0qjlsz7kx55j") ||
            s.endsWith("bmcrhjsj8zv7p1h4") ||
            s.endsWith("q22jtchnnrx9s3pg") ||
            s.endsWith("53hkf25p015s2zs") ||
            s.endsWith("42gkng5n0zlwxp5") ||
            s.endsWith("31fjn1gm5853bpr") ||
            s.endsWith("ljx01184543708h") ||
            s.endsWith("64jlgctq5gsj2ls") ||
            s.endsWith("53hlvs7p3shfcqx") ||
            s.endsWith("5k0ckm4r3t6mdq96") ||
            s.endsWith("z8cng026wjksnklw") ||
            s.endsWith("rp3551b939f7xhk") ||
            s.endsWith("khw04l535sq2k52")
}

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}