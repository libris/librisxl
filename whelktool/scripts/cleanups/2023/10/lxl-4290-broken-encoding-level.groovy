
// Taken from
// def where = """
// data#>>'{@graph,0,encodingLevel}' like '%→%'
// """

Collection<String> ids = [
        "hzvlk0p2f983jpk0",
        "r680tg92p9g7fpm4",
        "fwnxlb7vc4pv56tj",
        "2f6vnqrp0bz8fnwc",
        "dt10j33mb9wpl9wz",
        "4ldczhpl2w3gphnq",
        "wcz9751gtjnd8vd3",
        "dvgxml1sb73nsx1t",
        "wczjhxckt4mcm2bb",
        "5ks0vkhc3wl2x99c",
        "1h68pk6kzkflh2wx",
        "8qb4mmk2606cqh0n",
        "t82p5421r00hxgkf",
        "6np3wlv0459519v4",
        "1h5q7xbszd3bmmtw",
        "9p5kf83r71xs6zrs",
        "p2x3hc7tm7flwskj",
        "7pf4t8cb5tck67k3",
        "fvp77tkrc5qfgj92",
        "1fqs7p2szmttr675",
        "s6h99w7vqzjjj66q",
        "m15j8437kvdp52xv",
        "hwdg5rzhftvdddh6",
        "2h3dsqfz0mzgxp3m",
        "kvjvjc54hcb00drl",
        "2hgnfhsg0z5g5s6p",
        "q57xbt24nwn17k67",
        "1g9lqbfsz70d7th5",
        "bsbtlcm988b7xxt7",
        "cm37v47r9qwrk563",
        "3jnscqfb10fcnmzx",
        "n0gppz5nl7399vkv",
        "s820nfwqq08j1kg3"
]

def replaceMap = [
        'Biblioteksnivå → Nationalbibliografisk nivå' : 'marc:FullLevel',
        'Biblioteksnivå → Miniminivå' : 'marc:AbbreviatedLevel',
        'Förhandsinformation → Miniminivå' : 'marc:AbbreviatedLevel',
        'Förhandsinformation → Biblioteksnivå' : 'marc:MinimalLevel',
        'Preliminär → Biblioteksnivå' : 'marc:MinimalLevel',
        'Miniminivå → Biblioteksnivå' : 'marc:MinimalLevel',
        'Miniminivå → Preliminär' : 'marc:PartialPreliminaryLevel'
]

selectByIds(ids) {
    def record = it.graph[0]
    def encodingLevel = record['encodingLevel']
    if (replaceMap.keySet().contains(encodingLevel)) {
        record['encodingLevel'] = replaceMap[encodingLevel]
        it.scheduleSave()
    }
}