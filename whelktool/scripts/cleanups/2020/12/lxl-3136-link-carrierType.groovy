/**
 Link blank nodes in 'inScheme'

 See LXL-3390
 */

import whelk.util.Statistics
import whelk.filter.BlankNodeLinker

class Script {
    static PrintWriter modified
    static PrintWriter errors
    static BlankNodeLinker linker
    static Statistics statistics = new Statistics(5).printOnShutdown()
}

Script.modified = getReportWriter("modified.txt")
Script.errors = getReportWriter("errors.txt")
Script.linker = buildLinker()

println("Mappings:\n${Script.linker.map.sort { it.key }.collect { it.toString() }.join('\n')}\n")
println("Ambiguous:\n${Script.linker.ambiguousIdentifiers.sort { it.key }.collect { it.toString() }.join('\n')}}\n")

selectByCollection('bib') { bib ->
    try {
        process(bib)
    }
    catch (Exception e) {
        Script.errors.println("${bib.doc.shortId} $e")
        e.printStackTrace(Script.errors)
    }
}

void process(bib) {
    def thing = bib.graph[1]
    if (!(thing['carrierType'])) {
        return
    }

    Script.statistics.withContext(bib.doc.shortId) {
        if (Script.linker.linkAll(thing, 'carrierType')) {
            Script.modified.println("${bib.doc.shortId}")
            bib.scheduleSave()
        }
    }
}

def buildLinker() {
    def types = ['CarrierType']
    def matchFields = ['code', 'label', 'prefLabelByLang']
    def linker = new BlankNodeLinker(types, matchFields, Script.statistics)

    linker.loadDefinitions(getWhelk())
    linker.addSubstitutions(substitutions())
    linker.addDeletions(deletions())

    return linker
}

def getWhelk() {
    // A little hack to get a handle to whelk...
    def whelk = null
    selectByIds(['https://id.kb.se/marc']) { docItem ->
        whelk = docItem.whelk
    }
    if (!whelk) {
        throw new RuntimeException("Could not get Whelk")
    }
    return whelk
}

def substitutions() {
    [
            'gz'                  : 'mz',
            'livre'               : 'volym',
            'volumes'             : 'volym',
            'term/rda/volume'     : 'volym',
            'vo lume'             : 'volym',
            'volume 6'            : 'volym',
            'volume n'            : 'volym',
            'volume nc rdacarrier': 'volym',
            'volume2rdacarrier'   : 'volym',
            'voume'               : 'volym',
    ]
}


def deletions() {
    ['u', '#']
}

/*

not mapped (canonized values) (316493)
--------------------------------------
316181 u                       [4fdvx0172qm8p757, 3d8sfjzc1xdplbtj, 3d8sfcgk1g5x1fhn, dpk3q4w6bbt7hqkk, 1b6qc9kkz0d7cwgx]
   108 nz                      [s2zl9lctqrljtxp7, bmmsg0jc8xd3hphw, 9llrfxf175p5k1wh, w66c1j3ttwpm07np, z88f3l52wxhg97mc]
    37 gz                      [sb4hqxl43kdr8bw, cwp52hcp57rsxp9, 1kcqq2fc2763hg4, cwp22dsp0z8bxw1, r93gpvf32k2sv6x]
    21 n                       [vd6nkr663m4xrx4, jsw9rrfvg8k554fp, k3wcwzxw59k77vr, r93g44x32wrxq5x, 2b5j36x70pthfljk]
    17 livre                   [0jblt68b1rv9q5g, cwpxgtzp5t6q3bw, bvnrknxn28p17tl, 8mj18xx16dl9cxsd, 6qjm5s3j410g428]
    12 unmediated              [jsw9rrfvg8k554fp, k3wcwzxw59k77vr, 6qjzc43j39hqgmd, 2b5j36x70pthfljk, sb4jg9x407fl0jg]
    10 #                       [r4r7sg2zp1pj9h3h, 1dbtlfzvz0rvb2vc, w802c46ft4kzqt8d, q3wdzh08n4kv930m, gtkjn01vdlfxh0hl]
     9 nga                     [lwm1wkjjjg1lzhv2, grn0qkxpdsr428vr, fpzk7zlhc0xkptcv, xg8qxgm82vb861z, 6j3610fs4fjmfhfh]
     7 cz                      [v54h47pssw4blv3w, v54h426zs5lqz843, 2ldd27bd481m9lh, vd6jzhb638s6fw2, v5tq0kkks07dqf2r]
     6 annan oförmedlad bärare [fzr38g0r4dlsvxr, 4ngsz5xg24vcjcs, n60bjlw00jf5ps9, 5phtzzth0vpb18r, l4x8gjvx1zl6l2d]
     5 a                       [0jbk6kwb4rnbqj5, wf7g39r75lb04n2, xg8h6qv81r2h5cr, fzr2mb2r3hrthbs, g0s2g6ls0vj9gs3]
     5 basılı                  [q82jpl223phv629, 4f6hn9912hn5qb7w, mxmb8kp1k1c0b3kg, s2jq5094q8sld700, jthplngpgmphh1l2]
     5 boek                    [0jbk6kwb4rnbqj5, wf7g39r75lb04n2, xg8h6qv81r2h5cr, fzr2mb2r3hrthbs, g0s2g6ls0vj9gs3]
     5 c                       [p7123pf123ksvb5, 5phxwpjh4pkxckb, h1t99z1t26mfb64, 3mfgh3tf3x2ldcv, 4ngx4xwg1j3zzt8]
     4 d                       [gq8j1h58dwzq18vb, fpcnx3m9cv7bnc2m, blhknnhd8jw8wx94, 0jbsvrzb3vhf590]
     4 r                       [wd652777258krtm, gzrrp1cs59jjwqc, btmmr06n3r56w57, 0h986dnb3h0qkbn]
     4 unspecified             [7kccxx515cq4cx36, 1bkqhkpqzhzd4ghb, vd6cbzj626hr967, j2v0nb7v5b2n6cp]
     4 zu                      [7kccxx515cq4cx36, 1bkqhkpqzhzd4ghb, vd6cbzj626hr967, j2v0nb7v5b2n6cp]
     3 oförmedlad              [bvnxcxwn4xfcdhg, 8slxs0ml2trxf6f, cwp0rd2p5qhr8bg]
     3 pdf                     [cwp0463p4ndjzl3, zh9lqsg92sl44vb, p719c9z10npt5zk]
     3 volumes                 [k3w68x3w1p8pd5l, 5phv0srh46sg8f2, 3mfp4kkf4gqrwdj]
     2 nd                      [2ldrvcrd5hm7twj, 7rk17nxk309rtk2]
     2 svazek                  [cwp1x3kp473kt6k, wf7kghx73d3mqcc]
     2 sz                      [j2v6fg6v53f2fx4, w7hmx36ht6qksf3b]
     1 2 volume                [fzr319tr514krbc]
     1 2014-2016               [3mfssbqf4451ppv]
     1 380 pages               [1kcpwx1c397zrdm]
     1 [nc (= volym)]          [07w1l0khxbbsh83z]
     1 [o, q]                  [19h4vwkrzphw0pxq]
     1 audio disk              [x8383r0mvt8xm9kg]
     1 bc                      [3mfs0wxf328hq1p]
     1 c r                     [9tm3xqcm0zq9dr6]
     1 cartographic image      [vd6mh0961wfx7m9]
     1 computer                [nxh9q1vkl4kxzfxs]
     1 cre                     [xg8qxh284ncclvj]
     1 cri                     [vd6mh0961wfx7m9]
     1 inbunden                [6jfnj0fr4v7wdjgn]
     1 ljudkassett             [8slt91wl3vprl75]
     1 ljudskiva               [q82jpk824t833mj]
     1 monthly                 [3mfssbqf4451ppv]
     1 muu                     [z8v4zgs9wvhxplhs]
     1 nc 2 rdacarrier         [bvn100wn1hlvgdg]
     1 nc rdacarrier           [r93frwc3264rb61]
     1 online                  [9tmzc6mm1swnvm3]
     1 online resourc          [xg8qxh284ncclvj]
     1 other video carriers    [g0s1d5hs054gqkn]
     1 revue                   [wf7gpxk73f9ghvs]
     1 term/rda/unmediated     [4d3b3vv82srr4hsc]
     1 term/rda/volume         [2b223k190fgqqbf0]
     1 text                    [vd6fz4g606mtk9h]
     1 vo lume                 [6qjv24dj2z3x2h8]
     1 volume 6                [k3w1k00w0s0c5xh]
     1 volume n                [m5z8hq1z0nh1nhw]
     1 volume nc rdacarrier    [0jbg77xb0cm43w4]
     1 volume2rdacarrier       [l4x3ltqx1d26wkd]
     1 voume                   [p718cgp15s37t41]
     1 vz                      [g0s1d5hs054gqkn]
     1 x                       [0cddjss3xsdj04j7]
 */