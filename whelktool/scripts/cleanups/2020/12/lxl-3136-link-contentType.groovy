/**
 Link blank nodes in 'contentType'

 See LXL-3136
 */

import whelk.util.Statistics
import whelk.filter.BlankNodeLinker

class Script {
    static PrintWriter errors
    static BlankNodeLinker linker
    static Statistics statistics = new Statistics(5).printOnShutdown()
}

Script.errors = getReportWriter("errors.txt")
Script.linker = buildLinker()

println("Mappings:\n${Script.linker.map.sort{ it.key }.collect{it.toString()}.join('\n') }\n")
println("Ambiguous:\n${Script.linker.ambiguousIdentifiers.sort{ it.key }.collect{it.toString()}.join('\n') }}\n")

selectByCollection('bib') { bib ->
    try {
        process(bib)
    }
    catch(Exception e) {
        Script.errors.println("${bib.doc.shortId} $e")
        e.printStackTrace(Script.errors)
    }
}

void process(bib) {
    def thing = bib.graph[1]
    if(!(thing['instanceOf'] && thing['instanceOf']['contentType'])) {
        return
    }

    Script.statistics.withContext(bib.doc.shortId) {
        if(Script.linker.linkAll(thing['instanceOf'], 'contentType')) {
            bib.scheduleSave()
        }
    }
}

def buildLinker() {
    def types = ['ContentType']
    def matchFields = ['code', 'label', 'prefLabelByLang']
    def linker = new BlankNodeLinker(types, matchFields, Script.statistics)

    linker.loadDefinitions(getWhelk())
    linker.addSubstitutions(substitutions())

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
            'texto (visual)'                        : 'text',
            'imagen (fija ; bidimensional ; visual)': 'stillbild',
            'stillbilder'                           : 'stillbild',
            'kuva (still)'                          : 'stillbild',
            'kuva (kartografinen)'                  : 'kartografisk bild',
            '$atext$2rdacontent'                    : 'text',
            'karta'                                 : 'kartografisk bild',
            'kartor'                                : 'kartografisk bild',
            'image'                                 : 'stillbild',
            'musiikki (notatoitu)'                  : 'notated music',
            'música (notada)'                       : 'notated music',
            'text2rdacontent'                       : 'text',
            'cartographic image$bcri$2rdacontent'   : 'kartografisk bild',
            'sonido'                                : 'sonidos',
            'term/rda/notatedmusic'                 : 'notated music',
            'cartographic'                          : 'cartographic image',
            'computer software'                     : 'computerprogram',
            'musiknotation'                         : 'notated music',
            'still  image'                          : 'stillbild',
            'still image2rdacontent'                : 'stillbild',
            'still images'                          : 'stillbild',
            'still imge'                            : 'stillbild',
            'still-image'                           : 'stillbild',
            'still-kuva'                            : 'stillbild',
            'stillbillder'                          : 'stillbild',
            'stilllbild'                            : 'stillbild',
            'teskti'                                : 'text',
            'text txt rdacontent'                   : 'text',
            'textee'                                : 'text',
            'texto visual'                          : 'text',
            'two dimentional moving image'          : 'two-dimensional moving image',
            'txt 2 rdacontent'                      : 'text'
    ]
}

/*
not mapped (canonized values) (1154)
------------------------------------
814 texto (visual)                                               [x9wrq2r3v0v5rs93, fzr45x8r1gd9cd1, 2ldvwc5d179g6fz, p01wcgmvm0ds9mfn, kwj6ncxrhjbww7tv]
105 [imagen (fija ; bidimensional ; visual), texto (visual)]     [8k92gfg4644k75ln, h1t7kr1t1bw6mv0, bmsnpzlk8nszbxgp, xg8h5j2807v8f4t, g0sxpqds3wdh7ns]
 74 stillbilder                                                  [k3w6xkqw4mf7906, q829qmf2522k14g, j2v423qv3x87m59, 6qjrjwtj2vc3f1v, 2ldpq2xd2jz1dwb]
 29 [kuva (still)., teksti]                                      [j2v67ldv04j4714, wf7h3d775qsb0tl, sb4gl8444jsn7tb, 8slx1kkl3x4s4fk, j2v4tp8v38j45p8]
 17 [text, stillbilder]                                          [wf7gxb775jp5kg1, 4ngq5nhg48l43jg, vd6fxs562w1tbd3, p718xzz11jfr5p4, 1kcl1tqc3l1tqm1]
 15 nc                                                           [0jbnzb4b3jz18xg, n60gb5403cr6tl8, xg8mqg8809dw7c3, gzrg1qds0cp68q2, dxq36xqq544zs61]
 13 n                                                            [0jbnzb4b3jz18xg, nxjmxd1clbd51pp4, dxq2cm2q3j1l667, htpwp40bfjktfm1p, 5phtb02h25nc5qp]
  7 kuva (kartografinen)                                         [wf7dktm73ngtpr5, k3w0rlhw1d0kqtx, sb4fl774244vr6r, q825d9l25kf9q3q, j2v3btxv05fwgg5]
  4 [$atext$2rdacontent]                                         [h1t4dhzt5jgd8t4, 6qjs8sgj43f3l30, tc5gwtj51n5rghj, 7rksxp3k40mhh6q]
  4 zzz                                                          [7rkz1wbk1b4x90m, q82h82622437sg5, l4xch2wx2c5bjv8, 5phwx2dh0kqgdz9]
  3 [imagen (fija ; bidimensional ; visual). texto (visual)]     [q82b16p20g7lvp3, p71cvpn12l45ghn, q82fdb321r09nch]
  3 karta                                                        [3mfrnjxf2dn0ltc, zh9m0n992s9g4hh, fzr22mgr3bc8pfl]
  3 unmediated                                                   [xg8fpwj80l99515, nxjmxd1clbd51pp4, htpwp40bfjktfm1p]
  2 ill                                                          [5phv1ljh496j51b, zh9m7ss912vnqhh]
  2 image                                                        [dnmffqxkb784hpkr, cwp5cbhp4xvmw5d]
  2 kartor                                                       [tc5g7nf50xpx79d, dxq0dh7q1tzg6qf]
  2 musiikki (notatoitu)                                         [wf7b5cx718xhjwr, p71b5zf13rltf2l]
  2 musiktryck                                                   [pzvjxb6tm8jssdmm, fpqstztbcrb4xx5f]
  2 música (notada)                                              [kxpdz2d4hrqd7tbj, q8276hx24wp2sx8]
  2 text2rdacontent                                              [vd6jtkn60z52q0g, l4x3ltqx1d26wkd]
  2 useita sisältötyyppejä                                       [7rkz1wbk1b4x90m, q82h82622437sg5]
  2 volume                                                       [6kdsxqfl4m6n18hv, g0s50hls1f9cntj]
  1 232 p                                                        [4ngrlzlg1ll42z4]
  1 [cartographic image$bcri$2rdacontent]                        [wf7mkrh732s5jhp]
  1 [imagen (fija ; bidimensional ; visual), sonido, texto (visual)] [lx6bxwmrj016t63j]
  1 [imagen (fija ; bidimensional ; visual), sonidos, texto (visual)] [vd6nvxl612xv0xz]
  1 [imagen (fija ; bidimensional ; visual)., texto (visual)]    [gqxgjwksdgdjb1nq]
  1 [imagen (fija ; bidimensional ; visual)]                     [cwp4w6fp2phk62q]
  1 [kuva (still), teksti]                                       [0jbmjx9b4swkrd1]
  1 [teksti., kuva (still)]                                      [h1t4m7wt1d774sn]
  1 [term/rda/notatedmusic]                                      [3cwjp6k11rp5cj4k]
  1 [xviii, 428, [36] p. :]                                      [zh9m7ss912vnqhh]
  1 [существует ли на самом деле таинственный и редкий зверек лухосо, которого заказал для своего личного зоопарка олигарх игорь зуйков? исполнить его мечту взялись доставщики экзотических животных вера галкина и николай воробьев. и вот лухосо найден на берегах амазонки и доставлен олигарху. ] [w54m633ttxz88jt7]
  1 b txt                                                        [vd6nmbx63bjfwxp]
  1 c                                                            [h1t90xnt386815h]
  1 cartographic                                                 [wf7n0qs73s391qd]
  1 computer software                                            [3mfk99pf0t59nvm]
  1 cr                                                           [p71g0rd11t4hrml]
  1 cri image                                                    [wf7n0qs73s391qd]
  1 internet - monographie                                       [6qjxv90j42rx7mm]
  1 k                                                            [7rk17nxk309rtk2]
  1 ljud                                                         [5pht1pgh4d6ds1b]
  1 musiknotation                                                [j2v5f5kv14l2p5k]
  1 nicht spezifiziert                                           [l4xch2wx2c5bjv8]
  1 oförmedlad                                                   [l4x6wh2x4b610bf]
  1 online resource                                              [3mfssbqf4451ppv]
  1 s ti                                                         [8sl00m9l21wh6hj]
  1 sri                                                          [bvn4nxnn0pck8w7]
  1 st i2rdacontent                                              [q82jbf221wp74nq]
  1 still  image                                                 [sb4jj4543p8gkd4]
  1 still image2rdacontent                                       [9tm2d5xm3gpxmzg]
  1 still images                                                 [8slqd40l3cb3b39]
  1 still imge                                                   [z8xd9rbwwn3068ht]
  1 still-image                                                  [cwptwkhp27jclfd]
  1 still-kuva                                                   [3mfpcv0f1dx30hm]
  1 stillbillder                                                 [7rkvsxlk5dj2twc]
  1 stilllbild                                                   [bvnxnwxn217d6hd]
  1 stit                                                         [j2v9f00v39cnxsl]
  1 t                                                            [n60bk7x01nzlh73]
  1 teskti                                                       [frx98cszc1j950ld]
  1 text txt rdacontent                                          [0jbg77xb0cm43w4]
  1 textee                                                       [j2v5hvlv1m8nsq4]
  1 texto visual                                                 [k3w4kqzw5fq3zsb]
  1 two dimentional moving image                                 [t5wdlxqtr8qmd1rp]
  1 txt 2 rdacontent                                             [n60c9rq01s7z0s8]
  1 volym                                                        [q82b79820x4ch9n]
  1 xxx                                                          [j2v9cw1v296rt2b]
 */
