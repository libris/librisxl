/**
 Link blank nodes in 'mediaType'

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
    if(!thing['mediaType']) {
        return
    }

    Script.statistics.withContext(bib.doc.shortId) {
        if(Script.linker.linkAll(thing, 'mediaType')) {
            bib.scheduleSave()
        }
    }
}

def buildLinker() {
    def types = ['MediaType']
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
            'ei välittävää laitetta'        : 'käytettävissä ilman laitetta',
            'ei välittävää laitettass'      : 'käytettävissä ilman laitetta',
            'compute'                       : 'computer',
            'oförmedlad'                    : 'omedierad',
            'no mediado'                    : 'omedierad',
            'ofärmedlad'                    : 'omedierad',
            'ingen medietyp'                : 'omedierad',
            'unmediated$2rdamedia'          : 'omedierad',
            'sem mediaã§ã£o'                : 'omedierad',
            'sans m©♭diation'               : 'omedierad',
            'unmediate d'                   : 'omedierad',
            'no mediat'                     : 'omedierad',
            'term/rda/unmediated'           : 'omedierad',
            'unmedia ted'                   : 'omedierad',
            'ohne hilfsmittel zu benutzen$b': 'omedierad',
            'inmediated'                    : 'omedierad',
            'nmediated'                     : 'omedierad',
            'u nmediated'                   : 'omedierad',
            'umediated'                     : 'omedierad',
            'ummediated'                    : 'omedierad',
            'unmediat ed'                   : 'omedierad',
            'unmediated n rdamedia'         : 'omedierad',
            'unmediated2rdamedia'           : 'omedierad',
            'unmedicated'                   : 'omedierad', // LOL
            'unmeditated'                   : 'omedierad', // LOL
    ]
}


/*
not mapped (canonized values) (6843)
------------------------------------
2833 ei välittävää laitetta           [r939bzx32hzv4gc, dxqvpdvq1qbbc7t, q825ph7228kgkpq, tc5fv8551gw3tdx, bvnx1d8n4hwwtw3]
2306 oförmedlad                       [wf7kxfw73sc6s55, dxq18mqq5ptghvn, zh9mz4p93n42k5f, g0s4rffs4rjv12v, n60b6hx05jtgpg9]
1468 ebook                            [2ldgjmgd19vwgd4, wf7b7kn73m74j7v, g0svkzms1g56h1s, xg8clm083gh8rhn, 9tmq7fjm2qbs8dw]
  57 ingen medietyp                   [g0sw35fs4p1c3z1, 8slvx60l5fs3crq, vd6g2hr6383kqkn, q828s4q24dd0v9s, xg8hfpf81kx1j4r]
  19 sti                              [q2f9vtjsnrgvpzpd, vd6nkr663m4xrx4, g0s4p5ms3nvcrzk, 4gbr66k12j05w810, n60fr4704mklnnv]
  15 no mediado                       [gsvwcpbbdjnvl68l, 7rkwf0bk5tbv8j5, r27qmvwpp73rgpgp, 7rktpg3k0hsbt1z, 2ddl50z90sp55d0v]
  13 elektroninen                     [cwpszw2p1gcl7b3, q82brw924b8nzjg, q82734824v8clhr, k3w2w8kw3vcrnd7, g0swqm8s4ldhf4t]
  12 r                                [wf7nqfh73xhhw3w, fzr7x20r1wf01wh, cwpq5h8p27tljp2, g0sxkkrs5swp8dd, xg8n1mt80hfjsvh]
   8 a                                [hs9r748lf36x5z5m, ht0wgpl7f3k8lhvl, dxq33zjq0sndmj3, zh9npc191vb9xz4, 9tm0014m4qx8dnc]
   8 still image                      [4gbr66k12j05w810, 9tm33v2m1z5t1fv, p2xtnv63m53hjfdj, bmbgfvnk8vc4m97l, wf7mnc475k2thdp]
   7 b                                [l4xcvszx1s9wbx5, bvnz6tfn0882689, zb03d5z4wzcx12xf, wf7jrdf75rwrvpx, 3csxzqlc1nlssl40]
   5 kağıt                            [q82jpl223phv629, 4f6hn9912hn5qb7w, mxmb8kp1k1c0b3kg, s2jq5094q8sld700, jthplngpgmphh1l2]
   5 stillimage                       [q2f9vtjsnrgvpzpd, 9m87p9lh7j1v0g8j, j2v6tdgv5nbpkq9, cpt8rh959524xl3k, 4f47q4z6254jfvcd]
   5 text                             [vd6fvn9649tg53v, fzrvk4hr5cp0cbx, xg8mv9282qxfjmz, lwh00qbvjtdflv1q, r93cm9633q6c551]
   5 txt                              [4ngwd3lg2shrmfd, 2ldtb1gd5m7jvrn, fzr76wpr588t3hq, cwp516mp4fj0q2f, lwh00qbvjtdflv1q]
   4 [sem mediaã§ã£o]                 [l4xcxr0x3hpbhs4, k3wb3t2w3mjxjgh, n60f24l03x8vst9, 2ldt27vd1cg2gkz]
   4 [unmediated$2rdamedia]           [h1t4dhzt5jgd8t4, 6qjs8sgj43f3l30, tc5gwtj51n5rghj, 7rksxp3k40mhh6q]
   4 c                                [kxjpm9plhjbkcv30, 6jdpk1w74th7h9kw, p23gpvlxmrnhjd96, p07dz8m2mlcxzhpw]
   4 elektronisk                      [zh9f2df92jt3hmh, xg8jk0480wfss2p, 1kch649c0dpf8wb, zh9jnpr94vpd66v]
   4 g                                [2ldp1qkd24z1tmt, fpzgrxwdc419ktkt, wf7nlj77317fg23, xg8mldm812jhfb1]
   3 [sans m©♭diation]                [n607b6j05h1qhsr, zh9j4nq94qxptf7, dxqwvfzq3zfs721]
   3 paket                            [s3c35gj0qcftd5vf, 9tm1v0qm1frtbp0, 4fpfc2tn2jj4644w]
   3 unmediate d                      [5phnm20h4jnlfg2, m5z56v4z48k79lj, dxqzrkwq1jvf1hc]
   3 y                                [k3wbjvhw2jm3ghl, zh9nsq691vb2cjq, 8sl0x3kl4b7rxqz]
   2 bez média                        [cwp1x3kp473kt6k, wf7kghx73d3mqcc]
   2 komputer                         [kxjpm9plhjbkcv30, 6jdpk1w74th7h9kw]
   2 no mediat                        [n606rfn03t25hpb, 8slx80ml181l91q]
   2 stillbilder                      [3mfqmq0f4nhdw7r, n60904x01mg74l6]
   2 term/rda/unmediated              [4d3b3vv82srr4hsc, 2b223k190fgqqbf0]
   2 unmedia ted                      [3mfq037f1wnmsw0, q82cgq021xmqw7w]
   1 2                                [r93f05s33l446rm]
   1 [ohne hilfsmittel zu benutzen$b] [l4x94bgx4gg1kth]
   1 cartographic image               [tc5lhh050p58sxn]
   1 compute                          [r93f05s33l446rm]
   1 cri                              [vd6l29665chb7cl]
   1 ei välittävää laitettass         [r93cbm1335nfghc]
   1 electrónico                      [7rks030k5r5td3w]
   1 harmathèque                      [6qjxv90j42rx7mm]
   1 ingen medityp                    [8slx5qwl2fvkbhp]
   1 inmediated                       [bvn3kvtn4jm5mxq]
   1 ljudupptagning                   [gqnj9665ddz4qrs1]
   1 monografi                        [6g6fxvc84tvs4kmj]
   1 musiktryck                       [dnnxj3czbt2rqk3b]
   1 n rdamedia                       [l4xd81mx1p194kt]
   1 nmediated                        [8slzpv7l0h8lm8z]
   1 ofärmedlad                       [8sltjdsl40zqq9w]
   1 pdf                              [3mfssbqf4451ppv]
   1 playstation 4                    [p23gpvlxmrnhjd96]
   1 rdamedia                         [1kctpwgc3d06m2z]
   1 s                                [p0r2f723m7xk3j1r]
   1 sans médiatio                    [8slxhksl5m8n5sq]
   1 särtryck                         [kvplj5wvhcs3xmgg]
   1 term/rda/unmediated
             [mxw23fdhkgdzvfgr]
   1 tidskrift (bilaga)               [7kdwdr9q5xvjmjpk]
   1 u nmediated                      [tc5d80j51wnh67g]
   1 umediated                        [fzr7fv2r33j4fx6]
   1 ummediated                       [fzr7f82r05n88vk]
   1 unmediat ed                      [7rksmr4k01cww2g]
   1 unmediated n rdamedia            [0jbg77xb0cm43w4]
   1 unmediated2rdamedia              [l4x3ltqx1d26wkd]
   1 unmedicated                      [tc5c06c53wmw8cv]
   1 unmeditated                      [6qjw35rj0mp87rs]
   1 x                                [h1t9gxct4hmptw3]
 */
