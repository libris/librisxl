/*

 
*/

import whelk.util.DocumentUtil

// SPARQL query for generating id (with #it) list
// 
//    SELECT ?g {
//      ?g bf2:instanceOf*/bf2:subject ?cs .
//      ?cs a madsrdf:ComplexSubject ;
//        madsrdf:componentList/rdf:first ?topic .
//      ?topic a ?x;
//        ?p "Busiga barn".
//      FILTER (!isBlank(?g))
//    }

BUSIG = ['@id': 'https://id.kb.se/term/barn/Busig']

selectByIds(ids()) { doc ->
    boolean changed = DocumentUtil.traverse(doc.graph) { value, path ->
        if (value instanceof Map && 'Busiga barn' == getPathSafe(value, ['termComponentList', 0, 'prefLabel'])) {
            return new DocumentUtil.Replace(BUSIG)
        }
    }
    
    if (changed) {
        doc.scheduleSave(loud: true)
    }
}

private Object getPathSafe(item, path, defaultTo = null) {
    for (p in path) {
        if (item[p] != null) {
            item = item[p]
        } else {
            return defaultTo
        }
    }
    return item
}

def ids() {
    [
            "6bjj4g0c3l741cd",
            "15cczf9606294j9",
            "bgnn24wh426scpg",
            "tz55kpd01cj8mzf",
            "bgnsx46h1wz8c0h",
            "1kcdm1zc3vs4hlz",
            "zg88s329042pn3z",
            "g0stbwks50fwkpb",
            "m5zzgf7z5vlbtsc",
            "tc5681v52x86q65",
            "zh9btts958rlq07",
            "q823ph820xpb7vd",
            "h1ttvvst281jsfd",
            "7qjjqz9k5d5r46q",
            "6phgdkmj5lr7jhn",
            "1jb9tbmc1661gf0",
            "h0sr9s2t29z3hwc",
            "k2vsxddw09m0tc2",
            "p60vl0n1308jbk8",
            "r82112r31rhpvhm",
            "0h995j1b11q8dxr",
            "s933pdd433ckc16",
            "bvnph6jn33s9r2v",
            "bvnpbxnn3zh34zx",
            "5phhb5wh35lj6gm",
            "g0std2ts3qbcq71",
            "cvnnc0kp1fqlmjv",
            "5ngg05kh0nvjj6h",
            "5nggkt6h25z5xpq",
            "8rkf5x2l0g1kqth",
            "l4xx1lgx00pmpj0",
            "vc54l6s633tf3jm",
            "k3wx4g5w3qvrzt9",
            "btmmz61n4lqjl9w",
            "tb442m854t8df18",
            "zh99r56946rrtrg",
            "3mfgnvsf3ttl0nt",
            "btml4mxn2czpjbh",
            "7qjhws9k2t63nzb",
            "1jbbnwvc3n3rxhh",
            "7qjjmj7k3s2r4l0",
            "fzrtv0rr2bvm7zr",
            "3lddjlhf4dngrjc",
            "wf77nd275mf00sq",
            "4nggmfcg4czf0b6",
            "vd67rs463jwg7tx",
            "cwpq66np3phfcv6",
            "vc54p5f62lv6m7g",
            "9sll8vhm1ttfjmc",
            "s933sxh43frddjj",
            "wf78rrq701jzjrg",
            "q82210t20crsdr6",
            "j2vwcp0v1v2dgnj",
            "vc525dw64851rp5",
            "tb44pdx50pxkwz9",
            "vd67frj62xrjxr8",
            "cvnjhjbp2v049nd",
            "vd68l1t63xd5cs5",
            "xf775qb80zv78bn",
            "k3wx4b4w214193h",
            "5phk3j4h0591qsj",
            "2ldhnq4d2b98ct9",
            "p60vl0l14vrv5zg",
            "sb4716d44xm7b90",
            "j1tt40pv1h8xz6c",
            "7qjj9vnk5v9sntt",
            "g0st97qs42zs3h9",
            "s933mwg40d5njc6",
            "7qjh2c6k4r75tgk",
            "2ldgsmrd21zcfhb",
            "n60c2x900k7k760",
            "l3ww13zx2b8954t",
            "tc56qr3502m51c6",
            "4mf98qtg1p7kt2g",
            "h0ssv16t1rpbps3",
            "k3wwlw9w5lc72xb",
            "m5z0jkvz1kq6h7l",
            "h0ssspgt09w0wzz",
            "cvnnf0tp2lzwm4v",
            "q825z4921jhdb3d",
            "5ngf4zjh5cfczbl",
            "p7116rq10hc24s0",
            "3mffjcvf07cczs3",
            "8rkkx3jl3d9cf0p",
            "8sll2gjl3rrk9fx",
            "wf77p6x71h7ljb2",
            "9tmpphqm2ptkv2h",
            "dxqrm0kq5qpvj9g",
            "q711jnl238djl6k",
            "tb43qjd558pnsrp",
            "gzrq41js2bd5xs5",
            "r936f93324n55g9",
            "vc55zts62pg253n",
            "tc56nsc51x6f000",
            "cvnks5cp1xk7vl8",
            "wd648qq71g0l2h8",
            "k2vtjc2w116kfg2",
            "0jbcjl8b28fl6c9",
            "m5z03vvz3jdq0tm",
            "2kcc6bvd0plrk5c",
            "l4xz2ttx5fprjc5",
            "4mffcv6g2xx3n2p",
            "j1tttqhv3gh76jp",
            "wf77q9j714l1bwr",
            "zg87k7b9518hh23",
            "2kcckszd56j0kgp",
            "1jbb46rc0q8qkq1",
            "5phldkrh47xt3hm",
            "n600pcl05q0527q",
            "l3wwtd0x45h09w8",
            "k3wwzfxw4z2fpx7",
            "vd66skk63gh8nlf",
            "gzrmllms0wz23vz",
            "dxqrb0rq1qc5wc8",
            "rw33hlqx255x2dl",
            "9slk5k5m2mjrjnl",
            "0h980p3b37sk2k5",
            "4mffr0tg4x2z093",
            "n5zxt9l04gfw1t2",
            "q71152b20h93h68",
            "j1ttz3zv1rq6xfx",
            "1jbbm3bc1cnj196",
            "9tmm43wm4vz85nj",
            "m4xxcktz3dvjkjs",
            "q711gnz20m5twwr",
            "m4xxgpmz1n3zdkr",
            "l3wwh96x569n4ml",
            "s933rzt446szj5m",
            "4mff4wvg4t47jhw",
            "r822rhj34fp57rw",
            "q711st220w16d4s",
            "ns00l1tt4rfbg1b",
            "tc55pgf53lhqd10",
            "7rkk7fgk0l497b1",
            "q822tql24433vl7",
            "bvnp3m6n4bgtlzh",
            "9tmn44pm37zpkbp",
            "0h9919nb5q3gd1n",
            "g0ssh5gs3w7m5v8",
            "q8243bm210q6jfz",
            "x28djpg3408x7g1",
            "fzr4vz5r272h4p4",
            "btmm7xqn0wnd6sr",
            "fzrtwcjr3tcvntx",
            "tb44b5050lsf06h",
            "tb44wgb52gmbsx6",
            "s933jq145kjkwhv",
            "9tmnn7fm27nqjkg",
            "8sll90hl5qrwp76",
            "h1tvp8mt289r9h6",
            "chpp35vj2w76228",
            "48ggvz190lc8h8p",
            "rw33hfdx28g8qdb",
            "7ckkz1sd12q99jr",
            "37fft4385k20102",
            "04bglpm55sclsf7",
            "26ddsbg751tqvfl",
            "9tmwjwhm0kkrpbl",
            "sb4d1c24526db30",
            "vc543z861njgrpc",
            "cwpp9prp4vggvtt",
            "n60c9pn019fmp0x",
            "4ngg39rg0bh3vx7",
            "n603w2h04mxvpgj",
    ]
}