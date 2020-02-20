import com.google.common.collect.ArrayListMultimap
import com.google.common.collect.Multimap
import spock.lang.Specification
import whelk.Document
import whelk.Embellisher
import whelk.JsonLd
import whelk.Link
import whelk.util.JsonLdSpec

class EmbellishSpec extends Specification{
    static final Map DISPLAY_DATA = [
            'lensGroups':
                    ['chips':
                             ['lenses': [
                                     'R': ['@type'         : 'fresnel:Lens',
                                           '@id'           : 'R-chips',
                                           'showProperties': ['mainEntity', 'pr1']
                                     ],
                                     'X': ['@type'         : 'fresnel:Lens',
                                           '@id'           : 'X-chips',
                                           'showProperties': ['px1', ['inverseOf': 'py1']]
                                     ],
                                     'Y': ['@type'         : 'fresnel:Lens',
                                           '@id'           : 'Y-chips',
                                           'showProperties': ['py1']
                                     ]
                             ]],
                     'cards':
                             ['lenses': [
                                     'R': ['@type'          : 'fresnel:Lens',
                                           '@id'            : 'R-cards',
                                           'fresnel:extends': ['@id': 'R-chips'],
                                           'showProperties' : ['fresnel:super', 'pr2']
                                     ],
                                     'X': ['@type'          : 'fresnel:Lens',
                                           '@id'            : 'X-cards',
                                           'fresnel:extends': ['@id': 'X-chips'],
                                           'showProperties' : ['fresnel:super', 'px2']
                                     ],
                                     'Y': ['@type'          : 'fresnel:Lens',
                                           '@id'            : 'Y-cards',
                                           'fresnel:extends': ['@id': 'Y-chips'],
                                           'showProperties' : ['fresnel:super', 'py2']
                                     ],
                             ]]]]
    
    /*


                       ┌−−−−−−−−−−−−−−−┐
                       ╎   embellish   ╎
                       ╎               ╎
                       ╎ ┌───────────┐ ╎
                       ╎ │ Y1 (chip) │ ╎
                       ╎ └───────────┘ ╎
                       ╎   │           ╎
                       ╎   │ py1       ╎
                       ╎   │           ╎
┌−−−−−−−−−−−−−−−−−−−−−−    │            −−−−−−−−−−−−−−−−−−−−−−−−┐
╎                          ▼                                    ╎
╎ ┌───────────┐  px2     ┌───────────┐   px1    ┌─────────────┐ ╎
╎ │ X4 (chip) │ ◀─────── │ X1 (card) │ ◀─────── │ doc (START) │ ╎
╎ └───────────┘          └───────────┘          └─────────────┘ ╎
╎                          │                                    ╎
╎                          │            −−−−−−−−−−−−−−−−−−−−−−−−┘
╎                          │           ╎
╎                          │ px1       ╎
╎                          ▼           ╎
╎ ┌───────────┐  py1     ┌───────────┐ ╎  px2   ┌─────────────┐
╎ │ Y2 (chip) │ ───────▶ │ X2 (chip) │ ╎ ─────▶ │     X5      │
╎ └───────────┘          └───────────┘ ╎        └─────────────┘
╎                          │           ╎
└−−−−−−−−−−−−−−−−−−−−−−    │           ╎
                       ╎   │           ╎
                       ╎   │ px1       ╎
                       ╎   ▼           ╎
  ┌───────────┐  py1   ╎ ┌───────────┐ ╎
  │    Y3     │ ─────▶ ╎ │ X3 (chip) │ ╎
  └───────────┘        ╎ └───────────┘ ╎
                       ╎               ╎
                       └−−−−−−−−−−−−−−−┘
                           │
                           │ px1
                           ▼
                         ┌───────────┐
                         │    X6     │
                         └───────────┘

Generated with: https://dot-to-ascii.ggerganov.com/

.dot:
digraph {
    subgraph cluster_0 {
        doc -> X1 [ label = "px1" ];
        X1 -> X2 [ label = "px1" ];
        X1 -> X4 [ label = "px2" ];
        Y1 -> X1 [ label = "py1" ];
        X2 -> X3 [ label = "px1" ];
        Y2 -> X2 [ label = "py1" ];

        label = "embellish";
    }

    X2 -> X5 [ label = "px2" ];
    X3 -> X6 [ label = "px1" ];
    Y3 -> X3 [ label = "py1" ];
    doc [label = "doc (START)"];
    X1 [label = "X1 (card)"];
    X2 [label = "X2 (chip)"];
    X3 [label = "X3 (chip)"];
    X4 [label = "X4 (chip)"];
    Y1 [label = "Y1 (chip)"];
    Y2 [label = "Y2 (chip)"];
}

     */

    def "should embellish recursively"() {
        given:
        def ld = new JsonLd(JsonLdSpec.CONTEXT_DATA, DISPLAY_DATA, JsonLdSpec.VOCAB_DATA)

        def doc = ['@graph': [
                [
                        '@type'     : 'R',
                        '@id'       : '/record',
                        'mainEntity': ['@id': '/thing']
                ],
                [
                        '@type': 'X',
                        '@id'  : '/thing',
                        'px1'   : ['@id': '/thingX1']
                ]
        ]]

        def docs = [
                ['@graph': [
                        [
                                '@type'     : 'R',
                                '@id'       : '/recordX1',
                                'mainEntity': ['@id': '/thingX1']
                        ],
                        [
                                '@type': 'X',
                                '@id'  : '/thingX1',
                                'px1'  : ['@id': '/thingX2'],
                                'px2'  : ['@id': '/thingX4']
                        ]
                ]],

                ['@graph': [
                        [
                                '@type'     : 'R',
                                '@id'       : '/recordX2',
                                'mainEntity': ['@id': '/thingX2']
                        ],
                        [
                                '@type': 'X',
                                '@id'  : '/thingX2',
                                'px1'  : ['@id': '/thingX3'],
                                'px2'  : ['@id': '/thingX5']
                        ]
                ]],

                ['@graph': [
                        [
                                '@type'     : 'R',
                                '@id'       : '/recordX3',
                                'mainEntity': ['@id': '/thingX3']
                        ],
                        [
                                '@type': 'X',
                                '@id'  : '/thingX3',
                                'px1'  : ['@id': '/thingX6'],
                                'px2'  : 'foo'
                        ]
                ]],

                ['@graph': [
                        [
                                '@type'     : 'R',
                                '@id'       : '/recordX4',
                                'mainEntity': ['@id': '/thingX4']
                        ],
                        [
                                '@type': 'X',
                                '@id'  : '/thingX4',
                                'px1'  : 'foo',
                                'px2'  : 'foo'
                        ]
                ]],

                ['@graph': [
                        [
                                '@type'     : 'R',
                                '@id'       : '/recordX5',
                                'mainEntity': ['@id': '/thingX5']
                        ],
                        [
                                '@type': 'X',
                                '@id'  : '/thingX5',
                                'px1'  : 'foo',
                                'px2'  : 'foo'
                        ]
                ]],

                ['@graph': [
                        [
                                '@type'     : 'R',
                                '@id'       : '/recordX6',
                                'mainEntity': ['@id': '/thingX6']
                        ],
                        [
                                '@type': 'X',
                                '@id'  : '/thingX6',
                                'px1'  : 'foo',
                                'px2'  : 'foo'
                        ]
                ]],

                ['@graph': [
                        [
                                '@type'     : 'R',
                                '@id'       : '/recordY1',
                                'mainEntity': ['@id': '/thingY1']
                        ],
                        [
                                '@type': 'Y',
                                '@id'  : '/thingY1',
                                'py1'  : ['@id': '/thingX1'],
                                'py2'  : 'foo'
                        ]
                ]],

                ['@graph': [
                        [
                                '@type'     : 'R',
                                '@id'       : '/recordY2',
                                'mainEntity': ['@id': '/thingY2']
                        ],
                        [
                                '@type': 'Y',
                                '@id'  : '/thingY2',
                                'py1'  : ['@id': '/thingX2'],
                                'py2'  : 'foo'
                        ]
                ]],

                ['@graph': [
                        [
                                '@type'     : 'R',
                                '@id'       : '/recordY3',
                                'mainEntity': ['@id': '/thingY3']
                        ],
                        [
                                '@type': 'Y',
                                '@id'  : '/thingY3',
                                'py1'  : ['@id': '/thingX3'],
                                'py2'  : 'foo'
                        ]
                ]],
        ]

        def storage = new Storage(ld)
        docs.each(storage.&add)

        def embellisher = new Embellisher(ld, storage.&getCards, storage.&getReverseLinks)

        Document document = new Document(doc)

        embellisher.embellish(document, false)
        def result = document.data

        expect:
        def x1 = find(result, '/thingX1')
        lens(x1) == 'card'
        x1['@reverse'] == ['py1': [['@id': '/thingY1']]]

        def x2 = find(result, '/thingX2')
        lens(x2) == 'chip'
        x2['@reverse'] == ['py1': [['@id': '/thingY2']]]

        def x3 = find(result, '/thingX3')
        lens(x3) == 'chip'
        x3['@reverse'] == ['py1': [['@id': '/thingY3']]]
        !find(result, '/thingY3')

        lens(find(result, '/thingX4')) == 'chip'
        lens(find(result, '/thingY1')) == 'chip'
        lens(find(result, '/thingY2')) == 'chip'

        !find(result, '/thingX5')
        !find(result, '/thingX6')
    }

    private Map find(Map graph, String id) {
        for (Map m in graph['@graph']) {
            if (m['@id'] && m['@id'] == id) {
                return m
            }
            if (m['@graph']) {
                Map n = find(m, id)
                if (n) {
                    return n
                }
            }
        }
        return null
    }

    private String lens(Map thing) {
        if (thing['@type'] == 'X') {
            if (thing['px1'] && thing['px2']) {
                return 'card'
            }
            if (thing['px1']) {
                return 'chip'
            }
        }
        if (thing['@type'] == 'Y') {
            if (thing['py1'] && thing['py2']) {
                return 'card'
            }
            if (thing['py1']) {
                return 'chip'
            }
        }
        throw new RuntimeException("Could not determine lens")
    }

    class Storage {
        Map<String, Map> cards = new HashMap<>()
        Multimap<Link, String> reverseLinks = new ArrayListMultimap<>()

        JsonLd jsonld
        Storage(JsonLd jsonld) {
            this.jsonld = jsonld
        }

        void add(Map document) {
            String iri = new Document(document).getThingIdentifiers().first()

            jsonld.getExternalReferences(document).each { l ->
                reverseLinks.put(l, iri)
            }

            cards.put(iri, jsonld.toCard(document, false))
        }

        Iterable<Map> getCards(Iterable<String> iris) {
            return cards.findAll { key, value ->
                iris.contains(key)
            }.values()
        }

        Set<String> getReverseLinks(String iri, List<String> relations) {
            return new HashSet(relations.collect() { r ->
                reverseLinks.get(new Link(iri: iri, relation: r))
            }.flatten())
        }
    }
}
