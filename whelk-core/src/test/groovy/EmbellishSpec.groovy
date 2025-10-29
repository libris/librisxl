import com.google.common.collect.ArrayListMultimap
import com.google.common.collect.Multimap
import spock.lang.Specification
import whelk.Document
import whelk.Embellisher
import whelk.JsonLd
import whelk.Link
import whelk.util.JsonLdSpec

class EmbellishSpec extends Specification {
    static final Map CONTEXT_DATA = [
            "@context": [
                    "@vocab": "https://example.org/ns/",
                    "pfx": "https://example.org/pfx/"
            ]
    ]
    
    static final Map VOCAB_DATA = [
            "@graph": [
                    ["@id": "https://example.org/ns/R"],
                    ["@id": "https://example.org/ns/X"],
                    ["@id": "https://example.org/ns/Y"],
                    ["@id": "https://example.org/ns/pr1"],
                    ["@id": "https://example.org/ns/px1"],
                    ["@id": "https://example.org/ns/px2"],
                    ["@id": "https://example.org/ns/px3"],
                    ["@id": "https://example.org/ns/py1"],
                    ["@id": "https://example.org/ns/IR",
                     "category": ["@id": "integral"]],
                    ["@id": "https://example.org/ns/IR2",
                     "category": ["@id": "integral"]],
                    ["@id": "https://example.org/ns/IR3",
                     "category": ["@id": "integral"],
                     "inverseOf": ["@id": "https://example.org/ns/RIR3"]],
                    ["@id": "https://example.org/ns/RIR3",
                     "category": ["@id": "integral"],
                     "inverseOf": ["@id": "https://example.org/ns/IR3"]],
            ]
    ]

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
                                           'showProperties': ['px1', ['inverseOf': 'py1'], 'IR', 'IR2', ['inverseOf': 'IR3'], ['inverseOf': 'px3']]
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
                             ]],
                     'full':
                             ['lenses': [
                                     'R': ['@type'          : 'fresnel:Lens',
                                           'fresnel:extends': ['@id': 'R-cards'],
                                           'showProperties' : ['fresnel:super']
                                     ],
                                     'X': ['@type'          : 'fresnel:Lens',
                                           'fresnel:extends': ['@id': 'X-cards'],
                                           'showProperties' : ['fresnel:super']
                                     ],
                                     'Y': ['@type'          : 'fresnel:Lens',
                                           'fresnel:extends': ['@id': 'Y-cards'],
                                           'showProperties' : ['fresnel:super']
                                     ],
                             ]]]]




    /*

                       ┌−−−−−−−−−−−−−−−−−┐
                       ╎    embellish    ╎
                       ╎                 ╎
                       ╎ ┌─────────────┐ ╎
                       ╎ │  Y0 (card)  │ ╎
                       ╎ └─────────────┘ ╎
                       ╎   │             ╎
                       ╎   │ py1         ╎
                       ╎   ▼             ╎
                       ╎ ┌─────────────┐ ╎
                       ╎ │ doc (START) │ ╎
                       ╎ └─────────────┘ ╎
                       ╎   │             ╎
                       ╎   │ px1         ╎
                       ╎   │             ╎
┌−−−−−−−−−−−−−−−−−−−−−−    │              −−−−−−−−−−−−−−−−−−−−−−┐
╎                          ▼                                    ╎
╎ ┌───────────┐  py1     ┌─────────────┐   px2    ┌───────────┐ ╎
╎ │ Y1 (chip) │ ───────▶ │  X1 (card)  │ ───────▶ │ X4 (chip) │ ╎
╎ └───────────┘          └─────────────┘          └───────────┘ ╎
╎                          │                                    ╎
└−−−−−−−−−−−−−−−−−−−−−−    │              −−−−−−−−−−−−−−−−−−−−−−┘
                       ╎   │             ╎
                       ╎   │ px1         ╎
                       ╎   ▼             ╎
  ┌───────────┐  py1   ╎ ┌─────────────┐ ╎
  │    Y2     │ ─────▶ ╎ │  X2 (chip)  │ ╎
  └───────────┘        ╎ └─────────────┘ ╎
                       ╎                 ╎
                       └−−−−−−−−−−−−−−−−−┘
                           │
                           │ px1
                           ▼
                         ┌─────────────┐
                         │     X3      │
                         └─────────────┘
Generated with: https://dot-to-ascii.ggerganov.com/

.dot:
digraph {
    subgraph cluster_0 {
        doc -> X1 [ label = "px1" ];
        Y0 -> doc [ label = "py1" ];
        X1 -> X2 [ label = "px1" ];
        X1 -> X4 [ label = "px2" ];
        Y1 -> X1 [ label = "py1" ];

        label = "embellish";
    }

    X2 -> X3 [ label = "px1" ];
    Y2 -> X2 [ label = "py1" ];
    doc [label = "doc (START)"];
    X1 [label = "X1 (card)"];
    X2 [label = "X2 (chip)"];
    X4 [label = "X4 (chip)"];
    Y0 [label = "Y0 (card)"];
    Y1 [label = "Y1 (chip)"];
}

*/

    def "should by default embellish recursively, two levels, using cards, chips"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, DISPLAY_DATA, VOCAB_DATA)

        def doc = ['@graph': [['@type': 'R', '@id': '/record', 'mainEntity': ['@id': '/thing']],
                              ['@type': 'X', '@id': '/thing', 'px1': ['@id': '/thingX1']]
        ]]

        def docs = [
                ['@graph': [['@type': 'R', '@id': '/recordX1', 'mainEntity': ['@id': '/thingX1']],
                            ['@type': 'X', '@id': '/thingX1', 'px1': ['@id': '/thingX2'], 'px2': ['@id': '/thingX4']]]],

                ['@graph': [['@type': 'R', '@id': '/recordX2', 'mainEntity': ['@id': '/thingX2']],
                            ['@type': 'X', '@id': '/thingX2', 'px1': ['@id': '/thingX3'], 'px2': ['@id': '/thingX5']]]],

                ['@graph': [['@type': 'R', '@id': '/recordX3', 'mainEntity': ['@id': '/thingX3']],
                            ['@type': 'X', '@id': '/thingX3', 'px1': ['@id': '/thingX6'], 'px2': 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordX4', 'mainEntity': ['@id': '/thingX4']],
                            ['@type': 'X', '@id': '/thingX4', 'px1': 'foo', 'px2': 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordY0', 'mainEntity': ['@id': '/thingY0']],
                            ['@type': 'Y', '@id': '/thingY0', 'py1': ['@id': '/thing'], 'py2': 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordY1', 'mainEntity': ['@id': '/thingY1']],
                            ['@type': 'Y', '@id': '/thingY1', 'py1': ['@id': '/thingX1'], 'py2': 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordY2', 'mainEntity': ['@id': '/thingY2']],
                            ['@type': 'Y', '@id': '/thingY2', 'py1': ['@id': '/thingX2'], 'py2': 'foo']]],
        ]

        def storage = new TestStorage(ld)
        storage.add(doc)
        docs.each(storage.&add)

        def embellisher = new Embellisher(ld, storage.&getFull, storage.&getCards, storage.&getReverseLinks)

        Document document = new Document(doc)

        embellisher.embellish(document)
        def result = document.data

        expect:
        def x1 = find(result, '/thingX1')
        lens(x1) == 'card'
        x1['@reverse'] == ['py1': [['@id': '/thingY1']]]

        def x2 = find(result, '/thingX2')
        lens(x2) == 'chip'
        x2['@reverse'] == ['py1': [['@id': '/thingY2']]]

        find(result, '/thing')['@reverse'] == ['py1': [['@id': '/thingY0']]]
        lens(find(result, '/thingY0')) == 'card'

        lens(find(result, '/thingX4')) == 'chip'
        lens(find(result, '/thingY1')) == 'chip'

        !find(result, '/thingX3')
        !find(result, '/thingY2')

        result['@graph'].size() == 2 + 5
    }




    /*

                           ┌−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−┐
                           ╎                        embellish                          ╎
                           ╎                                                           ╎
                           ╎ ┌───────────┐  IR    ┌───────────┐   px1    ┌───────────┐ ╎  px1   ┌───────────┐
                           ╎ │ X6 (card) │ ─────▶ │ X7 (card) │ ───────▶ │ X8 (chip) │ ╎ ─────▶ │    Y3     │
                           ╎ └───────────┘        └───────────┘          └───────────┘ ╎        └───────────┘
                           ╎   ▲                                                       ╎
                           ╎   │ px1                                                   ╎
                           ╎   │                                                       ╎
┌−−−−−−−−−−−−−−−−−−−−−−−−−−    │                                                        −−−−−−−−−−−−−−−−−−−−−−┐
╎                              │                                                                              ╎
╎ ┌─────────────┐   IR       ┌───────────┐  px1   ┌───────────┐   IR     ┌───────────┐   px1    ┌───────────┐ ╎  px1   ┌────┐
╎ │ doc (START) │ ─────────▶ │ X0 (full) │ ─────▶ │ X3 (card) │ ───────▶ │ X4 (card) │ ───────▶ │ X5 (chip) │ ╎ ─────▶ │ Y2 │
╎ └─────────────┘            └───────────┘        └───────────┘          └───────────┘          └───────────┘ ╎        └────┘
╎   │                                               ▲                                                         ╎
╎   │                                               │            −−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−┘
╎   │                                               │           ╎
╎   │ px1                                           │           ╎
╎   ▼                                               │           ╎
╎ ┌─────────────┐   px1                             │           ╎
╎ │  X1 (card)  │ ──────────────────────────────────┘           ╎
╎ └─────────────┘                                               ╎
╎   │                                                           ╎
╎   │              −−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−┘
╎   │             ╎
╎   │ px1         ╎
╎   ▼             ╎
╎ ┌─────────────┐ ╎  px1     ┌───────────┐
╎ │  X2 (chip)  │ ╎ ─────▶   │    Y1     │
╎ └─────────────┘ ╎          └───────────┘
╎                 ╎
└−−−−−−−−−−−−−−−−−┘
Generated with: https://dot-to-ascii.ggerganov.com/

.dot:
digraph {
    rankdir=LR;
    subgraph cluster_0 {
        doc -> X0 [ label = "IR" ];
        doc -> X1 [ label = "px1" ];
        X0 -> X3 [ label = "px1" ];
        X0 -> X6 [ label = "px1" ];
        X1 -> X2 [ label = "px1" ];
        X1 -> X3 [ label = "px1" ];
        X3 -> X4 [ label = "IR" ];
        X4 -> X5 [ label = "px1" ];
        X6 -> X7 [ label = "IR" ];
        X7 -> X8 [ label = "px1" ];
        label = "embellish";
    }
    X2 -> Y1 [ label = "px1" ];
    X5 -> Y2 [ label = "px1" ];
    X8 -> Y3 [ label = "px1" ];

    doc [label = "doc (START)"];
    X0 [label = "X0 (full)"];
    X1 [label = "X1 (card)"];
    X2 [label = "X2 (chip)"];
    X3 [label = "X3 (card)"];
    X4 [label = "X4 (card)"];
    X5 [label = "X5 (chip)"];
    X6 [label = "X6 (card)"];
    X7 [label = "X7 (card)"];
    X8 [label = "X8 (chip)"];
}

*/

    def "should handle integral relations"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, DISPLAY_DATA, VOCAB_DATA)

        def doc = ['@graph': [['@type': 'R', '@id': '/record', 'mainEntity': ['@id': '/thing']],
                              ['@type': 'X', '@id': '/thing', 'IR': ['@id': '/thingX0'], 'px1': ['@id': '/thingX1']]
        ]]

        def docs = [
                ['@graph': [['@type': 'R', '@id': '/recordX0', 'mainEntity': ['@id': '/thingX0']],
                            ['@type': 'X', '@id': '/thingX0',
                             'px1'  : [['@id': '/thingX3'], ['@id': '/thingX6']],
                             'px2'  : 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordX1', 'mainEntity': ['@id': '/thingX1']],
                            ['@type': 'X', '@id': '/thingX1',
                             'px1'  : [['@id': '/thingX2'], ['@id': '/thingX3']],
                             'px2'  : 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordX2', 'mainEntity': ['@id': '/thingX2']],
                            ['@type': 'X', '@id': '/thingX2', 'px1': ['@id': '/thingY1'], 'px2': 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordX3', 'mainEntity': ['@id': '/thingX3']],
                            ['@type': 'X', '@id': '/thingX3',
                             'IR'   : ['@id': '/thingX4'],
                             'px1'  : 'foo',
                             'px2'  : 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordX4', 'mainEntity': ['@id': '/thingX4']],
                            ['@type': 'X', '@id': '/thingX4', 'px1': ['@id': '/thingX5'], 'px2': 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordX5', 'mainEntity': ['@id': '/thingX5']],
                            ['@type': 'X', '@id': '/thingX5', 'px1': ['@id': '/thingY2'], 'px2': 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordX6', 'mainEntity': ['@id': '/thingX6']],
                            ['@type': 'X', '@id': '/thingX6',
                             'IR'   : ['@id': '/thingX7'],
                             'px1'  : 'foo',
                             'px2'  : 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordX7', 'mainEntity': ['@id': '/thingX7']],
                            ['@type': 'X', '@id': '/thingX7', 'px1': ['@id': '/thingX8'], 'px2': 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordX8', 'mainEntity': ['@id': '/thingX8']],
                            ['@type': 'X', '@id': '/thingX8', 'px1': ['@id': '/thingY3'], 'px2': 'foo']]],
        ]

        def storage = new TestStorage(ld)
        storage.add(doc)
        docs.each(storage.&add)

        def embellisher = new Embellisher(ld, storage.&getFull, storage.&getCards, storage.&getReverseLinks)

        Document document = new Document(doc)

        embellisher.embellish(document)
        def result = document.data

        expect:
        lens(find(result, '/thingX0')) == 'full'

        lens(find(result, '/thingX1')) == 'card'
        lens(find(result, '/thingX2')) == 'chip'

        lens(find(result, '/thingX3')) == 'card'
        lens(find(result, '/thingX4')) == 'card'
        lens(find(result, '/thingX5')) == 'chip'

        lens(find(result, '/thingX6')) == 'card'
        lens(find(result, '/thingX7')) == 'card'
        lens(find(result, '/thingX8')) == 'chip'

        !find(result, '/thingY1')
        !find(result, '/thingY2')
        !find(result, '/thingY3')

        result['@graph'].size() == 2 + 9
    }




    /*

                         ┌−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−┐
                         ╎                        embellish                          ╎
                         ╎                                                           ╎
                         ╎ ┌───────────┐  IR    ┌───────────┐   px1    ┌───────────┐ ╎
                         ╎ │ X7 (full) │ ─────▶ │ X8 (full) │ ───────▶ │ X9 (card) │ ╎
                         ╎ └───────────┘        └───────────┘          └───────────┘ ╎
                         ╎   ▲                                                       ╎
                         ╎   │ IR2                                                   ╎
                         ╎   │                                                       ╎
┌−−−−−−−−−−−−−−−−−−−−−−−−    │                                                        −−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−┐
╎                            │                                                                                                  ╎
╎ ┌─────────────┐  IR      ┌───────────┐  px1   ┌───────────┐   IR     ┌───────────┐   IR    ┌───────────┐  px1   ┌───────────┐ ╎  px1   ┌────┐
╎ │ doc (START) │ ───────▶ │ X0 (full) │ ─────▶ │ X3 (card) │ ───────▶ │ X4 (card) │ ──────▶ │ X5 (card) │ ─────▶ │ X6 (chip) │ ╎ ─────▶ │ Y1 │
╎ └─────────────┘          └───────────┘        └───────────┘          └───────────┘         └───────────┘        └───────────┘ ╎        └────┘
╎   │                                             ▲                                                                             ╎
╎   │                                             │            −−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−┘
╎   │                                             │           ╎
╎   │ px1                                         │           ╎
╎   ▼                                             │           ╎
╎ ┌─────────────┐  px1                            │           ╎
╎ │  X1 (card)  │ ────────────────────────────────┘           ╎
╎ └─────────────┘                                             ╎
╎                                                             ╎
└−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−┘


Generated with: https://dot-to-ascii.ggerganov.com/

.dot:
digraph {
    rankdir=LR;
    subgraph cluster_0 {
        doc -> X0 [ label = "IR" ];
        doc -> X1 [ label = "px1" ];
        X0 -> X3 [ label = "px1" ];
        X0 -> X7 [ label = "IR2" ];
        X1 -> X3 [ label = "px1" ];
        X3 -> X4 [ label = "IR" ];
        X4 -> X5 [ label = "IR" ];
        X5 -> X6 [ label = "px1" ];
        X7 -> X8 [ label = "IR" ];
        X8 -> X9 [ label = "px1" ];
        label = "embellish";
    }

    X6 -> Y1 [ label = "px1" ];

    doc [label = "doc (START)"];
    X0 [label = "X0 (full)"];
    X1 [label = "X1 (card)"];
    X3 [label = "X3 (card)"];
    X4 [label = "X4 (card)"];
    X5 [label = "X5 (card)"];
    X6 [label = "X6 (chip)"];
    X7 [label = "X7 (full)"];
    X8 [label = "X8 (full)"];
    X9 [label = "X9 (card)"];
}
*/

    def "should handle multi-level integral relations"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, DISPLAY_DATA, VOCAB_DATA)

        def doc = ['@graph': [['@type': 'R', '@id': '/record', 'mainEntity': ['@id': '/thing']],
                              ['@type': 'X', '@id': '/thing', 'IR': ['@id': '/thingX0'], 'px1': ['@id': '/thingX1']]
        ]]

        def docs = [
                ['@graph': [['@type': 'R', '@id': '/recordX0', 'mainEntity': ['@id': '/thingX0']],
                            ['@type': 'X', '@id': '/thingX0',
                             'IR2'  : [['@id': '/thingX7']],
                             'px1'  : [['@id': '/thingX3']],
                             'px2'  : 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordX1', 'mainEntity': ['@id': '/thingX1']],
                            ['@type': 'X', '@id': '/thingX1',
                             'px1'  : [['@id': '/thingX3']],
                             'px2'  : 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordX3', 'mainEntity': ['@id': '/thingX3']],
                            ['@type': 'X', '@id': '/thingX3',
                             'IR'   : ['@id': '/thingX4'],
                             'px1'  : 'foo',
                             'px2'  : 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordX4', 'mainEntity': ['@id': '/thingX4']],
                            ['@type': 'X', '@id': '/thingX4',
                             'IR'   : ['@id': '/thingX5'],
                             'px1'  : 'foo',
                             'px2'  : 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordX5', 'mainEntity': ['@id': '/thingX5']],
                            ['@type': 'X', '@id': '/thingX5',
                             'px1'  : ['@id': '/thingX6'],
                             'px2'  : 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordX6', 'mainEntity': ['@id': '/thingX6']],
                            ['@type': 'X', '@id': '/thingX6',
                             'px1'  : ['@id': '/thingY1'],
                             'px2'  : 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordX7', 'mainEntity': ['@id': '/thingX7']],
                            ['@type': 'X', '@id': '/thingX7', 'IR': ['@id': '/thingX8'], 'px1': 'foo', 'px2': 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordX8', 'mainEntity': ['@id': '/thingX8']],
                            ['@type': 'X', '@id': '/thingX8', 'px1': ['@id': '/thingX9'], 'px2': 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordX9', 'mainEntity': ['@id': '/thingX9']],
                            ['@type': 'X', '@id': '/thingX9', 'px1': 'foo', 'px1': 'foo', 'px2': 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordY1', 'mainEntity': ['@id': '/thingY1']],
                            ['@type': 'Y', '@id': '/thingY1', 'py1': 'foo', 'px1': 'foo', 'py2': 'foo']]],
        ]

        def storage = new TestStorage(ld)
        storage.add(doc)
        docs.each(storage.&add)

        def embellisher = new Embellisher(ld, storage.&getFull, storage.&getCards, storage.&getReverseLinks)

        Document document = new Document(doc)

        embellisher.embellish(document)
        def result = document.data

        expect:
        lens(find(result, '/thingX0')) == 'full'

        lens(find(result, '/thingX1')) == 'card'

        lens(find(result, '/thingX3')) == 'card'
        lens(find(result, '/thingX4')) == 'card'
        lens(find(result, '/thingX5')) == 'card'
        lens(find(result, '/thingX6')) == 'chip'

        lens(find(result, '/thingX7')) == 'full'
        lens(find(result, '/thingX8')) == 'full'
        lens(find(result, '/thingX9')) == 'card'

        !find(result, '/thingY1')

        result['@graph'].size() == 2 + 9
    }




    /*

┌−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−┐
╎                 embellish               ╎
╎                                         ╎
╎                 px1                     ╎
╎   ┌─────────────────────────┐           ╎
╎   │                         ▼           ╎
╎ ┌─────────────┐  IR       ┌───────────┐ ╎
╎ │ doc (START) │ ────────▶ │ X0 (full) │ ╎
╎ └─────────────┘           └───────────┘ ╎
╎                                         ╎
└−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−┘

Generated with: https://dot-to-ascii.ggerganov.com/

.dot:
digraph {
    rankdir=LR;
    subgraph cluster_0 {
        doc -> X0 [ label = "IR" ];
        doc -> X0 [ label = "px1" ];


        label = "embellish";
    }

    doc [label = "doc (START)"];
    X0 [label = "X0 (full)"];
}


    */

    def "should follow integral relations before other relations"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, DISPLAY_DATA, VOCAB_DATA)

        def doc = ['@graph': [['@type': 'R', '@id': '/record', 'mainEntity': ['@id': '/thing']],
                              ['@type': 'X', '@id': '/thing', 'IR': ['@id': '/thingX0'], 'px1': ['@id': '/thingX0']]
        ]]

        def docs = [
                ['@graph': [['@type': 'R', '@id': '/recordX0', 'mainEntity': ['@id': '/thingX0']],
                            ['@type': 'X', '@id': '/thingX0',
                             'px1': 'foo',
                             'px2': 'foo']]]
        ]

        def storage = new TestStorage(ld)
        storage.add(doc)
        docs.each(storage.&add)

        def embellisher = new Embellisher(ld, storage.&getFull, storage.&getCards, storage.&getReverseLinks)

        Document document = new Document(doc)

        embellisher.embellish(document)
        def result = document.data

        expect:
        lens(find(result, '/thingX0')) == 'full'

        result['@graph'].size() == 2 + 1
    }




    /*

┌−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−┐
╎                         embellish                       ╎
╎                                                         ╎
╎                 px1                                     ╎
╎   ┌─────────────────────────────────────────┐           ╎
╎   │                                         ▼           ╎
╎ ┌─────────────┐  IR   ┌───────────┐  IR   ┌───────────┐ ╎
╎ │ doc (START) │ ────▶ │ X0 (full) │ ────▶ │ X1 (full) │ ╎
╎ └─────────────┘       └───────────┘       └───────────┘ ╎
╎                                                         ╎
└−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−┘

Generated with: https://dot-to-ascii.ggerganov.com/

.dot:
digraph {
    rankdir=LR;
    subgraph cluster_0 {
        doc -> X0 [ label = "IR" ];
        doc -> X1 [ label = "px1" ];
        X0 -> X1 [ label = "IR" ];

        label = "embellish";
    }

    doc [label = "doc (START)"];
    X0 [label = "X0 (full)"];
    X1 [label = "X1 (full)"];
}

    */

    def "should follow integral relations before other relations (also when shorter path exists)"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, DISPLAY_DATA, VOCAB_DATA)

        def doc = ['@graph': [
                ['@type': 'R', '@id': '/record', 'mainEntity': ['@id': '/thing']],
                ['@type': 'X', '@id': '/thing',
                 'IR' : ['@id': '/thingX0'],
                 'px1': ['@id': '/thingX1']]
        ]]

        def docs = [
                ['@graph': [['@type': 'R', '@id': '/recordX0', 'mainEntity': ['@id': '/thingX0']],
                            ['@type': 'X', '@id': '/thingX0',
                             'IR'   : [['@id': '/thingX1']],
                             'px1'  : 'foo',
                             'px2'  : 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordX1', 'mainEntity': ['@id': '/thingX1']],
                            ['@type': 'X', '@id': '/thingX1',
                             'px1'  : 'foo',
                             'px2'  : 'foo']]],
        ]

        def storage = new TestStorage(ld)
        storage.add(doc)
        docs.each(storage.&add)

        def embellisher = new Embellisher(ld, storage.&getFull, storage.&getCards, storage.&getReverseLinks)

        Document document = new Document(doc)

        embellisher.embellish(document)
        def result = document.data

        expect:
        lens(find(result, '/thingX0')) == 'full'
        lens(find(result, '/thingX1')) == 'full'

        result['@graph'].size() == 2 + 2
    }


    /*

┌−−−−−−−−−−−−−−−−−┐
╎    embellish    ╎
╎                 ╎
╎ ┌─────────────┐ ╎
╎ │  X1 (full)  │ ╎
╎ └─────────────┘ ╎
╎   │             ╎
╎   │ IR3         ╎
╎   ▼             ╎
╎ ┌─────────────┐ ╎
╎ │  X0 (full)  │ ╎
╎ └─────────────┘ ╎
╎   │             ╎
╎   │ IR3         ╎
╎   ▼             ╎
╎ ┌─────────────┐ ╎
╎ │ doc (START) │ ╎
╎ └─────────────┘ ╎
╎                 ╎
└−−−−−−−−−−−−−−−−−┘

Generated with: https://dot-to-ascii.ggerganov.com/

.dot:
digraph {
    subgraph cluster_0 {
        X1 -> X0 [ label = "IR3" ];
        X0 -> doc [ label = "IR3" ];

        label = "embellish";
    }

    doc [label = "doc (START)"];
    X0 [label = "X0 (full)"];
    X1 [label = "X1 (full)"];
}


    */

    def "should handle inverse relation defined as integral"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, DISPLAY_DATA, VOCAB_DATA)

        def doc = ['@graph': [
                ['@type': 'R', '@id': '/record', 'mainEntity': ['@id': '/thing']],
                ['@type': 'X', '@id': '/thing']
        ]]

        def docs = [
                ['@graph': [['@type': 'R', '@id': '/recordX0', 'mainEntity': ['@id': '/thingX0']],
                            ['@type': 'X', '@id': '/thingX0',
                             'IR3'  : [['@id': '/thing']],
                             'px1'  : 'foo',
                             'px2'  : 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordX1', 'mainEntity': ['@id': '/thingX1']],
                            ['@type': 'X', '@id': '/thingX1',
                             'IR3'  : [['@id': '/thingX0']],
                             'px1'  : 'foo',
                             'px2'  : 'foo']]],
        ]

        def storage = new TestStorage(ld)
        storage.add(doc)
        docs.each(storage.&add)

        def embellisher = new Embellisher(ld, storage.&getFull, storage.&getCards, storage.&getReverseLinks)

        Document document = new Document(doc)

        embellisher.embellish(document)
        def result = document.data

        expect:
        lens(find(result, '/thingX0')) == 'full'
        lens(find(result, '/thingX1')) == 'full'

        result['@graph'].size() == 2 + 2
    }




    /*

┌−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−┐
╎                 embellish          ╎
╎                                    ╎
╎ ┌─────────────┐                    ╎
╎ │ doc (START) │            ◀┐      ╎
╎ └─────────────┘             │      ╎
╎   │                         │      ╎
╎   │ p1                      │ IR3  ╎
╎   ▼                         │      ╎
╎ ┌─────────────┐             │      ╎
╎ │  X0 (full)  │            ─┘      ╎
╎ └─────────────┘                    ╎
╎                                    ╎
└−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−┘

Generated with: https://dot-to-ascii.ggerganov.com/

.dot:
digraph {
    subgraph cluster_0 {
        doc -> X0 [ label = "p1" ];
        X0 -> doc [ label = "IR3" ];


        label = "embellish";
    }

    doc [label = "doc (START)"];
    X0 [label = "X0 (full)"];
}


    */

    def "follows inverse integral relation before non-inverse non-integral relation"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, DISPLAY_DATA, VOCAB_DATA)

        def doc = ['@graph': [
                ['@type': 'R', '@id': '/record', 'mainEntity': ['@id': '/thing']],
                ['@type': 'X', '@id': '/thing',
                 'px1' : ['@id': '/thingX0']]
        ]]

        def docs = [
                ['@graph': [['@type': 'R', '@id': '/recordX0', 'mainEntity': ['@id': '/thingX0']],
                            ['@type': 'X', '@id': '/thingX0',
                             'IR3'  : [['@id': '/thing']],
                             'px1'  : 'foo',
                             'px2'  : 'foo']]],
        ]

        def storage = new TestStorage(ld)
        storage.add(doc)
        docs.each(storage.&add)

        def embellisher = new Embellisher(ld, storage.&getFull, storage.&getCards, storage.&getReverseLinks)

        Document document = new Document(doc)

        embellisher.embellish(document)
        def result = document.data

        expect:
        lens(find(result, '/thingX0')) == 'full'
        //lens(find(result, '/thingX1')) == 'full'

        result['@graph'].size() == 2 + 1
    }




    /*

┌−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−┐
╎            embellish            ╎
╎                                 ╎
╎         ┌─────────────┐         ╎
╎   ┌───▶ │  X0 (full)  │ ─┐      ╎
╎   │     └─────────────┘  │      ╎
╎   │       │              │      ╎
╎   │ px1   │ IR3          │ px3  ╎
╎   │       ▼              │      ╎
╎   │     ┌─────────────┐  │      ╎
╎   └──── │ doc (START) │ ◀┘      ╎
╎         └─────────────┘         ╎
╎                                 ╎
└−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−−┘

Generated with: https://dot-to-ascii.ggerganov.com/

.dot:
digraph {
    subgraph cluster_0 {
        X0 -> doc [ label = "IR3" ];
        X0 -> doc [ label = "px3" ];
        doc -> X0 [ label = "px1" ];

        label = "embellish";
    }

    doc [label = "doc (START)"];
    X0 [label = "X0 (full)"];
}


    */

    def "should follow inverse integral relations before other relations"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, DISPLAY_DATA, VOCAB_DATA)

        def doc = ['@graph': [
                              ['@type': 'R', '@id': '/record', 'mainEntity': ['@id': '/thing']],
                              ['@type': 'X', '@id': '/thing',
                               'px1' : ['@id': '/thingX0']]
        ]]

        def docs = [
                ['@graph': [['@type': 'R', '@id': '/recordX0', 'mainEntity': ['@id': '/thingX0']],
                            ['@type': 'X', '@id': '/thingX0',
                             'IR3': [['@id': '/thing']],
                             'px3': [['@id': '/thing']],
                             'px1': 'foo',
                             'px2': 'foo']]]
        ]

        def storage = new TestStorage(ld)
        storage.add(doc)
        docs.each(storage.&add)

        def embellisher = new Embellisher(ld, storage.&getFull, storage.&getCards, storage.&getReverseLinks)

        Document document = new Document(doc)

        embellisher.embellish(document)
        def result = document.data

        expect:
        lens(find(result, '/thingX0')) == 'full'

        result['@graph'].size() == 2 + 1
    }




    /*

                       ┌−−−−−−−−−−−−−−−−−┐
                       ╎    embellish    ╎
                       ╎                 ╎
                       ╎ ┌─────────────┐ ╎
                       ╎ │  Y0 (card)  │ ╎
                       ╎ └─────────────┘ ╎
                       ╎   │             ╎
                       ╎   │ py1         ╎
                       ╎   ▼             ╎
                       ╎ ┌─────────────┐ ╎
                       ╎ │ doc (START) │ ╎
                       ╎ └─────────────┘ ╎
                       ╎   │             ╎
                       ╎   │ px1         ╎
                       ╎   │             ╎
┌−−−−−−−−−−−−−−−−−−−−−−    │              −−−−−−−−−−−−−−−−−−−−−−┐
╎                          ▼                                    ╎
╎ ┌───────────┐  py1     ┌─────────────┐   px2    ┌───────────┐ ╎
╎ │ Y1 (chip) │ ───────▶ │  X1 (card)  │ ───────▶ │ X4 (chip) │ ╎
╎ └───────────┘          └─────────────┘          └───────────┘ ╎
╎                          │                                    ╎
╎                          │              −−−−−−−−−−−−−−−−−−−−−−┘
╎                          │             ╎
╎                          │ px1         ╎
╎                          ▼             ╎
╎ ┌───────────┐  py1     ┌─────────────┐ ╎  px2   ┌───────────┐
╎ │ Y2 (chip) │ ───────▶ │  X2 (chip)  │ ╎ ─────▶ │    X5     │
╎ └───────────┘          └─────────────┘ ╎        └───────────┘
╎                          │             ╎
└−−−−−−−−−−−−−−−−−−−−−−    │             ╎
                       ╎   │             ╎
                       ╎   │ px1         ╎
                       ╎   ▼             ╎
  ┌───────────┐  py1   ╎ ┌─────────────┐ ╎
  │    Y3     │ ─────▶ ╎ │  X3 (chip)  │ ╎
  └───────────┘        ╎ └─────────────┘ ╎
                       ╎                 ╎
                       └−−−−−−−−−−−−−−−−−┘
                           │
                           │ px1
                           ▼
                         ┌─────────────┐
                         │     X6      │
                         └─────────────┘

Generated with: https://dot-to-ascii.ggerganov.com/

.dot:
digraph {
    subgraph cluster_0 {
        doc -> X1 [ label = "px1" ];
        Y0 -> doc [ label = "py1" ];
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
    Y0 [label = "Y0 (card)"];
    Y1 [label = "Y1 (chip)"];
    Y2 [label = "Y2 (chip)"];
}



     */

    def "should embellish recursively, three levels, using cards, chips, chips"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, DISPLAY_DATA, VOCAB_DATA)

        def doc = ['@graph': [['@type': 'R', '@id': '/record', 'mainEntity': ['@id': '/thing']],
                              ['@type': 'X', '@id': '/thing', 'px1': ['@id': '/thingX1']]
        ]]

        def docs = [
                ['@graph': [['@type': 'R', '@id': '/recordX1', 'mainEntity': ['@id': '/thingX1']],
                            ['@type': 'X', '@id': '/thingX1', 'px1': ['@id': '/thingX2'], 'px2': ['@id': '/thingX4']]]],

                ['@graph': [['@type': 'R', '@id': '/recordX2', 'mainEntity': ['@id': '/thingX2']], 
                            ['@type': 'X', '@id': '/thingX2', 'px1': ['@id': '/thingX3'], 'px2': ['@id': '/thingX5']]]],

                ['@graph': [['@type': 'R', '@id': '/recordX3', 'mainEntity': ['@id': '/thingX3']],
                            ['@type': 'X', '@id': '/thingX3', 'px1': ['@id': '/thingX6'], 'px2': 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordX4', 'mainEntity': ['@id': '/thingX4']],
                            ['@type': 'X', '@id': '/thingX4', 'px1': 'foo', 'px2': 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordX5', 'mainEntity': ['@id': '/thingX5']],
                            ['@type': 'X', '@id': '/thingX5', 'px1': 'foo', 'px2': 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordX6', 'mainEntity': ['@id': '/thingX6']],
                            ['@type': 'X', '@id': '/thingX6', 'px1': 'foo', 'px2': 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordY0', 'mainEntity': ['@id': '/thingY0']],
                            ['@type': 'Y', '@id': '/thingY0', 'py1': ['@id': '/thing'], 'py2': 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordY1', 'mainEntity': ['@id': '/thingY1']],
                            ['@type': 'Y', '@id': '/thingY1', 'py1': ['@id': '/thingX1'], 'py2': 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordY2', 'mainEntity': ['@id': '/thingY2']],
                            ['@type': 'Y', '@id': '/thingY2', 'py1': ['@id': '/thingX2'], 'py2': 'foo']]],

                ['@graph': [['@type': 'R', '@id': '/recordY3', 'mainEntity': ['@id': '/thingY3']],
                            ['@type': 'Y', '@id': '/thingY3', 'py1': ['@id': '/thingX3'], 'py2': 'foo']]],
        ]

        def storage = new TestStorage(ld)
        storage.add(doc)
        docs.each(storage.&add)

        def embellisher = new Embellisher(ld, storage.&getFull, storage.&getCards, storage.&getReverseLinks)
        embellisher.setEmbellishLevels(['cards', 'chips', 'chips'])

        Document document = new Document(doc)

        embellisher.embellish(document)
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
        
        find(result, '/thing')['@reverse'] == ['py1': [['@id': '/thingY0']]]
        lens(find(result, '/thingY0')) == 'card'

        lens(find(result, '/thingX4')) == 'chip'
        lens(find(result, '/thingY1')) == 'chip'
        lens(find(result, '/thingY2')) == 'chip'

        !find(result, '/thingX5')
        !find(result, '/thingX6')

        result['@graph'].size() == 2 + 7
    }

    def "should understand sameAs when avoiding loops in embellish graph"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, DISPLAY_DATA, VOCAB_DATA)

        def doc = ['@graph': [['@type' : 'R', '@id': '/record', 'mainEntity': ['@id': '/thing']],
                              ['@type' : 'X',
                               '@id'   : '/thing',
                               'px1'   : ['@id': '/thingX1'],
                               'sameAs': [['@id': '/thingAlias1'], ['@id': '/thingAlias2']]
                              ]
        ]]

        def docs = [
                ['@graph': [['@type' : 'R', '@id': '/recordX1', 'mainEntity': ['@id': '/thingX1']],
                            ['@type' : 'X',
                             '@id'   : '/thingX1',
                             'sameAs': [['@id': '/thingX1Alias1']],
                             'px1'   : [['@id': '/thingAlias1'], ['@id': '/thingX2']],
                             'px2'   : ['@id': '/thingAlias2']]]],

                ['@graph': [['@type' : 'R', '@id': '/recordX2', 'mainEntity': ['@id': '/thingX2']],
                            ['@type' : 'X', '@id': '/thingX2',
                             'px1'   : ['@id': '/thingX1Alias1']]]],
        ]

        def storage = new TestStorage(ld)
        storage.add(doc)
        docs.each(storage.&add)

        def embellisher = new Embellisher(ld, storage.&getFull, storage.&getCards, storage.&getReverseLinks)

        Document document = new Document(doc)

        embellisher.embellish(document)
        def result = document.data

        expect:
        result['@graph'].size() == 2 + 2
        find(result, '/thingX1')
        find(result, '/thingX2')
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
            if (thing['px1'] && thing['px2'] && thing['full']) {
                return 'full'
            }
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

    class TestStorage {
        Map<String, Map> cards = new HashMap<>()
        Multimap<Link, String> reverseLinks = new ArrayListMultimap<>()

        JsonLd jsonld

        TestStorage(JsonLd jsonld) {
            this.jsonld = jsonld
        }

        void add(Map document) {
            def iris = new Document(document).getThingIdentifiers()
            jsonld.getExternalReferences(document).each { l ->
                reverseLinks.put(l, iris.first())
            }

            def card = jsonld.toCard(document, false)

            iris.each { cards.put(it, card) }
        }

        Iterable<Map> getCards(Iterable<String> iris) {
            return cards.findAll { key, value ->
                iris.contains(key)
            }.values().collect(Document.&deepCopy)
        }

        Iterable<Map> getFull(Iterable<String> iris) {
            return getCards(iris).collect{
                it['@graph'][1].put('full', 'full'); it
            }
        }

        Set<String> getReverseLinks(String iri, List<String> relations) {
            return new HashSet(relations.collect() { r ->
                reverseLinks.get(new Link(iri: iri, relation: r))
            }.flatten())
        }
    }
}
