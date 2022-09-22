/**
 * Changes prefLabels for a few kao and kao//eng subjects in bib and hold.
 *
 * See LGC-86 for more info.
 */

class Script {
    static Map substitutions = [
    'https://id.kb.se/term/kao': [
      'samer'  : 'samiska kvinnor',
      'Samer'  : 'Samiska kvinnor',
      'gotisk fiktion'      : 'gotiska berättelser',
      'Gotisk fiktion'      : 'Gotiska berättelser',
      'indianer'             : 'amerikanska urfolk',
      'Indianer'             : 'Amerikanska urfolk',
      'aboriginer'            : 'australienska urfolk',
      'Aboriginer'            : 'Australienska urfolk',
      'facklig verksamhet'            : 'fackligt arbete',
      'Facklig verksamhet'            : 'Fackligt arbete',
      'HBTQ'            : 'HBTQI',
      'queerteori'            : 'queer',
      'Queerteori'            : 'Queer',
    ],
    'https://id.kb.se/term/kao-eng': [
      'crime victims'                  : 'victims of crimes',
      'Crime victims'                  : 'Victims of crimes',
      'day nurseries'                : 'day care centers',
      'Day nurseries'                : 'Day care centers',
      'office workers' : 'clerks',
      'Office workers' : 'Clerks',
      'nativity'                  : 'fertility, Human',
      'Nativity'                  : 'Fertility, Human',
      'clergymen'                : 'priests',
      'Clergymen'                : 'Priests',
      'sculptresses'              : 'sculptors',
      'Sculptresses'              : 'Sculptors',
      'actresses'                       : 'actors',
      'Actresses'                       : 'Actors',
      'care of the aged'                    : 'geriatric nursing',
      'Care of the aged'                    : 'Geriatric nursing',
      'towns'              : 'cities and towns',
      'Towns'              : 'Cities and towns',
      'abuse'              : 'assault and battery',
      'Abuse'              : 'Assault and battery',
      'indians'              : 'native Americans',
      'Indians'              : 'Native Americans',
      'american indians'              : 'native Americans',
      'American indians'              : 'Native Americans',
      'aborigines'              : 'native Australians',
      'Aborigines'              : 'Native Australians',
      'home working'              : 'home labor',
      'Home working'              : 'Home labor',
      'LGBTQ'              : 'LGBTQI',
      'queer theory'              : 'queer',
      'Queer theory'              : 'Queer',
    ]
  ]
}

String where = """
  (
    collection = 'bib' AND
    (
      data#>'{@graph,1,instanceOf,subject}' @> '[{"inScheme": {"@id": "https://id.kb.se/term/kao"}}]' OR
      data#>'{@graph,1,instanceOf,subject}' @> '[{"inScheme": {"@id": "https://id.kb.se/term/kao-eng"}}]'
    )
  )
  OR
  (
    collection = 'hold' AND
    (
      data#>'{@graph,1,subject}' @> '[{"inScheme": {"@id": "https://id.kb.se/term/kao"}}]' OR
      data#>'{@graph,1,subject}' @> '[{"inScheme": {"@id": "https://id.kb.se/term/kao-eng"}}]'
    )
  )
  AND deleted = false
  """

selectBySqlWhere(where) { data ->
    def thing = data.graph[1]
    boolean modified = false
    List subjects = []

    if (thing.subject) {
        subjects.addAll(asList(thing.subject))
    }

    if (thing.instanceOf?.subject) {
        subjects.addAll(asList(thing.instanceOf.subject))
    }

    subjects.each { Map subject ->
        if (
          subject['@type'] &&
          subject['inScheme'] &&
          subject['inScheme']['@id'] &&
          Script.substitutions.containsKey(subject['inScheme']['@id'])) {
            modified |= fixSubject(subject)
      }
    }

    if (modified) {
        data.scheduleSave()
    }
}

private boolean fixSubject(Map subject) {
    String id = subject['inScheme']['@id']
    boolean fixed = false

    if (subject['@type'] == 'ComplexSubject') {
        subject.termComponentList?.each { term ->
            if (term.prefLabel && Script.substitutions[id].containsKey(term.prefLabel)) {
                term.prefLabel = Script.substitutions[id][term.prefLabel]
                fixed = true
            }
        }
        // Recreate composite prefLabel
        if (subject.prefLabel && fixed) {
            subject.prefLabel = subject.termComponentList
                            .findAll { it.containsKey('prefLabel') }
                            .collect { it.prefLabel }.join('--')
        }
    } else if (subject['@type'] == 'Topic') {
        if (subject.prefLabel && Script.substitutions[id].containsKey(subject.prefLabel)) {
            subject.prefLabel = Script.substitutions[id][subject.prefLabel]
            fixed = true
        }
    }

    return fixed
}

private List asList(Object o) {
    (o instanceof List) ? (List) o : (o ? [o] : [])
}