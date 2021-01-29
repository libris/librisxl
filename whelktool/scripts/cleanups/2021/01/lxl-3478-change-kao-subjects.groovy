/**
 * Changes prefLabels for a few kao and kao//eng subjects in bib and hold.
 *
 * See LXL-3478 for more info.
 */

class Script {
    static Map substitutions = [
    'https://id.kb.se/term/kao': [
      'lesbiska'        : 'lesbianism',
      'Lesbiska'        : 'Lesbianism',
      'klasskillnad'    : 'klasskillnader',
      'Klasskillnad'    : 'Klasskillnader',
      'modernismen'     : 'modernism',
      'Modernismen'     : 'Modernism',
    ],
    'https://id.kb.se/term/kao-eng': [
      'lesbians'        : 'lesbianism',
      'Lesbians'        : 'Lesbianism',
      'class difference': 'class differences',
      'Class difference': 'Class differences',
      'social activity' : 'social activities',
      'Social activity' : 'Social activities',
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
