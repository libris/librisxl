/**
 * LXL-2512: Move contribution by relator domain
 *
 * Moves contribution to instance if its role (relator) domain is a subclass of
 * Embodiment. Also splits a contribution into two if it has multiple roles
 * with different relator domains.
 */
whelk = whelk.Whelk.createLoadedCoreWhelk()
ld = whelk.jsonld

// NOTE: Assuming OK to move role with a non-Instance Embodiment domain (Item
// or Representation) to Instance (an instance is an embodiment, a work isn't).
INSTANCE_RELATORS = queryDocs(['@type': ['Role']]).findResults {
     def domain = ld.toTermKey(it.domain[ID])
     if (ld.isSubClassOf(domain, 'Embodiment')) it[ID]
} as Set
println "Using as instance relations: $INSTANCE_RELATORS"

// TODO: Optimize to only select those with instance relations?
def where = """
  collection = 'bib' AND deleted = false
  AND data#>'{@graph, 1, instanceOf, contribution}' notnull
"""

selectBySqlWhere(where) {
    def instance = it.graph[1]
    def instanceType = instance[TYPE]

    def work = instance.instanceOf
    if (work == null) {
        return
    }
    assert work.keySet().grep { !it.startsWith('@') }.size() > 1 // not just a link

    var instanceContribs = []
    var workContribs = []
    asList(work.contribution).each {
        var instanceRoles = []
        var workRoles = []
        asList(it.role).each {
            if (it[ID] in INSTANCE_RELATORS) {
                instanceRoles << it
            } else {
                workRoles << it
            }
        }
        def contrib = it.clone()
        if (instanceRoles) {
            contrib.role = instanceRoles
            instanceContribs << contrib
        } else {
            contrib.role = workRoles
            workContribs << contrib
        }
    }

    if (instanceContribs) {
        if (!workContribs) {
            work.remove('contribution')
        } else {
            work.contribution = workContribs
        }
        if (!instance.contribution) {
            instance.contribution = []
        }
        instance.contribution += instanceContribs
        it.scheduleSave()
    }
}
