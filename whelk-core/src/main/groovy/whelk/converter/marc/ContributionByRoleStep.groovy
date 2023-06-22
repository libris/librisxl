package whelk.converter.marc

import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2 as Log

import static whelk.JsonLd.asList

@Log
/**
 * Moves contribution to instance if its role (relator) domain is a subclass of
 * Embodiment. Also splits a contribution into two if it has multiple roles
 * with different relator domains.
 */
class ContributionByRoleStep extends MarcFramePostProcStepBase {

    boolean requiresResources = true
    Set<String> instanceRelators

    void init() {
        // NOTE: Assuming OK to move role with a non-Instance Embodiment domain (Item
        // or Representation) to Instance (an instance is an embodiment, a work isn't).
        instanceRelators = resourceCache?.relators?.findResults {
            def domain = ld.toTermKey(it.domain[ID])
                if (ld.isSubClassOf(domain, 'Embodiment')) it[ID]
        } as Set
        log.debug "Using as instance relations: $INSTANCE_RELATORS"
    }

    void modify(Map record, Map thing) {
        moveRoles(thing)
    }

    boolean moveRoles(Map thing) {
        def instance = thing
        def work = instance.instanceOf
        if (work == null) {
            return
        }

        if (work.keySet().every { it.startsWith('@') }) {
            return // no regular keys, likely a link
        }

        var instanceContribs = []
        var workContribs = []
        asList(work.contribution).each {
            var instanceRoles = []
            var workRoles = []
            asList(it.role).each {
                if (it[ID] in instanceRelators) {
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
                if (workRoles) {
                    contrib.role = workRoles
                }
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
            return true
        } else {
            return false
        }
    }

    void unmodify(Map record, Map thing) {
        def instance = thing
        def work = instance.instanceOf
        if (!work) {
            return
        }
        if (!instance.contribution) {
            return
        }

        if (!work.contribution) {
            work.contribution = []
        } else if (work.contribution !instanceof List) {
            work.contribution = [work.contribution]
        }
        work.contribution += asList(instance.contribution)
        instance.remove('contribution')
    }
}
