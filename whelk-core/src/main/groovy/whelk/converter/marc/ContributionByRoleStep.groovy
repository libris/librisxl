package whelk.converter.marc

import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2 as Log
import whelk.filter.BlankNodeLinker

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
    BlankNodeLinker relatorLinker

    void init() {
        def relatorResources = resourceCache?.relatorResources

        if (!relatorResources) return

        relatorLinker = relatorResources.relatorLinker
        // NOTE: Assuming OK to move role with a non-Instance Embodiment domain (Item
        // or Representation) to Instance (an instance is an embodiment, a work isn't).
        instanceRelators = relatorResources.relators.findResults {
            def domainRef = it.domain?[ID]
            if (domainRef) {
              def domain = ld.toTermKey(domainRef)
              if (ld.isSubClassOf(domain, 'Embodiment')) {
                return it[ID]
              }
            }
        } as Set
        log.debug "Using as instance relations: $instanceRelators"
    }

    void modify(Map record, Map thing) {
        if (!relatorLinker || !instanceRelators) {
            log.error("Conversion failed: Missing required resources")
            return
        }
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
            relatorLinker.linkAll(it, 'role')
            asList(it.role).each {
                if (it[ID] in instanceRelators) {
                    instanceRoles << it
                } else {
                    workRoles << it
                }
            }

            if (instanceRoles) {
              def contrib = it.clone()
                contrib.role = instanceRoles
                setToPlainContribution(contrib)
                instanceContribs << contrib
            }
            if (workRoles) {
                def contrib = it.clone()
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
        var instanceContribs = asList(instance.remove('contribution'))
        instanceContribs.each { setToPlainContribution(it) }
        work.contribution += instanceContribs
    }

    void setToPlainContribution(contrib) {
      if (contrib[TYPE] != 'Contribution') {
        contrib[TYPE] = 'Contribution'
      }
    }
}
