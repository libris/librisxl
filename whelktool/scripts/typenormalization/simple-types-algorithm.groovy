import whelk.util.DocumentUtil

import java.util.regex.Pattern

import whelk.JsonLd
import whelk.Whelk
import whelk.TypeCategoryNormalizer

import static whelk.JsonLd.TYPE_KEY as TYPE
import static whelk.JsonLd.ID_KEY as ID
import static whelk.JsonLd.asList
import static whelk.JsonLd.isLink
import static whelk.util.Jackson.mapper

def missingCategoryLog = getReportWriter("missing_category_log.tsv")
def errorLog = getReportWriter("error_log.txt")


class TypeNormalizer {

    static final var ANNOTATION = '@annotation'

    TypeCategoryNormalizer normalizer

    String MARC
    String SAOGF
    String KBRDA
    String SAOBF

    TypeNormalizer(TypeCategoryNormalizer normalizer) {
        this.normalizer = normalizer
        MARC = normalizer.SCHEMES.marc + '/'
        SAOGF = normalizer.SCHEMES.saogf + '/'
        KBRDA = normalizer.SCHEMES.kbrda + '/'
        SAOBF = normalizer.SCHEMES.saobf + '/'

        // Sanity checks on startup (if these fail, either the data isn't up to date, or the domain knowledge has changed without being reflected here)
        assert normalizer.isImpliedBy([(ID): 'https://id.kb.se/term/rda/Unmediated'], [(ID): 'https://id.kb.se/term/rda/Volume'])
        assert normalizer.reduceSymbols([[(ID): 'https://id.kb.se/term/rda/Unmediated'], [(ID): 'https://id.kb.se/term/rda/Volume']]) == [[(ID): 'https://id.kb.se/term/rda/Volume']]

        assert normalizer.isImpliedBy([(ID): 'https://id.kb.se/term/saobf/AbstractElectronic'], [(ID): 'https://id.kb.se/term/rda/ComputerDiscCartridge'])
        assert normalizer.isImpliedBy([(ID): 'https://id.kb.se/term/saobf/AbstractElectronic'], [(ID): 'https://id.kb.se/term/rda/OnlineResource'])
    }

    /** Normalization action */
    boolean normalize(Map instance, Map work, boolean recursive = true) {
        var changed = false

        changed |= fixMarcLegacyInstanceType(instance, work)
        changed |= simplifyInstanceType(instance)

        changed |= moveInstanceGenreFormsToWork(instance, work) // a cleanup step
        changed |= simplifyWorkType(work)

        changed |= convertIssuanceType(instance, work)

        if (recursive) {
            changed |= normalizeLocalEntity(instance)
            changed |= normalizeLocalEntity(work)
        }

        if (changed) {
            reorderForReadability(work)
            reorderForReadability(instance)
        }

        return changed
    }

    boolean normalizeAndCheck(Map instance, Map work, def missingCategoryLog) {
        var oldItype = getType(instance)
        var oldWtype = getType(work)

        var changed = normalize(instance, work)

        if (!instance.category || instance.category.isEmpty()){
            missingCategoryLog.println("${instance[ID]}\tNo INSTANCE categories after reducing work / instance types\t${oldWtype} / ${oldItype}")
        }
        if (!work.category || work.category.isEmpty()){
            missingCategoryLog.println("${instance[ID]}\tNo WORK categories after reducing work / instance types\t${oldWtype} / ${oldItype}")
        }

        return changed
    }

    boolean normalizeLocalEntity(Map entity) {
        var entityIsDigital = entity.get(TYPE) == "DigitalResource"
        boolean changed = false
        DocumentUtil.traverse(entity) { value, path ->
            if (!path.isEmpty() && !path.contains('instanceOf') && !path.contains('hasInstance')) {
                // If the part is a Work or subclass thereof
                if (value instanceof Map && normalizer.jsonld.isSubClassOf(getType(value), "Work")) {
                    changed |= normalize([:], value, false)
                } else if (value instanceof Map && normalizer.jsonld.isSubClassOf(getType(value), "Instance")) {
                    // If the part is an instance or subclass thereof
                    // Normalize
                    changed |= normalize(value, [:], false)

                    // Always keep old instance type
                    value.put(TYPE, "Instance")
                    // Since we typically can't tell if Electronic ones are digital or not, don't assume
                    if (value.category) {
                        if (value.category.removeAll { it['@id'] == "https://id.kb.se/term/saobf/ElectronicStorageMedium" }) {
                            value.get('category', []) << [(ID): SAOBF + 'AbstractElectronic']
                        }
                        if (value.category.size() == 0) {
                            value.remove("category")
                        }
                    }
                    changed = true
                }

                        // Special handling for when the part is Electronic
                    if ((value.get(TYPE) == "PhysicalResource") && entityIsDigital) {
                        value.put(TYPE, "DigitalResource")

                }
            }
            return new DocumentUtil.Nop()
        }
        return changed
    }

    /**
     * Replace complex legacy types with structures that this algorithm will subsequently normalize.
     */
    boolean fixMarcLegacyInstanceType(Map instance, Map work) {
        var changed = false

        var itype = (String) instance[TYPE]
        def mappedCategory = itype ? normalizer.typeToCategory[itype] : null
        if (mappedCategory) {
            instance.get('carrierType', []) << [(ID): mappedCategory]
            changed = true
        } else {
            var mappedTypes = normalizer.remappedLegacyInstanceTypes[itype]
            if (mappedTypes) {
                if (mappedTypes.workCategory.contains('/rda/')) {
                    work.get('contentType', []) << [(ID): mappedTypes.workCategory]
                } else {
                    work.get('genreForm', []) << [(ID): mappedTypes.workCategory]
                }
                if (mappedTypes?.category) {
                    instance.get('carrierType', []) << [(ID): mappedTypes.category]
                }

                changed = true
            }

        }
        return changed
    }

    /**
     * - Replace deprecated issuanceType values
     * - Assign issuanceType value to work type
     * - Assign either "SingleUnit" or "MultipelUnit" to instance issuanceType
     */
    boolean convertIssuanceType(Map instance, Map work) {
        // TODO: check genres and heuristics (some Serial are mistyped!)
        var issuancetype = (String) instance.remove("issuanceType")

        // Don't put new types on things that already have one of the new types
        if (getType(work) in normalizer.newWorkTypes) {
            return true
        }

        // If there is no issuanceType, set type to the generic Work
        if (!issuancetype) {
            // Special handling for Signe works with issuanceType as work property
            var workIssuanceType = (String) work.remove("issuanceType")
            if (workIssuanceType) {
                issuancetype = workIssuanceType
            } else {
                issuancetype = 'Work'
            }
        }

        // Already new type of value == already normalized:
        if (issuancetype == 'SingleUnit' || issuancetype == 'MultipleUnits') {
            return false
        }

        if (issuancetype == 'SubCollection') {
            issuancetype = 'Collection'
        }

        // Remove ComponentPart as a work/issuance type, retaining the information with an instance category
        if (issuancetype == 'SerialComponentPart' || issuancetype == 'ComponentPart') {
            instance.get('category', []) << [(ID): SAOBF + 'ComponentPart']
            issuancetype = 'Monograph'
        }

        // Set the new work type
        work[TYPE] = issuancetype

        return true
    }

    /**
     * Work simplification.
     * - Retain information from old work type by assigning a mapped contentType or genreForm.
     * - Reduce redundancy by removing genreForms and contentTypes that can be inferred from more specific ones.
     * - Optionally, replace properties contentType and genreForm with new property category.
     */
    boolean simplifyWorkType(Map work) {
        var refSize = work.containsKey(ANNOTATION) ? 2 : 1
        if (work.containsKey(ID) && work.size() == refSize) {
            return false
        }

        var changed = false

        String wtype = getType(work)

        if (wtype == 'Text') {
            if (!asList(work.get("contentType")).findAll { (it[ID] == KBRDA + "TactileText") }) {
                // If there is NO contentType "TactileText", add "Text"
                work.get('contentType', []) << [(ID): KBRDA + 'Text']
            }
            // If there is ANY contentType "TactileText", do nothing
        } else if (wtype == 'ManuscriptText') {
            work.get('genreForm', []) << [(ID): SAOGF + 'Handskrifter']
        } else if (wtype == 'ManuscriptNotatedMusic') {
            work.get('genreForm', []) << [(ID): SAOGF + 'Handskrifter']
            work.get('contentType', []) << [(ID): KBRDA + 'NotatedMusic']
        } else if (wtype == 'Cartography') {
            if (!work['contentType'].any { it[ID]?.startsWith(KBRDA + 'Cartographic') }) {
                work.get('contentType', []) << [(ID): KBRDA + 'CartographicImage'] // TODO: good enough guess?
            }
        } else {
            def mappedCategory = wtype ? normalizer.typeToCategory[wtype] : null
            //assert mappedCategory, "Unable to map ${wtype} to contentType or genreForm"
            if (mappedCategory) {
                assert mappedCategory instanceof String
                work.get('contentType', []) << [(ID): mappedCategory]
            }
        }

        var contentTypes = normalizer.reduceSymbols(asList(work.get("contentType")))
        var workGenreForms = normalizer.reduceSymbols(asList(work.get("genreForm")))

        // Replace genreForm and contentType properties with category
        List<String> categories = []
        categories += workGenreForms + contentTypes
        work.remove("genreForm")
        work.remove("contentType")

        if (categories.size() > 0) {
            // Further reduce, e.g. removing contentTypes implied by workGenreForms.
            work.put("category", normalizer.reduceSymbols(categories))
            changed = true
        }

        return changed
    }

    /**
     * Instance simplification.
     * - Assign one of instance types PhysicalResource/DigitalResource
     * - Clean up and enrich genreForm, carrierType and mediaType again
     */
    boolean simplifyInstanceType(Map instance) {
        // Only keep the most specific mediaTypes and carrierTypes
        List mediaTypes = normalizer.reduceSymbols(asList(instance["mediaType"]))
        List carrierTypes = normalizer.reduceSymbols(asList(instance["carrierType"]))

        var changed = cleanupInstance(instance, carrierTypes)

        // Replace properties mediaType and carrierType with category
        List<Map> categories = []

        categories += mediaTypes
        categories += carrierTypes

        // Remove the ambiguous NARC term Other
        categories.removeAll { it['@id'] == "https://id.kb.se/marc/Other" }

        instance.remove("carrierType")
        instance.remove("mediaType")

        if (categories.size() > 0) {
            categories = normalizer.reduceSymbols(categories)
            instance.put("category", categories)
            changed = true
        }

        return changed
    }

    boolean cleanupInstance(Map instance, List carrierTypes) {
        List instanceGenreForms = asList(instance.get("genreForm"))

        var changed = false

        var itype = getType(instance)

        // Store information about the old instanceType
        var isElectronic = itype in ["Electronic", "DigitalResource"]

        // If an instance has a certain (old) type which implies physical electronic carrier
        // and carrierTypes corroborating this:
        if (normalizer.anyImplies(carrierTypes, 'https://id.kb.se/term/saobf/AbstractElectronic')) {
            isElectronic = true
        }

        // ----- Section: Set new instanceType -----
        /**
         * The part right below applies the new simple instance types Digital/Physical
         */
        // Don't put new types on things that already have one of the new types
        if (!(itype in normalizer.newInstanceTypes)) {
            // If the resource is electronic and has at least on carrierType that contains "Online"
            if (isElectronic && normalizer.anyImplies(carrierTypes, 'https://id.kb.se/term/rda/OnlineResource')) {
                // Apply new instance types DigitalResource and PhysicalResource
                instance.put(TYPE, "DigitalResource")
                changed = true
            } else {
                instance.put(TYPE, "PhysicalResource")
                changed = true
            }
        }

        // ----- Section: Clean up Tactile and Braille -----
        var isTactile = itype == "Tactile"
        var isBraille = dropRedundantString(instance, "marc:mediaTerm", ~/(?i)punktskrift/)

        // resourceCache.get("https://id.kb.se/term/saobf/Braille").closeMatch.findResults { it[ID] } ==
        var legacyBrailleCategories = [MARC + "Braille", MARC + "TacMaterialType-b", MARC + "TextMaterialType-c"] as Set
        var nonBrailleCarrierTypes = carrierTypes.findAll { !legacyBrailleCategories.contains(it.get(ID)) }
        if (nonBrailleCarrierTypes.size() < carrierTypes.size()) {
            isBraille = true
            //carrierTypes = nonBrailleCarrierTypes
        }

        if (looksLikeVolume(instance)) {
            carrierTypes << [(ID): KBRDA + 'Volume']
        }

        if (isTactile && isBraille) {
            carrierTypes << [(ID): SAOBF + 'Braille']
            changed = true
        }
        // No "else" clause here, since all Tactile instances are also Braille, except a handful which require manual handling.


        // ----- Section: Clean up Electronic -----

        // If something has old itype Electronic and new itype PhysicalResource,
        // we can assume it id an electronic storage medium
        if (isElectronic && (instance.get(TYPE, '') == 'PhysicalResource')) {
            carrierTypes << [(ID): SAOBF + 'ElectronicStorageMedium']
            changed = true
        }

        // Remove redundant MARC mediaTerms if the information is implied by computed category:
        var isSoundRecording = normalizer.anyImplies(carrierTypes, 'https://id.kb.se/term/saobf/SoundStorageMedium')
        if (isSoundRecording && dropRedundantString(instance, "marc:mediaTerm", ~/(?i)ljudupptagning/)) {
            changed = true
        }

        var isVideoRecording = normalizer.anyImplies(carrierTypes, 'https://id.kb.se/term/saobf/VideoStorageMedium')
        if (isVideoRecording && dropRedundantString(instance, "marc:mediaTerm", ~/(?i)videoupptagning/)) {
            changed = true
        }

        if (isElectronic && dropRedundantString(instance, "marc:mediaTerm", ~/(?i)elektronisk (resurs|utgåva)/)) {
            changed = true
        }
        if (instanceGenreForms.removeIf { it['prefLabel'] == 'E-böcker'}) {
            instanceGenreForms << [(ID): SAOBF + 'EBook']
            if (instanceGenreForms.size() > 0) {
                instance.put("genreForm", instanceGenreForms) }
            changed = true
        }

        return changed
    }

    boolean moveInstanceGenreFormsToWork(Map instance, Map work) {
        var changed = false

        var instanceGenreForms = asList(instance.get("genreForm"))

        var instanceGenreFormsToMove = instanceGenreForms.findAll {
            isLink(it) && (it.'@id'?.startsWith('https://id.kb.se/term/barngf') || it.'@id'?.startsWith('https://id.kb.se/term/saogf'))
        }

        var instanceGenreFormsToKeep  = instanceGenreForms - instanceGenreFormsToMove

        if (instanceGenreFormsToKeep) {
            instance.category = asList(instance.category) + instanceGenreFormsToKeep
            instance.remove("genreForm")
            changed = true
        }

        var instanceCategories = asList(instance.get("category"))
        if (normalizer.anyImplies(instanceCategories, 'https://id.kb.se/term/rda/Sheet')) {
            if (dropRedundantString(instance, "marc:mediaTerm", ~/(?i)affisch/)) {
                instanceGenreFormsToMove << [(ID): SAOGF + 'Poster']
            }
            changed = true
        }

        // Move all other instance GFs to work GF
        if (instanceGenreFormsToMove) {
            work.genreForm = asList(work.genreForm) + instanceGenreFormsToMove
            instance.remove("genreForm")
            changed = true
        }

        return changed
    }

    boolean looksLikeVolume(Map instance) {
        for (extent in asList(instance.get("extent"))) {
            for (label in asList(extent.get("label"))) {
                if (label instanceof String && label.matches(/(?i).*\s(s|p|sidor|pages|vol(ym(er)?)?)\b\.?/)) {
                    return true
                }
            }
        }
        return false
    }


    // ----- Typenormalizer helper methods -----

    static boolean dropRedundantString(Map instance, String propertyKey, Pattern pattern) {
        def value = instance.get(propertyKey)
        if (value instanceof List && value.size() == 1) {
          value = value[0]
        }
        if (value instanceof String && value.matches(pattern)) {
            instance.remove(propertyKey)
            return true
        }
        return false
    }

    static String getType(Map entity) {
        if (!entity.containsKey(TYPE)) {
            return null
        } else {
            return asList(entity.get(TYPE)).getFirst()
        }
    }

    protected static void reorderForReadability(Map thing) {
        var copy = thing.clone()
        thing.clear()
        for (var key : [ID, TYPE, 'sameAs', 'category']) {
            if (key in copy) thing[key] = copy[key]
        }
        thing.putAll(copy)
    }

}

// ----- Main action -----

// NOTE: Since instance and work types may co-depend; fetch work and normalize
// that in tandem. We store work ids in memory to avoid converting again.
// TODO: Instead, normalize linked works first, then instances w/o linked works?
convertedWorks = java.util.concurrent.ConcurrentHashMap.newKeySet()

typeNormalizer = new TypeNormalizer(new TypeCategoryNormalizer(getWhelk().resourceCache))

process { def doc, Closure loadWorkItem ->
    def (record, mainEntity) = doc.graph

    try {
        // If mainEntity contains "instanceOf", it's an instance
        if ('instanceOf' in mainEntity) {
            // Instances and locally embedded works
            if (ID !in mainEntity.instanceOf) {
                def work = mainEntity.instanceOf
                if (work instanceof List && work.size() == 1) {
                    work = work[0]
                }

                var changed = typeNormalizer.normalizeAndCheck(mainEntity, work, missingCategoryLog)

                if (changed) doc.scheduleSave()

            } else {
                // Instances and linked works
                def loadedWorkId = mainEntity.instanceOf[ID]
                // TODO: refactor very hacky solution...
                loadWorkItem(loadedWorkId) { workIt ->
                    def (workRecord, work) = workIt.graph

                    var changed = typeNormalizer.normalizeAndCheck(mainEntity, work, missingCategoryLog)

                    if (changed) {
                        doc.scheduleSave()
                        if (loadedWorkId !in convertedWorks) workIt.scheduleSave()
                    }
                    convertedWorks << loadedWorkId
                }
            }
        } else if ('hasInstance' in mainEntity) {
            // Else if it contains the property 'hasInstance', it's a Signe work that reuqires special handling
            var changed = typeNormalizer.normalize([:], mainEntity)
            if (changed) {
                if (mainEntity[ID] !in convertedWorks) doc.scheduleSave()
            }
            convertedWorks << mainEntity[ID]
        }
    }
    catch(Exception e) {
        errorLog.println("${mainEntity[ID]} $e")
        e.printStackTrace(errorLog)
        e.printStackTrace()
    }
}
