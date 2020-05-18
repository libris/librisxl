// Infer Instance-subclass

INSTANCE = 'Instance'
PRINT = 'Print'
ELECTRONIC = 'Electronic'
MANUSCRIPT = 'Manuscript'
TACTILE = 'Tactile'

TEXT_INSTANCE = 'TextInstance'
NOTATEDMUSIC_INSTANCE = 'NotatedMusicInstance'
STILLIMAGE_INSTANCE = 'StillImageInstance'
PROJECTEDIMAGE_INSTANCE = 'ProjectedImageInstance'
KIT_INSTANCE = 'KitInstance'

COMBINED_INSTANCE_TYPES = [
    TEXT_INSTANCE,
    NOTATEDMUSIC_INSTANCE,
    STILLIMAGE_INSTANCE,
    PROJECTEDIMAGE_INSTANCE,
    KIT_INSTANCE
] as Set

MANUSCRIPT_WORK_TYPES = [
    'ManuscriptText': 'Text',
    'ManuscriptNotatedMusic': 'NotatedMusic',
    'ManuscriptCartography': 'Cartography'
]


def deriveTypeFromMediaTerm(mediaTerm) {
    // TODO: map marc:mediaTerm to mediaType, carrierType or genreForm (sometimes of work)? return [type: X, carrier: Y, genreForm: Z]
    switch (mediaTerm) {
        case ~/(?i)ele[ck]tron.*.+|multimedia.*/:
        ELECTRONIC
        break

        case ~/(?i)\s*handskrift|\s*manus[ck]ript.*/:
        MANUSCRIPT
        break

        case ~/(?i).*tryck.*/:
        PRINT
        break

        case ~/(?i)\s*punktskrift.*/:
        TACTILE
        break

        case ~/(?i)\s*kombinerat material.*/: // work is Kit/MixedMaterial ?
        INSTANCE
        break
    }
}


    /* TODO:
    // TextInstance:
    "@value Kombinerat material": 1022,
    "@value kombinerat material": 3,
    "@value Kombinerad material] ...": 1 + 1 + ...,

    "@value Elektronisk resurs": 11,
    "@value Multimédia multisupport": 1,
    "@value Electronic resource": 2,
    "@value Elektronisk version": 1,
    "@value elektronisk ressurs": 1,

    "@value Punktskrift": 24,
    "@value Punktskrift] ...": 1 + ...,

    "@value Handskrift": 10,

    "@value Musiktryck": 7,

    "@value Tidskrift": 2,

    "@value graphic novel": 1,

    "@value Ljudupptagning": 2,

    // StillImageInstance:
    "@value Affisch": 6181,

    // NotatedMusicInstance
    "@value Musiktryck": 2635,
    "@value musikktrykk": 4,
    "@value musiktryck": 11,
    "@value Elektronisk resurs": 1,
    "@value Noten": 8,
    "@value Nuottijulkaisu": 10,
    "@value Musikdruck": 14,
    "@value Musiktryck] : Op. 13 : [avec accompagnement d'orchestre": 1,

    // StillImageInstance
    "@value Bild": 20316,
    "@value Elektronisk resurs": 5896,
    "@value Affisch": 6181,
    "@value Affisch] : ...": 1,
    "@value Affisch] :": 297,
    "@value Samling": 42,
    "@value Samling] [Bild],": 3,
    "@value Bild] : Gotland och Wisby i taflor": 19,
    "@value Musiktryck": 1,
    */

carrierTypeMap = [
    "https://id.kb.se/marc/RegularPrint": PRINT,
    "https://id.kb.se/marc/RegularPrintReproduction": PRINT,
    "https://id.kb.se/marc/RegularPrintReproductionEyeReadablePrint": PRINT,
    "https://id.kb.se/marc/LargePrint": PRINT, // TODO: see bib 340
        "https://id.kb.se/marc/TextMaterialType-b": PRINT,

    "https://id.kb.se/marc/DirectElectronic": ELECTRONIC,
    "https://id.kb.se/marc/Electronic": ELECTRONIC,
    "https://id.kb.se/marc/Microfiche": ELECTRONIC,
    "https://id.kb.se/marc/Microfilm": ELECTRONIC,
    "https://id.kb.se/marc/Microopaque": ELECTRONIC,
    "https://id.kb.se/marc/Online": ELECTRONIC,
    "https://id.kb.se/marc/OnlineResource": ELECTRONIC,
    "https://id.kb.se/marc/OpticalDisc": ELECTRONIC,
    "https://id.kb.se/marc/SoundCassette": ELECTRONIC,
    "https://id.kb.se/marc/SoundDisc": ELECTRONIC,
    "https://id.kb.se/term/rda/AudioDisc": ELECTRONIC,
    "https://id.kb.se/term/rda/ComputerDisc": ELECTRONIC,
    "https://id.kb.se/term/rda/Microfiche": ELECTRONIC,
    "https://id.kb.se/term/rda/MicrofilmReel": ELECTRONIC,
    "https://id.kb.se/term/rda/OnlineResource": ELECTRONIC,
    "https://id.kb.se/term/rda/Videocassette": ELECTRONIC,
    "https://id.kb.se/term/rda/Videodisc": ELECTRONIC,
        "https://id.kb.se/marc/VideoMaterialType-d": ELECTRONIC,

    "https://id.kb.se/marc/Braille": TACTILE,
        "https://id.kb.se/marc/TextMaterialType-c": TACTILE,

    /* PRINT OR MANUSCRIPT ?
    "https://id.kb.se/term/rda/Volume",
        "https://id.kb.se/term/rda/carrier/nc",
    "https://id.kb.se/marc/TextInLooseleafBinder"
    // "https://id.kb.se/term/rda/Sheet": 1119 + 7801 for StillImageInstance
    // "https://id.kb.se/term/rda/Card": 7, + 59 for StillImageInstance
    */

    /*
    // just Instance?
    "https://id.kb.se/term/rda/Object": 'PhysicalObject' ...
    */
]

mediaTypeMap = [
    "https://id.kb.se/marc/RegularPrint": PRINT,
    "https://id.kb.se/marc/LargePrint": PRINT,

    //"https://id.kb.se/term/rda/Unmediated": PRINT or MANUSCRIPT,
    //"https://id.kb.se/term/rda/Volume": PRINT or MANUSCRIPT,

    "https://id.kb.se/term/rda/Video": ELECTRONIC,
    "https://id.kb.se/term/rda/Audio": ELECTRONIC,
    "https://id.kb.se/term/rda/Computer": ELECTRONIC,
    "https://id.kb.se/term/rda/Microform": ELECTRONIC,
    "https://id.kb.se/term/rda/ComputerDisc": ELECTRONIC,
    "https://id.kb.se/marc/ComputerOpticalDiscCartridge": ELECTRONIC,
    "https://id.kb.se/marc/SoundDisc": ELECTRONIC,
    "https://id.kb.se/marc/OpticalDisc": ELECTRONIC,

    // TODO: Slide...
    //"https://id.kb.se/term/rda/Projected": 37 // for 'StillImageInstance'
]

instanceGenreFormMap = [
    "https://id.kb.se/marc/Print": PRINT,
    "https://id.kb.se/term/saogf/Handskrifter": MANUSCRIPT,
]

selectByIds([
    'bvn2bm8n5qv8m1m', // Manuscript
]) { data ->
/*
selectBySqlWhere('''
    collection = 'bib' AND
    -- data#>>'{@graph,1,@type}' = 'Instance' OR
    data#>>'{@graph,1,@type}' LIKE '%Instance'
''') { data ->
*/
    def (record, instance, work) = data.graph

    assert instance[TYPE] == INSTANCE || instance[TYPE] in COMBINED_INSTANCE_TYPES

    if (work) {
        if (moveGenreForms(instance, work)) {
            data.scheduleSave()
        }

    }
    String mappedType = computeType(instance, work) ?: 'Instance'

    if (mappedType && mappedType != instance[TYPE]) {
        instance[TYPE] = mappedType
        data.scheduleSave()
    }
}

boolean moveGenreForms(Map instance, Map work) {
    def instanceGenreForms = []
    work?.genreForm?.removeAll {
        if (it[ID] in instanceGenreFormMap) {
            instanceGenreForms << it
            return true
        }
    }
    if (instanceGenreForms) {
        if (!instance.genreForm) {
            instance.genreForm = []
        } else if ((!instance.genreForm instanceof List)) {
            instance.genreForm = [instance.genreForm]
        }
        instance.genreForm += instanceGenreForms
    }
}

String computeType(Map instance, Map work) {

    if (instance[TYPE] == STILLIMAGE_INSTANCE) {
        // FIXME: keep as is? Check assumptions?
    }

    def typeDerivedFromMediaTerm = deriveTypeFromMediaTerm(instance['marc:mediaTerm'])
    if (typeDerivedFromMediaTerm) {
        instance.remove('marc:mediaTerm')
    }

    def mappedType = instance.carrierType.findResult { carrierTypeMap[it[ID]] }
    if (!mappedType) {
        mappedType = instance.mediaType.findResult { mediaTypeMap[it[ID]] }
    }
    if (!mappedType) {
        mappedType = instance.genreForm.findResult { instanceGenreFormMap[it[ID]] }
    }

    /* Could check these too, but we won't:
    OR MATCH 'hasTitle[0].subtitle' match /.+\s+\[([^\]]+)\]/ IN
        ANY OF ...
    OR MATCH 'identifiedBy.qualifier' ...
    */

    def simpleWorkType = MANUSCRIPT_WORK_TYPES[work?.get(TYPE)]
    if (simpleWorkType) {
        if (mappedType) {
            assert mappedType == MANUSCRIPT
        } else {
            mappedType = MANUSCRIPT
        }
        work[TYPE] = simpleWorkType
    } else if (mappedType && instance[TYPE] in COMBINED_INSTANCE_TYPES) {
        //assert instance[TYPE].startsWith(work[TYPE])
        if (!instance[TYPE].startsWith(work[TYPE])) {
            System.err.println "Instance/Work type mismatch: ${instance[TYPE]} / ${work[TYPE]}"
        }
    }

    if (typeDerivedFromMediaTerm) {
        if (typeDerivedFromMediaTerm == INSTANCE) {
            mappedType = typeDerivedFromMediaTerm
        } else {
            assert !mappedType || typeDerivedFromMediaTerm == mappedType
            mappedType = typeDerivedFromMediaTerm
        }
    }

    assert mappedType != 'TextInstance'

    return mappedType
}

/*
  'KitInstance' JUST 'Instance' TODO: comment out all MARC enums for this!
    assert work[TYPE] == 'Kit'
  'ProjectedImageInstance' TO BE CLEANED by EliBen (not to be used anymore(?))
*/

// TODO: add specific type heuristics to utility for re-use in importers?
