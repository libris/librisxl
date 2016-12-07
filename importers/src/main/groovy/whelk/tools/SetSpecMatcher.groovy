package whelk.tools

/**
 * Created by Theodor on 2016-11-21.
 */
class SetSpecMatcher {

    //static List auhtLinkableFieldNames = ['100', '600', '700', '800', '110', '610', '710', '810', '130', '630', '730', '830', '650', '651', '655']
    static List ignoredAuthFields = ['180', '181', '182', '185', '162','148']
    //TODO: franska diakriter

    static Map fieldRules = [
            '100': [subFieldsToIgnore: [bib: ['0', '4'], auth: ['6']],
                    bibFields        : ['100', '600', '700', '800']],
            '110': [subFieldsToIgnore: [bib: ['0', '4'], auth: ['6']],
                    bibFields        : ['110', '610', '710']],
            '111': [subFieldsToIgnore: [bib: ['0', '4'], auth: ['6']],
                    bibFields        : ['111', '611', '711']],
            '130': [subFieldsToIgnore: [bib: ['0', '4'], auth: ['6']],
                    bibFields        : ['130','240', '630', '730', '830']],
            '150': [subFieldsToIgnore: [bib: ['0', '4'], auth: ['6']],
                    bibFields        : ['650']],
            '151': [subFieldsToIgnore: [bib: ['0', '4'], auth: ['6']],
                    bibFields        : ['651']],
            '155': [subFieldsToIgnore: [bib: ['0',  '4'], auth: ['6']],
                    bibFields        : ['655']]
    ]


    static Map matchAuthToBib(Map doc, List allSetSpecs) {
        List matchedFields = []
        List misMatchedFields = []
        List setSpecs = allSetSpecs.findAll { it -> !ignoredAuthFields.contains(it.field) }
        //Prepare setspec groups
        if (setSpecs.any()) {
            setSpecs.each { spec ->
                if (!fieldRules[spec.field])
                    throw new Exception("No rules for field ${spec.field}")
                if (!fieldRules[spec.field].subFieldsToIgnore)
                    throw new Exception("No subFieldsToIgnore for field ${spec.field}")

                spec.put("normalizedSubfields",
                        normaliseSubfields(spec.subfields)
                                .findAll { it -> !fieldRules[spec.field].subFieldsToIgnore.auth.contains(it.keySet()[0]) }
                                .collect { it -> it })
            }
            def specGroups = setSpecs.groupBy { it.field }
                    .toSorted {
                -(it.hasProperty('normalizedSubfields') ? it.normalizedSubfields.count {
                    it
                } : 0)
            }

            //Prepare bib field groups
            List<String> auhtLinkableFieldNames =
                    fieldRules.collectMany { rule ->
                        rule.getValue().bibFields.collect { s ->
                            [authfield: rule.getKey(), bibField: s]
                        }
                    }

            def bibFieldGroups =
                    doc.fields
                            .findAll { bibField -> auhtLinkableFieldNames.collect { it -> it.bibField }.contains(bibField.keySet()[0]) }
                            .groupBy { bibField -> auhtLinkableFieldNames.find { a -> a.bibField == bibField.keySet()[0] }.authfield }

            bibFieldGroups.each { group ->
                group.value.sort { field ->
                    def subfields = normaliseSubfields(field[field.keySet()[0]].subfields)
                    def foundSF = subfields.findAll { it ->
                        !fieldRules[group.key].subFieldsToIgnore.bib.contains(it.keySet()[0])
                    }
                    return -foundSF.collect { it -> it }.count { it }
                }
            }

            Map specGroupsResult = [SolidMatches:0, MisMatchesOnA:0,MisMatchesOnB:0,bibInAukt:0,auktInBib:0,doubleDiff:0, possibleMatches:0]

            def possibleBibfieldsFromSetSpec = auhtLinkableFieldNames.findAll{f->setSpecs.collect{it.field}.contains(f.authfield) }.collect{it.bibField}
            def linkableBibfields = bibFieldGroups.collectMany {c->c.value.collect{it.keySet()[0]}}
            def bibFieldsWithoutAuthField = linkableBibfields.findAll{it->!possibleBibfieldsFromSetSpec.contains(it)}

            specGroups.each { specGroup ->
                def bibFieldGroup = bibFieldGroups.find {
                    it.key == specGroup.key
                }
                if (!bibFieldGroup?.value) {
                    def file = new File("/Users/Theodor/libris/missingbibfields.tsv")
                    file << "${specGroup?.key}\t ${bibFieldsWithoutAuthField} \t${setSpecs.first()?.bibid} \t${setSpecs.first()?.id} \t  http://libris.kb.se/bib/${setSpecs.first()?.bibid}?vw=full&tab3=marc \t http://libris.kb.se/auth/${setSpecs.first()?.id}\n"
                }
                else {
                    //println "Specgroup: ${specGroup?.key}, AuthFields: ${specGroups.count { it.value }} against  ${bibFieldGroup?.key}, Bibfields: ${bibFieldGroup?.value.count { it }} "
                    specGroup.value.each { spec ->
                        // println spec.normalizedSubfields


                        def diffs = getSetDiffs(spec, bibFieldGroup.value, fieldRules)


                        def completeMatches = diffs.findAll { match ->
                            match.diff.count { it } == 0 &&
                                    match.reversediff.count { it } == 0
                        }

                        def misMatchesOnA = diffs.findAll { match ->
                            match.reversediff.find { it -> it.a } != null && match.diff.find { it -> it.a } != null
                        }

                        def misMatchesOnB = diffs.findAll { match ->
                            match.reversediff.find { it -> it.b } != null && match.diff.find { it -> it.b } != null
                        }


                        def uncertainMatches = diffs.findAll { match ->
                            (match.diff.count { it } > 0 || match.reversediff.count { it } > 0) &&
                                    match.overlap.count{it} > 0 &&
                            (match.reversediff.find { it -> it.a } == null && match.diff.find { it -> it.a } == null)


                        }

                        specGroupsResult.possibleMatches +=1
                        specGroupsResult.SolidMatches += completeMatches.count{it}
                        specGroupsResult.MisMatchesOnA +=misMatchesOnA.count{it}
                        specGroupsResult.MisMatchesOnB +=misMatchesOnB.count{it}
                        specGroupsResult.bibInAukt += uncertainMatches.count { match ->
                            match.diff.count { it } > 0 && match.reversediff.count { it } == 0
                        }
                        specGroupsResult.auktInBib += uncertainMatches.count { match ->
                            match.reversediff.count { it } > 0 &&
                                    match.diff.count { it } == 0
                        }
                        specGroupsResult.doubleDiff += uncertainMatches.count { match ->
                            match.reversediff.count { it } > 0 &&
                                    match.diff.count { it } > 0
                        }

                        uncertainMatches.each { match ->
                            def file = new File("/Users/Theodor/libris/uncertainmatches.tsv")

                            def type = ""
                            switch (match) {
                                case {it.reversediff.count { i->i } > 0 && it.diff.count { i->i } > 0 }:
                                    type = 'doubleDiff'
                                    break
                                case  {it.reversediff.count { i->i } > 0 && it.diff.count { i->i } == 0}:
                                    type = 'inverseDiff'
                                    break
                                case {it.diff.count { i->i } > 0 && it.reversediff.count { i->i } == 0}:
                                    type = 'diff'
                                    break
                            }

                            boolean partialD = false
                            boolean bibHas035a = false
                            try{
                                if(doc.fields?.'035'?.subfields?.a != null) {
                                    bibHas035a = doc.fields.'035'.subfields.a.first().any()
                                }
                            }
                            catch(any){
                                println any.message
                            }

                            try {
                                def bibD = match.reversediff.collect { it -> it.d }
                                def auktD = match.diff.collect { it -> it.d }

                                partialD = (match.spec.field == '100' && bibD.any() && auktD.any() && bibD.first() != '' && auktD.first() != '') ? (bibD.first()?.substring(0, 4) == auktD.first()?.substring(0, 4)) : false
                            }
                            catch(any)
                            {
                                println any.message
                            }

                            file << "${type}" +
                                    "\t${match.diff.count{it}}" +
                                    "\t${match.spec.field}" +
                                    "\t${match.bibfield}" +
                                    "\t${match.subfieldsInOverlap}" +
                                    "\t${match.subfieldsIndiff}" +
                                    "\t${match.subfieldsInreversediff}" +
                                    "\t${match.reversediff.count{it}}" +
                                    "\t${match.overlap.count { it }}" +
                                    "\t${doc.leader?.substring(5,6)?:''}" +
                                    "\t${doc.leader?.substring(6,7)?:''}" +
                                    "\t${doc.leader?.substring(7,8)?:''}" +
                                    "\t${doc.leader?.substring(17,18)?:''}" +
                                    "\t${doc.fields?."008"?.find{it->it}?.take(2)?:''}" +
                                    "\t _" +
                                    "\t${doc.fields?."040"?.find{it->it}?.subfields?."a"?.find{it->it}?:''}" +
                                    "\t${doc.fields?."040"?.find{it->it}?.subfields?."d"?.find{it->it}?:''}" +
                                    "\t${match.spec.data.leader?.substring(5,6)?:''}" +
                                    "\t${match.spec.data.leader?.substring(6,7)?:''}" +
                                    "\t${match.spec.data.fields?."008"?.find{it->it}?.take(2)?:''}" +
                                    "\t${match.spec.data.fields?."008"?.find{it->it}?.substring(9,10)?:''}" +
                                    "\t${match.spec.data.fields?."008"?.find{it->it}?.substring(33,34)?:''}" +
                                    "\t${match.spec.data.fields?."008"?.find{it->it}?.substring(39,40)?:''}" +
                                    "\t${match.numBibfields}" +
                                    "\t${match.numAuthFields}" +
                                    "\t${partialD}" +
                                    "\t${bibHas035a}" +
                                    "\t${match.bibset} " +
                                    "\t${match.auktset} " +
                                    "\t${setSpecs.first()?.bibid} " +
                                    "\t${setSpecs.first()?.id} " +
                                    "\n"

                        }


                    }
                }
            }
            //println "\t${specGroupsResult.SolidMatches} \t${specGroupsResult.MisMatchesOnA} \t${specGroupsResult.bibInAukt} \t${specGroupsResult.auktInBib}"
            return specGroupsResult
        }
    }

    private
    static List<Map> getSetDiffs(setSpec, bibFieldGroup, Map fieldRules) {
        bibFieldGroup.collect { field ->
            Map returnMap = [diff: null, reversediff: null, bibfield: field.keySet().first(), spec: setSpec, errorMessage: "", overlap:0, numBibfields:0, numAuthFields:0]

            def rule = fieldRules[setSpec.field]
            if (!rule) {
                returnMap.errorMessage = "No RULE ${field.keySet()[0]}"
                return returnMap

            } else {
                def bibSubFields = normaliseSubfields(field[field.keySet()[0]].subfields).findAll {
                    !rule.subFieldsToIgnore.bib.contains(it.keySet()[0])
                }
                returnMap.bibset = bibSubFields.toSet()
                returnMap.auktset = setSpec.normalizedSubfields.toSet()
                returnMap.diff = returnMap.auktset - returnMap.bibset
                returnMap.numBibfields = returnMap.bibset.count{it}
                returnMap.numAuthFields =  returnMap.auktset.count{it}
                returnMap.reversediff = returnMap.bibset -  returnMap.auktset
                returnMap.overlap =  returnMap.bibset.intersect(returnMap.auktset)

                returnMap.subfieldsInOverlap =  returnMap.overlap.collect{it->it.keySet().first()}.toSorted().join()
                returnMap.subfieldsIndiff =  returnMap.diff.collect{it->it.keySet().first()}.toSorted().join()
                returnMap.subfieldsInreversediff =  returnMap.reversediff.collect{it->it.keySet().first()}.toSorted().join()
                //TODO: print stuff to file instead
                return returnMap
            }

        }
    }

    static def normaliseSubfields(subfields) {
        subfields.collect {
            it.collect { k, v -> [(k): (v as String).replaceAll(/(^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+\u0024)|(\s)/, "").toLowerCase()] }[0]
        }

    }

    static String getSubfieldValue(Map p, String s) {

        for (subfield in p.subfields) {
            String key = subfield.keySet()[0];
            if (key == s) {
                return subfield[key]
            }
        }
        return ""
    }

}
