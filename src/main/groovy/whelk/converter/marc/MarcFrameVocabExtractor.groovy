package whelk.converter.marc

class MarcFrameVocabExtractor {

    public static void main(String[] args) {
        extract(new MarcFrameConverter())
    }

    static void extract(MarcFrameConverter converter) {
        println '''\
        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        prefix owl: <http://www.w3.org/2002/07/owl#>
        prefix skos: <http://www.w3.org/2004/02/skos/core#>
        prefix sdo: <http://schema.org/>
        prefix : <https://id.kb.se/vocab/>
        prefix marc: <https://id.kb.se/marc/>

        construct {

            ?domainType a owl:Class .

            ?resourceType a owl:Class .

            ?term skos:closeMatch ?marcId .

            ?link a owl:ObjectProperty ;
                sdo:domainIncludes ?domainType ;
                sdo:rangeIncludes ?resourceType, ?enumType .

            ?property a owl:DatatypeProperty;
                sdo:domainIncludes ?propDomainType .

            ?domainType rdfs:subClassOf ?restriction .

            ?restriction a owl:Restriction ;
                owl:onProperty ?link ;
                owl:someValuesFrom ?enumType .

        } where {

            values (?marcId ?domainType ?link ?resourceType ?enumType ?property) {

        '''.stripIndent()

        use (FieldHandlerViewCategory) {
            converter.conversion.marcRuleSets.values().each {
                it.fieldHandlers.values().each {
                    it.printVocab()
                }
            }
        }

        println '''

            }

            bind(if(bound(?enumType) && bound(?domainType) && bound(?link),
                        bnode(concat(str(?domainType), str(?link))),
                        ?_) as ?restriction)

            bind(if(bound(?link),
                        ?resourceType,
                        ?domainType) as ?propDomainType)

            bind(if(bound(?property),
                        ?property,
                        ?link) as ?term)
        }
        '''.stripIndent()
    }

}

class FieldHandlerViewCategory {

    static void printVocab(MarcFixedFieldHandler self, scopedToType=null) {
        self.columns.each {
            it.printVocab(self.ruleSet, self.tag, scopedToType)
        }
    }

    static void printVocab(MarcFixedFieldHandler.Column self, MarcRuleSet ruleSet, tag, scopedToType) {
        def pos = "$self.start" + (self.end > self.start+1 ? '_' + "${self.end - 1}" : '')
        if (scopedToType) {
            tag = "$tag-$scopedToType"
        }
        def enumType = self.tokenMapName ? "marc:$self.tokenMapName" : null
        def domainType = scopedToType ?: ruleSet.topPendingResources[self.aboutEntityName].resourceType
        printTermRow([ruleSet.name, tag, pos], domainType, self.link, self.resourceType, enumType, self.property)
    }

    static void printVocab(TokenSwitchFieldHandler self) {
        def tNames = new HashSet()
        self.handlerMap.each { token, handler ->
            def tName = self.tokenNames[token]
            if (!(tName in tNames)) {
                tNames << tName
                handler.printVocab(tName)
            }
        }
    }

    static void printVocab(MarcSimpleFieldHandler self) {
        printTermRow([self.ruleSet.name, self.tag], self.domainType, null, null, null, self.property)
    }

    static void printVocab(MarcFieldHandler self) {
        printTermRow([self.ruleSet.name, self.tag], self.domainType, self.link, self.resourceType, null, null)
        self.subfields.values().each {
            it?.printVocab()
        }
        if (self.matchRules) {
            self.matchRules.each {
                it.handler.printVocab()
            }
            println()
        }
    }

    static void printVocab(MarcSubFieldHandler self) {
        def domainType = self.about && self.fieldHandler.pendingResources ?
            self.fieldHandler.pendingResources[self.about].resourceType
            : self.fieldHandler.link ? self.fieldHandler.resourceType
            : self.fieldHandler.domainType
        printTermRow([self.fieldHandler.ruleSet.name, self.fieldHandler.tag, self.code],
                domainType, self.link, self.resourceType, null, self.property)
    }

    static String getDomainType(BaseMarcFieldHandler self) {
        assert self.ruleSet.topPendingResources[self.aboutEntityName], self.aboutEntityName
        return self.definesDomainEntityType ?:
            self.ruleSet.topPendingResources[self.aboutEntityName].resourceType
    }

    static void printTermRow(List marcChain, domainType, link, resourceType, enumType, property) {
        def marcId = "marc:${marcChain.join('-')}"
            .replaceAll(/\[[^\]]+\]/, '')
        printTerms(marcId, domainType, link, resourceType, enumType, property)
    }

    static void printTerms(Object... terms) {
        print('(')
        terms.eachWithIndex { String it, int i ->
            if (i) print('\t')
            def term = it ? (
                    it.startsWith("'") || it.contains(':') ? it
                    : it == '@type' ? 'rdf:type'
                    : ":$it"
                ) : 'UNDEF'
            print(term)
        }
        println(')')
    }

}
