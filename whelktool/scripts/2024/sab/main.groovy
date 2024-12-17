SAB = "https://id.kb.se/term/kssb"

SAB_MAP = [:]

missing = [:]

boolean interpretClassification(Map thing) {
  var modified = false
  var isInstance = 'instanceOf' in thing

  List<String> additionalCodes = []

  for (Map cls : asList(thing.classification)) {
    if (asList(cls.inScheme).any {
      it[ID] == SAB || it.code?.toLowerCase()?.startsWith("kssb")
    }) {
      def clsCode = cls.code

      if (clsCode instanceof List && clsCode.size() > 0) {
        additionalCodes = clsCode[1..-1].findAll { it instanceof String }
        clsCode = clsCode[0]
      }

      if (clsCode !instanceof String) {
        continue
      }

      var newCls = getClassification(clsCode, isInstance)
      if (newCls != null) {
        cls.clear()
        cls.putAll(newCls)
        modified = true
      }
    }
  }

  if (thing.classification instanceof List && additionalCodes) {
    additionalCodes.each {
      var newCls = getClassification(it, isInstance)
      if (newCls != null) {
        thing.classification << newCls
      }
    }
  }

  if (isInstance) {
    if (ID !in thing.instanceOf) {
      modified |= interpretClassification(thing.instanceOf)
    }
  }

  return modified
}

Map getClassification(String clsCode, isInstance=false) {
  List<Map> sabRefs = null

  if (isInstance) {
    var mediaSubdiv = clsCode.find(/(\/[A-Z]+)/)
    if (mediaSubdiv in SAB_MAP) {
      var basecode = clsCode.replace(mediaSubdiv, '')
      if (basecode in SAB_MAP) {
        sabRefs = [ [(ID): SAB_MAP[basecode]], [(ID): SAB_MAP[mediaSubdiv]] ]
      }
    }
  }

  if (!sabRefs) {
    if (clsCode in SAB_MAP) {
      sabRefs = [ [(ID): SAB_MAP[clsCode]] ]
    } else if (clsCode) {
      sabRefs = splitSabCode(clsCode)
    }
  }

  if (sabRefs) {
    var cls = [:]
    if (sabRefs.size() == 1) {
      cls.putAll(sabRefs[0])
    } else {
      cls[TYPE] = 'Classification'
      cls.code = clsCode
      cls.inScheme = [(ID): SAB]
      cls.broader = sabRefs

      var missed = sabRefs.findAll { ID !in it && it[TYPE] != 'Resource' }
      if (missed) {
        missed.each {
          missing.get(it.code, []) << clsCode
        }
      }
    }

    return cls
  } else if (!clsCode.contains('z ')) {
    missing.get(clsCode, []) << ''
  }

  return null
}

List<Map> splitSabCode(String code) {
  var chunks = parseSabCode(code)
  return chunks.findResults { chunk ->
    if (chunk.size() == 0) {
      return null
    }

    if (chunk.indexOf(' ') > -1) {
      if (chunk.startsWith('z ')) {
        return [(TYPE): 'Resource', label: chunk.substring(2)]
      }
      return null
    }

    // TODO: Improve? Other parts may be subcomponents of chunks[0]...
    if (chunk =~ /^[.:(]/) {
        var prefixedCode = chunks[0][0] + chunk
        if (prefixedCode in SAB_MAP) {
          chunk = prefixedCode
        }
    }

    if (chunk in SAB_MAP) {
        return [(ID): SAB_MAP[chunk]]
    }

    //var slug = URLEncoder.encode(chunk)
    //return [(ID): "${SAB}/${slug}"]
    return [code: chunk]
  }
}

List<String> parseSabCode(String code) {
    // TODO: starts with any /[a-z]/?
    if (code.startsWith('u')) {
        code = code.substring(1) + ',u'
    }

    var spaceIdx = code.indexOf('z ')
    var rest = []
    if (spaceIdx > -1) {
      rest << code.substring(spaceIdx)
      code = code.substring(0, spaceIdx)
    }

    return code.split(/(?=z\s+.+|\(\w+\)|[,\/=:.-])/) + rest
}


selectBySqlWhere("""
    data#>>'{@graph,0,inDataset,0,@id}' IN ('https://id.kb.se/dataset/sab', 'https://id.kb.se/dataset/sab/precoordinated') AND
    data#>>'{@graph,1,inScheme,@id}' = 'https://id.kb.se/term/kssb' AND
    data#>>'{@graph,1,@type}' != 'Collection'
""") {
  def cls = it.graph[1]

  // FIXME: Make *all* sab.ttl codes unique! (Use altLabel for "shortcode"?)
  def code = cls.code
  if (cls[TYPE].indexOf('Subdivision') > -1) {
    def firstIdChar = URLDecoder.decode(cls[ID].substring(SAB.size() + 1))[0]
    if (!cls.code.startsWith(firstIdChar)) {
        code = firstIdChar + code
    }
  }
  //if (code != cls.code) println code + ' => ' + cls.code

  SAB_MAP[code] = cls[ID]
}
println "Loaded ${SAB_MAP.size()} SAB references"


selectBySqlWhere("""
    collection <> 'hold' AND
    data#>>'{@graph,1}' LIKE '%kssb%'
""") { data ->
/*
selectByIds(['8rkj0wql14q40gb', '2kc9d80d2kl6v14']) { data ->
*/
  def (record, instance) = data.graph

  if (interpretClassification(instance)) {
    data.scheduleSave()
  }
}

missingLog = getReportWriter("sab-missing.txt")
missing.keySet().sort().each {
  missingLog.println "${it}	${missing[it].size()}	${missing[it].unique().join(' | ')}"
}
