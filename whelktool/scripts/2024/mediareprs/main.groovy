static final var ANNOTATION = "@annotation"

static boolean simplifyAssociatedMedia(Map instance) {
  var modified = false

  for (tuple in [
    ["associatedMedia", "hasRepresentation"],
    ["isPrimaryTopicOf", "associatedMedia"]
  ]) {
    def (givenRel, mediaRel) = tuple
    for (Object qualification : asList(instance.get(givenRel))) {
      def uri = qualification.get("uri")
      if (uri) {
        if (uri instanceof Map) {
          uri = ((Map) uri).get(VALUE)
        }
        asList(qualification.getOrDefault("cataloguersNote", "")).each {
          if (it.toLowerCase().contains("digipic")) mediaRel = "image"
        }
        if (uri instanceof List) {
          assert uri.size() == 1
          uri = uri[0]
        }
        var mediaObj = [(ID): uri]
        instance.put(mediaRel, mediaObj)
        var scope = qualification.get("appliesTo")
        Map annot
        if ((scope instanceof Map && scope.containsKey("label"))) {
          if (!mediaObj.containsKey(ANNOTATION)) mediaObj.put(ANNOTATION, [:])
          annot = (Map) mediaObj.get(ANNOTATION)
          annot.put("label", ((Map) scope).get("label"))
        }
        String note = qualification.get("marc:publicNote")
        if (note) {
          if (!mediaObj.containsKey(ANNOTATION)) mediaObj.put(ANNOTATION, [:])
          annot = (Map) mediaObj.get(ANNOTATION)
          annot.put("comment", note)
        }
      }
      continue
    }
    instance.remove(givenRel)
    modified = true
  }

  return modified
}

selectByCollection('bib') { doc ->
  def (record, instance) = doc.graph
  var modified = simplifyAssociatedMedia(instance)
  if (modified) {
    doc.scheduleSave()
  }
}
