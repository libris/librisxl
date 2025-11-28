import org.apache.commons.lang3.StringEscapeUtils

String qry = """
collection = 'bib'
and deleted = 'false'
and data#>>'{@graph,0,descriptionCreator,@id}' = 'https://libris.kb.se/library/BOKR'
"""

selectBySqlWhere(qry) { b ->

	def item = b.getGraph()[1]

	item.summary.each { summary ->
			if ( summary.'@type' == 'Summary' ) {
				//decode xml/html entities
				if ( summary.label =~ /&[^ ]+;/ ) { 
					if (summary.label instanceof List) {
						summary.label = summary.label.collect { li -> StringEscapeUtils.unescapeXml(StringEscapeUtils.unescapeHtml4(li)) }
					} else {
						summary.label = StringEscapeUtils.unescapeXml(StringEscapeUtils.unescapeHtml4(summary.label))
					}

					//resave
					b.scheduleSave(loud: true)

					//println(b.getGraph()[1])
				}
			}
	}
}
