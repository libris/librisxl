<?xml version="1.0" encoding="iso-8859-1"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:output method="xml" encoding="ISO-8859-1"/>

	<xsl:template match="*|@*|text()">
		<xsl:copy>
  			<xsl:apply-templates select="*|@*|text()"/>
 		</xsl:copy>
	</xsl:template>

</xsl:stylesheet>
