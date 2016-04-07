<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0"
    xmlns:marc="http://www.loc.gov/MARC21/slim" xmlns:java="http://xml.apache.org/xslt/java"
    exclude-result-prefixes="marc java">
    <xsl:output method="xml" omit-xml-declaration="no" indent="yes"/>
	<xsl:template match="*|@*|text()">
		<xsl:copy>
  			<xsl:apply-templates select="*|@*|text()"/>
 		</xsl:copy>
	</xsl:template>
<!--
	<xsl:template match="marc:record">
		<xsl:copy-of select="."/>
	</xsl:template>
-->
</xsl:stylesheet>
