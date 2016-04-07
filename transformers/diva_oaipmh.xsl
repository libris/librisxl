<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0"
    xmlns:oai="http://www.openarchives.org/OAI/2.0/"
    xmlns:dc="http://purl.org/dc/elements/1.1/"
    xmlns:marc="http://www.loc.gov/MARC21/slim"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:java="http://xml.apache.org/xslt/java"
    xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
    exclude-result-prefixes="marc">

	<xsl:output method="xml" indent="yes" encoding="UTF-8"/>
        <xsl:param name="f008_00-05">050420</xsl:param>

        <xsl:template match="/">
            <xsl:apply-templates select="/oai:OAI-PMH"/>
        </xsl:template>

        <xsl:template match="oai:OAI-PMH">
            <xsl:apply-templates select="oai:ListRecords"/>
        </xsl:template>
 
        <xsl:template match="oai:ListRecords">
            <collection xmlns="http://www.loc.gov/MARC21/slim">
            <xsl:apply-templates select="oai:record"/>
            </collection>
        </xsl:template>

        <xsl:template match="oai:record">
            <xsl:apply-templates select="oai:metadata"/> 
        </xsl:template>

        <xsl:template match="oai:metadata">
            <xsl:apply-templates select="marc:record"/>
        </xsl:template>

	<xsl:template match="marc:record">
            <xsl:variable name="rawTimeStamp"><xsl:value-of select="java:format(java:java.text.SimpleDateFormat.new('yy-MM-dd'), java:java.util.Date.new())"/></xsl:variable>
            <xsl:variable name="timeStamp" select="translate($rawTimeStamp, '-', '')"/>
            <record type="Bibliographic">
                    <xsl:apply-templates select="marc:leader"/>
                    <xsl:apply-templates select="marc:controlfield"/>
                    <datafield tag="035" ind1=" " ind2=" "><subfield code="a">(DiVA)<xsl:value-of select="marc:controlfield[@tag = '001']"/></subfield></datafield>
                    <xsl:apply-templates select="marc:datafield[@tag != '852']"/>
            </record>
            <xsl:for-each select="marc:datafield[@tag='852' and marc:subfield[@code = 'b']]">
                <record type="Holdings">
                    <leader>#####nx  a22#####1n 4500</leader>
                    <controlfield tag="008"><xsl:value-of select="$timeStamp"/>||0000|||||001||||||000000</controlfield>
                    <xsl:apply-templates select="."/>
                </record>
            </xsl:for-each>
        </xsl:template>
        
        <xsl:template match="*|@*|text()">
                <xsl:copy>
                        <xsl:apply-templates select="*|@*|text()"/>
                </xsl:copy>
        </xsl:template>
</xsl:stylesheet>

