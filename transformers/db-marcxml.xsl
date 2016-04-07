<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0"
    xmlns:oai="http://www.openarchives.org/OAI/2.0/"
    xmlns:marc="http://www.loc.gov/MARC21/slim"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:output method="xml" indent="yes"/>
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
            <record type="Bibliographic">
                <leader><xsl:value-of select="marc:leader"/></leader>

                <xsl:for-each select="marc:controlfield[@tag != '003']">
                    <controlfield tag="{@tag}"><xsl:value-of select="."/></controlfield>
                </xsl:for-each>

                <xsl:for-each select="marc:datafield[@tag &gt; 9 and @tag &lt; 900 and not(@tag = '650' and @ind2 = '0')]">
                    <xsl:if test="not(marc:subfield[@code = '5'])">
                        <datafield tag="{@tag}" ind1="{@ind1}" ind2="{@ind2}">
                            <xsl:for-each select="marc:subfield">
                                <subfield code="{@code}"><xsl:value-of select="."/></subfield>
                            </xsl:for-each>
                        </datafield>
                    </xsl:if>
                </xsl:for-each>
            </record>

            <!-- Holdings record -->
            <record type="Holdings" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">
                <!-- leader -->
                <leader>#####nx   22#####1n 4500</leader>
 
	        <!-- 008 -->
	        <controlfield tag="008"><xsl:value-of select="concat($f008_00-05,'||    |||||001||||||000000')"/></controlfield>

                <!-- 852 -->
                <datafield tag="852" ind1=" " ind2=" ">
                    <subfield code="b">S</subfield>
                    <subfield code="z">Digitaliserat exemplar</subfield>
                    <subfield code="z">Fritt tillgängligt via Internet</subfield>
                </datafield>
            </record>
	</xsl:template>
</xsl:stylesheet>
