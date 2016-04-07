<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:marc="http://www.loc.gov/MARC21/slim" xmlns:java="http://xml.apache.org/xslt/java" version="1.0" exclude-result-prefixes="marc java">
    <xsl:output method="xml" indent="yes"/>
    <xsl:template match="root">
        <collection xmlns="http://www.loc.gov/MARC21/slim">
            <xsl:apply-templates select="marc:record[marc:datafield[@tag='976'] or marc:datafield[@tag='697' and marc:subfield[@code='c']] and (marc:datafield[@tag='024' and @ind1='7' and marc:subfield[@code='a'] and marc:subfield[@code='2' and normalize-space(.) = 'TPB']] or marc:controlfield[@tag='001'])]"/>
        </collection>
        <!--
        <root>
            <xsl:apply-templates select="marc:record[marc:datafield[@tag='976'] and marc:datafield[@tag='697' and marc:subfield[@code='c'] and marc:subfield[@code='e']]]"/>
        </root>
        -->
    </xsl:template>
    
    <xsl:template match="marc:record">
        <record xmlns="http://www.loc.gov/MARC21/slim" type="Bibliographic">
            <!--<xsl:apply-templates/>-->
            <xsl:apply-templates select="marc:leader | marc:controlfield | marc:datafield[@tag='976'] | marc:datafield[@tag='697' and marc:subfield[@code='c']] | marc:datafield[@tag='024'] | marc:controlfield[@tag='001']"/>
        </record>
    </xsl:template> 
    
    <xsl:template match="marc:datafield[@tag='024' and @ind1='7' and marc:subfield[@code='a'] and marc:subfield[@code='2' and normalize-space(.) = 'TPB']]">
        <datafield xmlns="http://www.loc.gov/MARC21/slim" ind1=" " ind2=" " tag="035">
            <subfield code='a'>(TPB)<xsl:value-of select="marc:subfield[@code='a']"/></subfield>
        </datafield>
        <xsl:copy-of select="."/>   
    </xsl:template>
    
    <xsl:template match="marc:controlfield[@tag='001']">
        <datafield ind1=" " ind2=" " tag="035" xmlns="http://www.loc.gov/MARC21/slim">
            <subfield code="a"><xsl:if test="../marc:controlfield[@tag='003']">(<xsl:value-of select="../marc:controlfield[@tag='003']"/>)</xsl:if><xsl:value-of select="."/></subfield>
        </datafield>
        <xsl:copy-of select="."/>
    </xsl:template>
    
    <!-- Copy template -->
    <xsl:template match="@* | node()">
        <xsl:copy>
            <xsl:apply-templates select="@* | node()"/>
        </xsl:copy>
    </xsl:template>
    
</xsl:stylesheet>