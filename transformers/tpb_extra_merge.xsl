<?xml version="1.0" encoding="UTF-8"?>

<!--
    Document   : tpb_merge.xsl
    Created on : April 3, 2012, 7:10 AM
    Author     : pelle
    Description:
        Purpose of transformation follows.
-->

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:marc="http://www.loc.gov/MARC21/slim" xmlns:java="http://xml.apache.org/xslt/java" version="1.0" exclude-result-prefixes="marc java">
    <xsl:output method="xml" indent="yes"/>
    <xsl:key name="subject653" match="merge/old_record/marc:record/marc:datafield[@tag='653' and marc:subfield[@code='a']]" use="normalize-space(marc:subfield[@code='a'])"/>
    <xsl:key name="subject976" match="merge/old_record/marc:record/marc:datafield[@tag='976' and marc:subfield[@code='b']]" use="normalize-space(marc:subfield[@code='b'])"/>
    <xsl:template match="/">
        <xsl:apply-templates select="merge"/>
    </xsl:template>
    
    <xsl:template match="merge">
        <collection xmlns="http://www.loc.gov/MARC21/slim">
            <xsl:apply-templates select="old_record"/>
        </collection>
    </xsl:template>
    
    <!-- record -->
    <xsl:template match="old_record">
        <xsl:apply-templates select="marc:record"/>
     </xsl:template>
    
    <xsl:template match="marc:record">
        <record xmlns="http://www.loc.gov/MARC21/slim" type="Bibliographic">
            <xsl:apply-templates/>
            <xsl:apply-templates select="../../new_record/marc:record/marc:datafield[@tag='697' and marc:subfield[@code = 'c']]"/>
            <xsl:apply-templates select="../../new_record/marc:record/marc:datafield[@tag='976' and marc:subfield[@code = 'b'] and not(key('subject976', normalize-space(marc:subfield[@code='b'])))]"/>
        </record>
    </xsl:template>

    <xsl:template match="marc:datafield[@tag='697' and marc:subfield[@code='c']]">
        <xsl:apply-templates select="marc:subfield[@code='c']" mode="subfields697"/>
    </xsl:template>
    
    <xsl:template match="marc:subfield[@code='c']" mode="subfields697">
        <xsl:variable name="subc"><xsl:value-of select="normalize-space(.)"/><xsl:if test="following-sibling::marc:subfield[1]/@code='e'"><xsl:text> (</xsl:text><xsl:value-of select="following-sibling::marc:subfield[@code='e'][1]"/><xsl:text>)</xsl:text></xsl:if></xsl:variable>
        <xsl:if test="not(key('subject653', $subc))">
        <datafield ind1=" " ind2=" " tag="653" xmlns="http://www.loc.gov/MARC21/slim">
            <subfield code="a"><xsl:value-of select="$subc"/></subfield>
        </datafield>
        </xsl:if>
    </xsl:template>
    
    <!-- Copy template -->
    <xsl:template match="@* | node()">
        <xsl:copy>
            <xsl:apply-templates select="@* | node()"/>
        </xsl:copy>
    </xsl:template>
</xsl:stylesheet>
