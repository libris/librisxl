<?xml version="1.0" encoding="UTF-8"?>
<!--
    Document   : boktjanst-merge.xsl
    Updated on: December 08, 2008
    Updated on: June 28, 2010 (merge logic for 856 links)
    Updated on: October 25, 2010 (kopierad fran sfx till boktjanst)
    Author     : pelle
    Description:
        Purpose of transformation follows.

-->
<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:marc="http://www.loc.gov/MARC21/slim"
                exclude-result-prefixes="marc">
    <xsl:output method="xml" omit-xml-declaration="no" indent="yes"/>

    <xsl:template match="/">
        <xsl:apply-templates select="merge"/>
    </xsl:template>

    <xsl:template match="merge">
        <collection xmlns="http://www.loc.gov/MARC21/slim">
            <xsl:variable name="old_leader"><xsl:value-of select="old_record/marc:record[@type='Bibliographic']/marc:leader"/></xsl:variable>
            <xsl:variable name="old_leader17" select="substring($old_leader,18,1)"/>
            <xsl:variable name="nbsafe"><xsl:if test="not(old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag='042' and marc:subfield[@code='9' and normalize-space(.) = 'NB']])">true</xsl:if></xsl:variable>
            <xsl:variable name="new_leader"><xsl:value-of select="new_record/marc:record[@type='Bibliographic']/marc:leader"/></xsl:variable>
            <xsl:variable name="new_leader17" select="substring($new_leader,18,1)"/>
            <xsl:variable name="new_has856free"><xsl:if test="new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag='856' and marc:subfield[@code='x' and normalize-space(.) = '856free']]">true</xsl:if></xsl:variable>
            <xsl:variable name="new_has856other"><xsl:if test="new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag='856' and marc:subfield[@code='x' and normalize-space(.) = '856other']]">true</xsl:if></xsl:variable>
            <xsl:variable name="case">
                <xsl:choose>
                    <xsl:when test="($new_leader17 = '1' or $new_leader17 = '2' or $new_leader17 = '3' or $new_leader17 = '7' or $new_leader17 = '8' or $new_leader17 = ' ') and ($old_leader17 = '5' or $old_leader17 = '8')">replace_old</xsl:when>
                    <xsl:when test="$new_leader17 = '5' and $old_leader17 = '5'">replace_old</xsl:when>
                    <xsl:otherwise>keep_old</xsl:otherwise>
                </xsl:choose>
            </xsl:variable>
            <xsl:choose>
                <xsl:when test="$case = 'replace_old' and $nbsafe = 'true'">
                    <xsl:apply-templates select="new_record/marc:record[@type='Bibliographic']">
                        <xsl:with-param name="new_leader05" select="substring($new_leader,6,1)"/>
                        <xsl:with-param name="new_has856free" select="$new_has856free"/>
                        <xsl:with-param name="new_has856other" select="$new_has856other"/>
                    </xsl:apply-templates>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:apply-templates select="old_record/marc:record[@type='Bibliographic']">
                        <xsl:with-param name="new_leader05" select="substring($new_leader,6,1)"/>
                        <xsl:with-param name="new_has856free" select="$new_has856free"/>
                        <xsl:with-param name="new_has856other" select="$new_has856other"/>
                    </xsl:apply-templates>
                </xsl:otherwise>
            </xsl:choose>

        </collection>
    </xsl:template>
    <xsl:template match="new_record/marc:record">
        <xsl:param name="new_leader05"/>
        <xsl:param name="new_has856free"/>
        <xsl:param name="new_has856other"/>

        <record xmlns="http://www.loc.gov/MARC21/slim" type="Bibliographic">
            <xsl:apply-templates select="marc:leader" mode="check_leader05"/>
            <xsl:apply-templates select="marc:controlfield"/>
            <xsl:apply-templates select="marc:datafield[@tag &lt; 856 or @tag = '856' and not(marc:subfield[@code='x' and (. = '856free' or . = '856other')])]"/>
            <xsl:if test="$new_leader05 != 'd'">
                <xsl:choose>
                    <xsl:when test="$new_has856free = 'true'">
                        <xsl:apply-templates select="marc:datafield[@tag = '856' and marc:subfield[@code='x' and . = '856free']]"/>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:apply-templates select="../../old_record/marc:record/marc:datafield[@tag = '856' and marc:subfield[@code='x' and . = '856free']]"/>
                    </xsl:otherwise>
                </xsl:choose>
                <xsl:choose>
                    <xsl:when test="$new_has856other = 'true'">
                        <xsl:apply-templates select="marc:datafield[@tag = '856' and marc:subfield[@code='x' and . = '856other']]"/>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:apply-templates select="../../old_record/marc:record/marc:datafield[@tag = '856' and marc:subfield[@code='x' and . = '856other']]"/>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:if>
            <xsl:apply-templates select="marc:datafield[@tag > 856]"/>
        </record>
    </xsl:template>

    <xsl:template match="old_record/marc:record">
        <xsl:param name="new_leader05"/>
        <xsl:param name="new_has856free"/>
        <xsl:param name="new_has856other"/>

        <record xmlns="http://www.loc.gov/MARC21/slim" type="Bibliographic">
            <xsl:apply-templates select="marc:leader"/>
            <xsl:apply-templates select="marc:controlfield"/>
            <xsl:apply-templates select="marc:datafield[@tag &lt; 856 or @tag = '856' and not(marc:subfield[@code='x' and (. = '856free' or . = '856other')])]"/>
            <xsl:if test="$new_leader05 != 'd'">
                <xsl:choose>
                    <xsl:when test="$new_has856free = 'true'">
                        <xsl:apply-templates select="../../new_record/marc:record/marc:datafield[@tag = '856' and marc:subfield[@code='x' and . = '856free']]"/>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:apply-templates select="marc:datafield[@tag = '856' and marc:subfield[@code='x' and . = '856free']]"/>
                    </xsl:otherwise>
                </xsl:choose>
                <xsl:choose>
                    <xsl:when test="$new_has856other = 'true'">
                        <xsl:apply-templates select="../../new_record/marc:record/marc:datafield[@tag = '856' and marc:subfield[@code='x' and . = '856other']]"/>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:apply-templates select="marc:datafield[@tag = '856' and marc:subfield[@code='x' and . = '856other']]"/>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:if>
            <xsl:apply-templates select="marc:datafield[@tag > 856]"/>
        </record>
    </xsl:template>

    <xsl:template match="marc:leader" mode="check_leader05">
        <xsl:variable name="before_leader05" select="substring(.,1,5)"/>
        <xsl:variable name="after_leader05" select="substring(., 7)"/>
        <xsl:variable name="leader05" select="substring(.,6,1)"/>
        <xsl:variable name="replace05"><xsl:choose><xsl:when test="$leader05 = 'd'">c</xsl:when><xsl:otherwise><xsl:value-of select="$leader05"/></xsl:otherwise></xsl:choose></xsl:variable>
        <leader><xsl:value-of select="$before_leader05"/><xsl:value-of select="$replace05"/><xsl:value-of select="$after_leader05"/></leader>
    </xsl:template>
    
    <!-- Copy template -->
    <xsl:template match="@* | node()">
        <xsl:copy>
            <xsl:apply-templates select="@* | node()"/>
        </xsl:copy>
    </xsl:template>

</xsl:stylesheet>

