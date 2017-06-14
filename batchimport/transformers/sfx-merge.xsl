<?xml version="1.0" encoding="UTF-8"?>
<!--
    Document   : sfx-merge.xsl
    Updated on: December 08, 2008
    Updated on: June 28, 2010 (merge logic for 856 links)
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
            <xsl:variable name="new_has856free"><xsl:if test="new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag='856' and marc:subfield[@code='x' and . = '856free']]">true</xsl:if></xsl:variable>
            <xsl:variable name="old_has856free"><xsl:if test="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag='856' and marc:subfield[@code='x' and . = '856free']]">true</xsl:if></xsl:variable>
            <xsl:choose>
                <xsl:when test="$new_has856free = 'true'">
                    <xsl:if test="$old_leader17 = '5'">
                        <xsl:apply-templates select="new_record/marc:record[@type='Bibliographic']" mode="simple_case"/>
                    </xsl:if>
                    <xsl:if test="$old_leader17 != '5'">
                        <xsl:apply-templates select="old_record/marc:record[@type='Bibliographic']" mode="replace_old_856_only"/>
                    </xsl:if>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:if test="$old_leader17 = '5' and $old_has856free = 'true'">
                        <xsl:apply-templates select="new_record/marc:record[@type='Bibliographic']" mode="replace_old_except_856"/>
                    </xsl:if>
                    <xsl:if test="$old_leader17 = '5' and $old_has856free != 'true'">
                        <xsl:apply-templates select="new_record/marc:record[@type='Bibliographic']" mode="simple_case"/>
                    </xsl:if>
                    <xsl:if test="$old_leader17 != '5'">
                        <xsl:apply-templates select="old_record/marc:record[@type='Bibliographic']" mode="simple_case"/>
                    </xsl:if>
                </xsl:otherwise>
            </xsl:choose>
        </collection>
    </xsl:template>

    <xsl:template match="marc:record" mode="simple_case">
        <record xmlns="http://www.loc.gov/MARC21/slim" type="Bibliographic">
            <xsl:apply-templates/>
        </record>
    </xsl:template>

    <xsl:template match="marc:record" mode="replace_old_856_only">
        <record xmlns="http://www.loc.gov/MARC21/slim" type="Bibliographic">
            <xsl:apply-templates select="marc:leader"/>
            <xsl:apply-templates select="marc:controlfield"/>
            <xsl:apply-templates select="marc:datafield[@tag &lt; 856 or @tag = '856' and not(marc:subfield[@code='x' and . = '856free'])]"/>
            <xsl:apply-templates select="../../new_record/marc:record/marc:datafield[@tag = '856' and marc:subfield[@code='x' and . = '856free']]"/>
            <xsl:apply-templates select="marc:datafield[@tag > 856]"/>
        </record>
    </xsl:template>

    <xsl:template match="marc:record" mode="replace_old_except_856">
        <record xmlns="http://www.loc.gov/MARC21/slim" type="Bibliographic">
            <xsl:apply-templates select="marc:leader"/>
            <xsl:apply-templates select="marc:controlfield"/>
            <xsl:apply-templates select="marc:datafield[@tag &lt;= 856]"/>
            <xsl:apply-templates select="../../old_record/marc:record/marc:datafield[@tag = '856' and marc:subfield[@code='x' and . = '856free']]"/>
            <xsl:apply-templates select="marc:datafield[@tag > 856]"/>
        </record>
    </xsl:template>

    <!-- Copy template -->
    <!--
    <xsl:apply-templates select="@* | node()[normalize-space(.) != '']"/>
    -->
    <xsl:template match="@* | node()">
        <xsl:copy>
            <xsl:apply-templates select="@* | node()"/>
        </xsl:copy>
    </xsl:template>

</xsl:stylesheet>

