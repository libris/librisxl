<?xml version="1.0" encoding="UTF-8"?>

<!--
    Document   : name_title_formatter.xsl
    Created on : October 12, 2009, 7:32 AM
    Author     : pelle
    Description:
        Purpose of transformation follows.
-->

<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:marc="http://www.loc.gov/MARC21/slim"
                exclude-result-prefixes="marc">
    <xsl:output method="xml" omit-xml-declaration="no" indent="yes" encoding="utf-8"/>

    <xsl:template match="marc:collection">
        <collection>
            <xsl:apply-templates select="marc:record"/>
        </collection>
    </xsl:template>

    <xsl:template match="marc:record">
        <record xmlns="http://www.loc.gov/MARC21/slim" type="Bibliographic">
            <xsl:apply-templates select="marc:leader"/>
            <xsl:apply-templates select="marc:controlfield"/>
            <xsl:apply-templates select="marc:datafield[@tag &lt;500 and not(@tag ='022' and marc:subfield[@code='a'])]"/>
            <xsl:apply-templates select="marc:datafield[@tag='022' and marc:subfield[@code='a']]"/>
            <xsl:apply-templates select="marc:datafield[@tag >=500 and @tag&lt;599]"/>
            <xsl:call-template name="create599"/>
            <xsl:apply-templates select="marc:datafield[@tag >=599 and @tag&lt;'776']"/>
            <xsl:if test="not(marc:datafield[@tag='776'])">
                <xsl:apply-templates select="marc:datafield[@tag='245']" mode="create776"/>
                <!--<xsl:apply-templates select="marc:datafield[@tag='245' and marc:subfield[@code='a'] and ../marc:datafield[@tag='260' and marc:subfield[@code='c']] and ../marc:datafield[@tag='020' and marc:subfield[@code='a']] and not(../marc:datafield[@tag='776'])]" mode="create776"/>-->
            </xsl:if>
            <xsl:apply-templates select="marc:datafield[@tag >= 776 and not(@tag='856')]"/>
        </record>
    </xsl:template>

    <!-- Leader -->
    <xsl:template match="marc:leader">
        <leader><xsl:value-of select="substring(., 1, 6)"/>a<xsl:value-of select="substring(., 8, 10)"/>7<xsl:value-of select="substring(., 19)"/></leader>
    </xsl:template>

    <!-- Controlfield 007 -->
    <xsl:template match="marc:controlfield[@tag='007' and starts-with(., 'c') and not(substring(., 1, 2) = 'r')]">
        <controlfield tag="007">cr||||||||||||</controlfield>
    </xsl:template>

    <!-- Controlfield 008 -->
    <xsl:template match="marc:controlfield[@tag='008']">
        <controlfield tag="008"><xsl:value-of select="substring(., 1, 6)"/>s<xsl:value-of select="substring(., 8, 4)"/><xsl:text>    </xsl:text><xsl:value-of select="substring(., 16,17)"/><xsl:text> </xsl:text><xsl:value-of select="substring(., 34,5)"/><xsl:text> </xsl:text><xsl:value-of select="substring(., 40)"/></controlfield>
    </xsl:template>

    <!-- Datafield 020 -->
    <xsl:template match="marc:datafield[@tag='020' and marc:subfield[@code='a']]">
        <datafield ind1="{@ind1}" ind2="{@ind2}" tag="020">
            <subfield code="z"><xsl:value-of select="normalize-space(translate(marc:subfield[@code='a'], ':;', ' '))"/><xsl:text> (Print)</xsl:text></subfield>
        </datafield>
    </xsl:template>

    <!-- Datafield 022 -->
    <xsl:template match="marc:datafield[@tag='022' and marc:subfield[@code='a']]">
        <datafield ind1=" " ind2=" " tag="500">
            <subfield code="a"><xsl:text>ISSN </xsl:text><xsl:value-of select="marc:subfield[@code='a']"/></subfield>
        </datafield>
    </xsl:template>

    <!-- Datafield 245, subfield h -->
    <xsl:template match="marc:datafield[@tag='245']">
        <datafield ind1="{@ind1}" ind2="{@ind2}" tag="245">
            <xsl:apply-templates/>
        </datafield>
    </xsl:template>
    <xsl:template match="marc:subfield[../@tag = '245' and @code='h' and contains(normalize-space(translate(., 'ELCTRONISU', 'elctronisu')), '[electronic resource]')]">
        <xsl:variable name="interp" select="substring(., 1 + string-length(.) - 1)"/>
        <subfield code="h">[Elektronisk resurs]<xsl:if test="$interp = '/' or $interp = ':' or $interp = '=' or $interp = '+' or $interp = ';'"><xsl:text> </xsl:text><xsl:value-of select="$interp"/></xsl:if><xsl:if test="$interp = '.' or $interp = ','"><xsl:value-of select="$interp"/></xsl:if></subfield>
    </xsl:template>

    <xsl:template match="marc:datafield[@tag='245']" mode="create776">
        <datafield ind1="0" ind2="8" tag="776">
            <subfield code="i">Print</subfield>
            <subfield code="t"><xsl:value-of select="marc:subfield[@code='a']"/></subfield>
            <xsl:if test="../marc:datafield[@tag='260' and marc:subfield[@code='c']]">
                <subfield code="d"><xsl:value-of select="../marc:datafield[@tag='260' and marc:subfield[@code='c']][1]/marc:subfield[@code='c'][1]"/></subfield>
            </xsl:if>
            <xsl:if test="../marc:datafield[@tag='020' and marc:subfield[@code='a']]">
                <xsl:variable name="data" select="../marc:datafield[@tag='020' and marc:subfield[@code='a']][1]/marc:subfield[@code='a'][1]"/>
                <subfield code="z"><xsl:value-of select="normalize-space(translate($data, ':;', ' '))"/></subfield>
            </xsl:if>
        </datafield>
    </xsl:template>

    <xsl:template name="create599">
        <datafield ind1=" " ind2=" " tag="599">
            <subfield code="a">Korrekt e-ISBN har tills vidare placerats i 020$z. Åtgärden har genomförts vid maskinell inmatning i LIBRIS och får ej ändras manuellt.</subfield>
        </datafield>
    </xsl:template>
    
    <!-- Copy template -->
    <xsl:template match="@* | node()">
        <xsl:copy>
            <xsl:apply-templates select="@* | node()"/>
        </xsl:copy>
    </xsl:template>
</xsl:stylesheet>
