<?xml version="1.0" encoding="UTF-8"?>

<!--
    Document   : springer.xsl
    Created on : May 17, 2010, 15:42 PM
    Author     : pelle
    Description:
        Purpose of transformation follows.
-->

<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:marc="http://www.loc.gov/MARC21/slim"
                xmlns:java="http://xml.apache.org/xslt/java"
                exclude-result-prefixes="marc java">
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
            <xsl:apply-templates select="marc:datafield[(not(contains(@tag, '9')) or @tag = '490') and @tag != '773' and @tag != '856']"/>
        </record>
        <record xmlns="http://www.loc.gov/MARC21/slim" type="Holdings">
            <leader><xsl:text>#####nx  a22#####1n 4500</xsl:text></leader>
            <xsl:variable name="rawTimeStamp"><xsl:value-of select="java:format(java:java.text.SimpleDateFormat.new('yy-MM-dd'), java:java.util.Date.new())"/></xsl:variable>
            <xsl:variable name="timeStamp" select="translate($rawTimeStamp, '-', '')"/>
            <controlfield tag="008"><xsl:value-of select="$timeStamp"/>||0000|||||001||||||000000</controlfield>
            <datafield ind1=" " ind2=" " tag="852">
                <subfield code="b">Sjbs</subfield>
                <subfield code="h">Springer eBook Medicine</subfield>
            </datafield>
            <xsl:apply-templates select="marc:datafield[@tag='856']"/>
        </record>
    </xsl:template>

    <!-- Leader -->
    <xsl:template match="marc:leader">
        <leader><xsl:value-of select="substring(., 1, 6)"/>a<xsl:value-of select="substring(., 8, 10)"/>3<xsl:value-of select="substring(., 19)"/></leader>
    </xsl:template>

    <!-- Controlfield 008 -->
    <xsl:template match="marc:controlfield[@tag='008']">
        <controlfield tag="008"><xsl:value-of select="substring(., 1, 18)"/><xsl:text>|||||o|||||||| 0|</xsl:text><xsl:value-of select="substring(., 36, 3)"/><xsl:text> d</xsl:text></controlfield>
    </xsl:template>

    <!-- Controlfield 001 -->
    <xsl:template match="marc:controlfield[@tag='001']">
        <datafield ind1=" " ind2=" " tag="020">
            <subfield code="z"><xsl:value-of select="."/><xsl:text> (Print)</xsl:text></subfield>
        </datafield>
    </xsl:template>

    <!-- Datafield 020 -->
    <xsl:template match="marc:datafield[@tag='020']">
        <datafield ind1="{@ind1}" ind2="{@ind2}" tag="020">
            <xsl:for-each select="marc:subfield[@code = 'a']">
                <subfield code="a"><xsl:value-of select="."/><xsl:text> (eBook)</xsl:text></subfield>
            </xsl:for-each>
            <xsl:for-each select="marc:subfield[@code != 'a']">
                <subfield code="{@code}"><xsl:value-of select="."/></subfield>
            </xsl:for-each>
        </datafield>
    </xsl:template>

<!-- Datafield 100 -->
    <xsl:template match="marc:datafield[@tag='100']">
        <xsl:variable name="tag">
            <xsl:variable name="df245c" select="../marc:datafield[@tag='245']/marc:subfield[@code='c']"/>
            <xsl:choose>
                <xsl:when test="starts-with($df245c, 'edited')">700</xsl:when>
                <xsl:otherwise>100</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{$tag}">
            <xsl:for-each select="marc:subfield">
                <subfield code="{@code}"><xsl:value-of select="."/></subfield>
                <xsl:if test="$tag = '700'">
                    <subfield code="4">edt</subfield>
                </xsl:if>
            </xsl:for-each>
        </datafield>
    </xsl:template>


    <!-- Datafield 245, subfield h -->
    <xsl:template match="marc:datafield[@tag='245']">
        <datafield ind1="{@ind1}" ind2="{@ind2}" tag="245">
            <xsl:apply-templates/>
        </datafield>
        <datafield ind1="0" ind2="8" tag="776">
            <subfield code="i">Print</subfield>
            <subfield code="t"><xsl:value-of select="marc:subfield[@code='a']"/></subfield>
            <xsl:if test="../marc:datafield[@tag='260' and marc:subfield[@code='c']]">
                <subfield code="d"><xsl:value-of select="../marc:datafield[@tag='260' and marc:subfield[@code='c']][1]/marc:subfield[@code='c'][1]"/></subfield>
            </xsl:if>
            <xsl:if test="../marc:controlfield[@tag='001']">
                <subfield code="z"><xsl:value-of select="../marc:controlfield[@tag='001']"/></subfield>
            </xsl:if>
        </datafield>
    </xsl:template>
    <xsl:template match="marc:subfield[../@tag = '245' and @code='h' and contains(normalize-space(translate(., 'ELCTRONISU', 'elctronisu')), '[electronic resource]')]">
        <xsl:variable name="interp" select="substring(., 1 + string-length(.) - 1)"/>
        <subfield code="h">[Elektronisk resurs]<xsl:if test="$interp = '/' or $interp = ':' or $interp = '=' or $interp = '+' or $interp = ';'"><xsl:text> </xsl:text><xsl:value-of select="$interp"/></xsl:if><xsl:if test="$interp = '.' or $interp = ','"><xsl:value-of select="$interp"/></xsl:if></subfield>
    </xsl:template>

    <!-- Datafield 700 -->
    <xsl:template match="marc:datafield[@tag='700']">
        <datafield ind1="{@ind1}" ind2="{@ind2}" tag="700">
            <xsl:apply-templates select="marc:subfield"/>
            <xsl:variable name="df245c" select="../marc:datafield[@tag='245']/marc:subfield[@code='c']"/>
                <xsl:if test="starts-with($df245c, 'edited')">
                    <subfield code="4">edt</subfield>
                </xsl:if>
        </datafield>
    </xsl:template>

    <!-- Datafield 760, 772 -->
    <xsl:template match="marc:datafield[@tag='760' or @tag='767' or @tag='772' or @tag='774' or @tag='776' or @tag='787']">
        <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
            <xsl:apply-templates select="marc:subfield[@code != 'w']"/>
        </datafield>
    </xsl:template>

    <!-- Datafield 856 -->
    <xsl:template match="marc:datafield[@tag='856']">
        <datafield ind1="{@ind1}" ind2="{@ind2}" tag="856">
            <xsl:apply-templates select="marc:subfield"/>
            <subfield code="z">Tillgänglig för användare vid svenska sjukhusbibliotek</subfield>
        </datafield>
    </xsl:template>

    <!-- Copy template -->
    <xsl:template match="@* | node()">
        <xsl:copy>
            <xsl:apply-templates select="@* | node()"/>
        </xsl:copy>
    </xsl:template>
</xsl:stylesheet>
