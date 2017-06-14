<?xml version="1.0" encoding="UTF-8"?>

<!--
    Document   : ebsco.xsl
    Created on : May 27, 2010, 3:22 PM
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
            <xsl:apply-templates select="marc:datafield[(not(contains(@tag, '9')) or @tag = '490') and @tag != '035' and @tag != '773' and @tag != '776' and @tag != '852' and @tag != '856']"/>
            <xsl:apply-templates select="marc:datafield[@tag='035' and marc:subfield[@code='a' and starts-with(., '(EBZ)')]]"/>
            <xsl:if test="marc:datafield[@tag='022' and marc:subfield[@code='a']] or (marc:datafield[@tag='022' and not(marc:subfield[@code='a']) and count(marc:subfield[@code='y']) > 1] and marc:datafield[@tag='776' and marc:subfield[@code='t' and contains(., '(Online)')]])">
            <!--<xsl:if test="marc:datafield[@tag='022' and (marc:subfield[@code='a'] or (count(marc:subfield[@code='y']) = 2)) and ../marc:datafield[@tag='776' and marc:subfield[@code='t' and contains(., '(Online)')]]]">-->
                <xsl:apply-templates select="marc:datafield[@tag='245']" mode="create222"/>
            </xsl:if>
            <!--<xsl:if test="marc:datafield[@tag='022' and (marc:subfield[@code='a'] or (count(marc:subfield[@code='y']) = 2)) and ../marc:datafield[@tag='776' and marc:subfield[@code='t' and contains(., '(Online)')]]]"><xsl:apply-templates select="marc:datafield[@tag='245']" mode="create222"/></xsl:if>-->
            <xsl:apply-templates select="marc:datafield[@tag='042' and marc:subfield[@code='a' and normalize-space(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'noncon-m']]" mode="create599"/>
        </record>
        <record xmlns="http://www.loc.gov/MARC21/slim" type="Holdings">
            <leader>#####<xsl:value-of select="substring(marc:leader, 6, 1)"/><xsl:text>y  a22#####3n 4500</xsl:text></leader>
            <xsl:variable name="rawTimeStamp"><xsl:value-of select="java:format(java:java.text.SimpleDateFormat.new('yy-MM-dd'), java:java.util.Date.new())"/></xsl:variable>
            <xsl:variable name="timeStamp" select="translate($rawTimeStamp, '-', '')"/>
            <controlfield tag="008"><xsl:value-of select="$timeStamp"/>||0000|||||001||||||000000</controlfield>
            <xsl:apply-templates select="marc:datafield[@tag='035' and marc:subfield[@code='a' and starts-with(., '(EBZ)')]]"/>
            <!--<xsl:apply-templates select="marc:datafield[@tag='035' and marc:subfield[@code='a' and starts-with(., '(EBZ)')]]" mode="holdings"/>-->
            <xsl:apply-templates select="marc:datafield[@tag='852']"/>
            <xsl:apply-templates select="marc:datafield[@tag='856']"/>
            <xsl:apply-templates select="marc:datafield[@tag='773' and (marc:subfield[@code = 'x'] or marc:subfield[@code = 't'])]" mode="convert773"/>
        </record>
    </xsl:template>

    <!-- Leader -->
    <xsl:template match="marc:leader">
        <xsl:variable name="leader17"><xsl:choose><xsl:when test="../marc:datafield[@tag='042' and marc:subfield[@code='a' and normalize-space(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'noncon-m']]">5</xsl:when><xsl:otherwise>7</xsl:otherwise></xsl:choose></xsl:variable>
        <leader>#####<xsl:value-of select="substring(., 6, 4)"/>a<xsl:value-of select="substring(., 11, 2)"/>#####<xsl:value-of select="$leader17"/><xsl:value-of select="substring(., 19)"/></leader>
    </xsl:template>

    <!-- Controlfield 008 -->
    <xsl:template match="marc:controlfield[@tag='008']">
        <xsl:variable name="cf008_23">
            <xsl:choose>
                <xsl:when test="substring(., 24, 1) = 'o' or substring(., 24, 1) = 's'"><xsl:value-of select="substring(., 24, 1)"/></xsl:when>
                <xsl:otherwise>o</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <controlfield tag="008"><xsl:value-of select="substring(., 1, 22)"/><xsl:text> </xsl:text><xsl:value-of select="$cf008_23"/><xsl:value-of select="substring(., 25, 14)"/><xsl:text> </xsl:text><xsl:value-of select="substring(., 40, 1)"/></controlfield>
    </xsl:template>

    <!-- Datafield 022 -->
    <xsl:template match="marc:datafield[@tag='022' and marc:subfield[@code = 'a'] and marc:subfield[@code = 'y']]">
        <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
            <subfield code="a"><xsl:value-of select="marc:subfield[@code = 'a']"/></subfield>
            <subfield code="z"><xsl:value-of select="marc:subfield[@code = 'y'][1]"/></subfield>
        </datafield>
    </xsl:template>
    <xsl:template match="marc:datafield[@tag='022' and not(marc:subfield[@code = 'a']) and count(marc:subfield[@code = 'y']) > 1]">
        <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
            <xsl:choose>
                <!-- Om 776t har (Online): -->
                <xsl:when test="../marc:datafield[@tag='776' and marc:subfield[@code='t' and contains(., '(Online)')]]">
                    <subfield code="a"><xsl:value-of select="marc:subfield[@code = 'y'][1]"/></subfield>
                    <subfield code="z"><xsl:value-of select="marc:subfield[@code = 'y'][2]"/></subfield>
                </xsl:when>
                <!-- Om 776t inte har (Online): -->
                <xsl:otherwise>
                    <!-- Första y till z, andra y tas bort -->
                    <subfield code="z"><xsl:value-of select="marc:subfield[@code = 'y'][1]"/></subfield>
                </xsl:otherwise>
            </xsl:choose>
        </datafield>
    </xsl:template>
    <xsl:template match="marc:datafield[@tag='022' and not(marc:subfield[@code = 'a']) and count(marc:subfield[@code = 'y']) = 1]">
        <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
            <subfield code="z"><xsl:value-of select="marc:subfield[@code = 'y']"/></subfield>
        </datafield>
    </xsl:template>

    <!-- Datafield 035 -->
    <!--<xsl:template match="marc:datafield[@tag='035' and marc:subfield[@code='a' and starts-with(., '(EBZ)')]]">
        <xsl:apply-templates select="."/>-->
        <!--<datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
            <xsl:apply-templates select="marc:subfield[@code='a' and starts-with(., '(EBZ)')]"/>
        </datafield>-->
    <!--</xsl:template>-->
    <!-- Datafield 035 for holdings-->
    <!--<xsl:template match="marc:datafield[@tag='035']" mode="holdings">
        <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
            <xsl:apply-templates select="marc:subfield[@code='a' and starts-with(., '(EBZ)')]"/>
        </datafield>
    </xsl:template>-->
    
    <!-- Datafield 222/245 -->
    <xsl:template match="marc:datafield[@tag='245']" mode="create222">
        <datafield ind1=" " ind2="0" tag="222">
            <subfield code="a"><xsl:value-of select="marc:subfield[@code='a']"/></subfield>
            <subfield code="b">(Online)</subfield>
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
        <subfield code="h">[Elektronisk resurs]<xsl:if test="$interp = '/' or $interp = ':' or $interp = ';'"><xsl:text> </xsl:text><xsl:value-of select="$interp"/></xsl:if><xsl:if test="$interp = '.' or $interp = ','"><xsl:value-of select="$interp"/></xsl:if></subfield>
    </xsl:template>

    <!-- Datafield 599 special -->
    <xsl:template match="marc:datafield[@tag='042']" mode="create599">
        <datafield ind1=" " ind2=" " tag="599">
            <subfield code="a">EBZ</subfield>
        </datafield>
        <datafield ind1=" " ind2=" " tag="599">
            <subfield code="a">Maskinellt genererad post från EBZ. Eventuella manuellt gjorda ändringar kommer att försvinna vid nästa uppdatering.</subfield>
        </datafield>
    </xsl:template>

    <!-- Datafield 760 m.fl. -->
    <xsl:template match="marc:datafield[@tag >= 760 and @tag &lt;= 787]">
        <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
            <xsl:apply-templates select="marc:subfield[@code!='w']"/>
        </datafield>
    </xsl:template>

    <!-- Datafield 773 -->
    <xsl:template match="marc:datafield[@tag = 773]" mode="convert773">
        <datafield ind1=" " ind2=" " tag="866">
            <xsl:if test="marc:subfield[@code = 'x']">
                <subfield code="a"><xsl:value-of select="marc:subfield[@code = 'x'][1]"/></subfield>
            </xsl:if>
            <xsl:if test="marc:subfield[@code = 't']">
                <subfield code="z"><xsl:value-of select="marc:subfield[@code = 't'][1]"/></subfield>
            </xsl:if>
        </datafield>
    </xsl:template>
    <!-- Datafield 776 -->
    <!--
    <xsl:template match="marc:datafield[@tag='776']">
        <datafield ind1="0" ind2=" " tag="{@tag}">
            <xsl:for-each select="marc:subfield[@code!='w' ]">
                <xsl:choose>
                    <xsl:when test="@code = 't'">
                        <subfield code="t"><xsl:value-of select="."/><xsl:text> (Print)</xsl:text></subfield>
                    </xsl:when>
                    <xsl:otherwise>
                        <subfield code="{@code}"><xsl:value-of select="normalize-space(.)"/></subfield>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:for-each>
        </datafield>
    </xsl:template>

    <xsl:template match="marc:datafield[@tag='776' and contains(marc:subfield[@code='t'], '(Online)')]">
        <datafield ind1="0" ind2=" " tag="{@tag}">
            <xsl:for-each select="marc:subfield[@code!='w' ]">
                <xsl:choose>
                    <xsl:when test="@code = 't' and contains(., '(Online)') and ../../marc:datafield[@tag='022' and marc:subfield[@code='a']]">
                        <xsl:variable name="tmp1" select="substring-before(., '(Online)')"/>
                        <xsl:variable name="tmp2" select="concat($tmp1, '(Print)')"/>
                        <subfield code="t"><xsl:value-of select="$tmp2"/></subfield>
                    </xsl:when>
                    <xsl:when test="@code = 't' and contains(., '(Online)') and ../../marc:datafield[@tag='022']">
                        <subfield code="t"><xsl:value-of select="normalize-space(substring-before(., '(Online)'))"/></subfield>
                    </xsl:when>
                    <xsl:when test="@code = 'x' and ../../marc:datafield[@tag='022' and marc:subfield[@code='y']]">
                        <subfield code="t"><xsl:value-of select="."/><xsl:text> (Print)</xsl:text></subfield>
                    </xsl:when>
                    <xsl:otherwise>
                        <subfield code="{@code}"><xsl:value-of select="normalize-space(.)"/></subfield>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:for-each>
        </datafield>
    </xsl:template>
    -->
    <!-- Datafield 856 -->
    <xsl:template match="marc:datafield[@tag='856']">
        <xsl:variable name="pos" select="position()"/>
        <datafield ind1="{@ind1}" ind2="{@ind2}" tag="856">
            <xsl:apply-templates select="marc:subfield[@code!='3' and @code!='z']"/>
            <xsl:apply-templates select="marc:subfield[@code='z']"/>
            <xsl:if test="position() = 1">
                <xsl:apply-templates select="../marc:datafield[@tag='773']" mode="getsubfieldt"/>
            </xsl:if>
            <!--<xsl:if test="../marc:datafield[@tag='773'][$pos]"><tjosan>sdfds</tjosan></xsl:if>-->
        </datafield>
    </xsl:template>
    <xsl:template match="marc:datafield[@tag='773']" mode="getsubfieldt">
        <subfield code="z"><xsl:value-of select="marc:subfield[@code='t']"/></subfield>
    </xsl:template>

    <!-- Copy template -->
    <xsl:template match="@* | node()">
        <xsl:copy>
            <xsl:apply-templates select="@* | node()"/>
        </xsl:copy>
    </xsl:template>
</xsl:stylesheet>
