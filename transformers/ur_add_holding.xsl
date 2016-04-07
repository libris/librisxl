<?xml version="1.0" encoding="UTF-8" ?>

<!--
    Document   : ur.xsl
    Created on : August 27, 2007, 4:37 PM
    Updated on: December 08, 2008
    Updated on: December 04, 2009 (delete flag check)
    Updated on: April 26, 2009 (holdings for Chalmers) etc.
    Author     : pelle
    Description:
        Sigel fix

-->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0"
                xmlns:ims="http://www.imsglobal.org/xsd/imscp_v1p1"
                xmlns:ims2="http://www.imsglobal.org/xsd/imsmd_v1p2"
                xmlns:ims3="http://www.ur.se/infocube/ims_ext"
                xmlns:java="http://xml.apache.org/xslt/java"
                exclude-result-prefixes="ims ims2 ims3 java">
    <xsl:output method="xml" omit-xml-declaration="no" indent="yes" encoding="utf-8"/>
    <xsl:param name="f008_00-05">050420</xsl:param>
    <xsl:template match="/ims:manifest[@identifier='root_manifest']">
        <collection xmlns="http://www.loc.gov/MARC21/slim">
            <xsl:apply-templates select="ims:manifest[@identifier !='root_manifest']/ims:metadata/ims2:lom"/>
        </collection>
    </xsl:template>

    <xsl:template match="ims:manifest[@identifier != 'root_manifest']/ims:metadata/ims2:lom">

        <!-- Används i cf001 -->
        <xsl:variable name="id" select="normalize-space(substring-after(../../@identifier, '_'))"/>

        <!-- Används som villkor för att posten ska läggas in, när leader skapas, samt i cf008, df245 och df300-->
        <xsl:variable name="case-format" select="normalize-space(translate(ims2:technical/ims2:format[1], 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>

        <!-- Om det finns ett distributionevent med platform = "internet", type = "download" och receivingagentgroup = "avc" -->
        <xsl:variable name="cond-deal">
            <xsl:if test="ims2:general/ims3:distributionevents/ims3:distributionevent[normalize-space(translate(ims3:platform, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'internet'
            and normalize-space(translate(ims3:type, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'download'
            and normalize-space(translate(ims3:receivingagentgroup, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'avc']">true</xsl:if>
        </xsl:variable>

        <!-- Används för att skapa 245-fältet samt som villkor för att posten ska läggas in -->
        <!-- 245 och 246 - Title  -->
        <xsl:variable name="title">
            <xsl:choose>
                <xsl:when test="normalize-space(ims2:general/ims3:maintitle[1]/ims2:langstring)">
                    <xsl:value-of select="normalize-space(ims2:general/ims3:maintitle[1]/ims2:langstring)"/>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:value-of select="normalize-space(ims2:general/ims2:title/ims2:langstring)"/>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:variable>

        <!-- Används som villkor för att posten ska läggas in -->
        <xsl:variable name="case-status" select="normalize-space(translate(ims2:lifecycle/ims2:status/ims2:value/ims2:langstring, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>

        <!-- Används som villkor för att posten ska läggas in -->
        <xsl:variable name="case-learningresourcetype">
            <xsl:if test="count(ims2:educational/ims2:learningresourcetype/ims2:value/ims2:langstring[normalize-space(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'program']) > 0">program</xsl:if>
        </xsl:variable>

        <!-- Villkor för transformationen  -->
        <xsl:if test="$title != '' and $cond-deal != '' and $case-status = 'final' and $case-learningresourcetype = 'program' and ($case-format = 'video' or $case-format = 'audio')">
            <xsl:variable name="interval">
                <xsl:apply-templates select="ims2:general/ims3:distributionevents/ims3:distributionevent[normalize-space(translate(ims3:platform, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'internet'
            and normalize-space(translate(ims3:type, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'download'
            and normalize-space(translate(ims3:receivingagentgroup, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'avc'][1]" mode="getDates"/>
            </xsl:variable>
            <record type="Bibliographic">
                <!-- leader  -->
                <leader>#####nim a22#####3a 4500</leader>

                <!-- 001 - Identifier attribute (bibid)  -->
                <controlfield tag="001"><xsl:value-of select="$id"/></controlfield>
               
                <!-- 008 - Date, language m.m.  -->
                <controlfield tag="008">100414s2005 sw ||| cs|||||| swe c</controlfield>

                <!-- 035 - Identifier -->
                <xsl:if test="normalize-space(ims2:general/ims2:identifier)">
                    <datafield tag="035" ind1="" ind2="">
                        <subfield code="a">UR<xsl:value-of select="$id"/></subfield>
                    </datafield>
                </xsl:if>
            </record>

            <!-- Chalmers -->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'Z'"/><xsl:with-param name="proxy" select="'http://proxy.lib.chalmers.se/login?url=http://uraccess.navicast.net/mov.php?xid='"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Online access for Chalmers'"/></xsl:call-template>
        </xsl:if>
    </xsl:template>

    <!-- Skapa Holding-post  -->
    <xsl:template name="create-holding">
        <xsl:param name="interval"/>
        <xsl:param name="sigel"/>
        <xsl:param name="proxy"/>
        <xsl:param name="id"/>
        <xsl:param name="comment"/>

        <xsl:variable name="leader5">
            <xsl:choose>
                <xsl:when test="../../ims:libris_leader[. = 'd' or . = 'n' or . = 'c']"><xsl:value-of select="../../ims:libris_leader"/></xsl:when>
                <xsl:otherwise>n</xsl:otherwise>
             </xsl:choose>
        </xsl:variable>
        <record type="Holdings">
            <!-- leader -->
            <leader>#####<xsl:value-of select="$leader5"/>x  a22#####1n 4500</leader>

            <!-- 008 -->
            <controlfield tag="008">080101||0000|||||001||||||000000</controlfield>

            <!-- 852 -->
            <datafield ind1=" " ind2=" " tag="852">
                <subfield code="b"><xsl:value-of select="$sigel"/></subfield>
            </datafield>

            <!-- 856 -->
            <datafield ind1="4" ind2="0" tag="856">
                <subfield code="u"><xsl:value-of select="concat($proxy, $id)"/></subfield>
                <xsl:variable name="startDate" select="substring-before($interval, '|')"/>
                <xsl:variable name="endDate" select="substring-after($interval, '|')"/>
                <xsl:choose>
                    <xsl:when test="$startDate != '' and $endDate != ''">
                        <subfield code="z"><xsl:value-of select="$startDate"/> - <xsl:value-of select="$endDate"/></subfield>
                    </xsl:when>
                    <xsl:when test="$startDate != ''">
                        <subfield code="z"><xsl:value-of select="$startDate"/> -</subfield>
                    </xsl:when>
                    <xsl:when test="$endDate != ''">
                        <subfield code="z">- <xsl:value-of select="$endDate"/></subfield>
                    </xsl:when>
                </xsl:choose>
                <subfield code="z"><xsl:value-of select="$comment"/></subfield>
            </datafield>
        </record>
    </xsl:template>

    <!-- 008 - Datum 1  -->
    <xsl:template match="ims2:metametadata/ims2:contribute[ims2:date/ims2:description/ims2:langstring[normalize-space(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'created']][1]">
        <xsl:variable name="tmp1" select="normalize-space(ims2:date/ims2:datetime)"/>
        <xsl:variable name="tmp2" select="translate(substring($tmp1, 1, 10), '-', '')"/>
        <xsl:value-of select="substring($tmp2, 3, 6)"/>
    </xsl:template>

    <!-- 008 - Datum 2  -->
    <xsl:template match="ims2:lifecycle/ims2:contribute[ims2:role/ims2:value/ims2:langstring[normalize-space(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'producingcompany']][1]">
        <xsl:value-of select="ims2:date/ims2:datetime"/>
    </xsl:template>

    <!-- 046 m.m. - Datum 3  (intervall) -->
    <xsl:template match="ims2:general/ims3:distributionevents/ims3:distributionevent" mode="getDates">
        <!--<xsl:variable name="rawTimeStamp"><xsl:value-of select="java:format(java:java.text.SimpleDateFormat.new('yyyy-MM-dd'), java:java.util.Date.new())"/></xsl:variable>-->
        <xsl:variable name="rawStartDate" select="normalize-space(substring-before(ims3:period/ims3:startdate, 'T'))"/>
        <xsl:variable name="rawEndDate" select="normalize-space(substring-before(ims3:period/ims3:enddate, 'T'))"/>
        <!--<xsl:variable name="timeStamp" select="translate($rawTimeStamp, '-', '')"/>-->
        <xsl:variable name="startDate" select="translate($rawStartDate, '-', '')"/>
        <xsl:variable name="endDate" select="translate($rawEndDate, '-', '')"/>
        <xsl:if test="$startDate != '' or $endDate != ''"><xsl:value-of select="$startDate"/>|<xsl:value-of select="$endDate"/></xsl:if>
        <!--<xsl:if test="($startDate != '' and $startDate &lt;= $timeStamp and ($endDate = '' or $endDate >= $timeStamp)) or ($startDate = '' and $endDate != '' and $endDate >= $timeStamp)"><xsl:value-of select="$startDate"/>|<xsl:value-of select="$endDate"/></xsl:if>-->
    </xsl:template>
    
    
</xsl:stylesheet>