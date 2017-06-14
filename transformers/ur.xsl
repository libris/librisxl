<?xml version="1.0" encoding="UTF-8" ?>

<!--
    Document   : ur.xsl
    Created on : August 27, 2007, 4:37 PM
    Updated on: December 08, 2008
    Updated on: December 04, 2009 (delete flag check)
    Updated on: April 26, 2009 (holdings for Chalmers) etc.
    Updated on: October 22, 2010 (holdings for KIB).
    Updated on: May 14, 2013 (delete flag synchronized with Kai).
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
        
        <!-- Används som villkor för att posten ska läggas in. Variabeln återanvänds också för att lagra start- och slutdatum (separerade med |) för posten -->
        <!-- old version
        <xsl:variable name="cond-deal">
            <xsl:for-each select="ims2:general/ims3:distributionevents/ims3:distributionevent">
                <xsl:variable name="case-platform" select="normalize-space(translate(ims3:platform, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>
                <xsl:variable name="case-type" select="normalize-space(translate(ims3:type, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>
                <xsl:variable name="case-rag" select="normalize-space(translate(ims3:receivingagentgroup, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>
                <xsl:variable name="rawTimeStamp"><xsl:value-of select="java:format(java:java.text.SimpleDateFormat.new('yyyy-MM-dd'), java:java.util.Date.new())"/></xsl:variable>
                <xsl:variable name="rawStartDate" select="normalize-space(substring-before(ims3:period/ims3:startdate, 'T'))"/>
                <xsl:variable name="rawEndDate" select="normalize-space(substring-before(ims3:period/ims3:enddate, 'T'))"/>
                <xsl:variable name="timeStamp" select="translate($rawTimeStamp, '-', '')"/>
                <xsl:variable name="startDate" select="translate($rawStartDate, '-', '')"/>
                <xsl:variable name="endDate" select="translate($rawEndDate, '-', '')"/>
                <xsl:if test="$case-platform = 'internet' and $case-type = 'download' and $case-rag = 'avc'">
                    <xsl:if test="($startDate != '' and $startDate &lt;= $timeStamp and ($endDate = '' or $endDate >= $timeStamp)) or ($startDate = '' and $endDate != '' and $endDate >= $timeStamp)"><xsl:value-of select="$startDate"/>|<xsl:value-of select="$endDate"/></xsl:if>
                </xsl:if>
            </xsl:for-each>
        </xsl:variable>
        -->
        <!-- Turn this check off: Kai has already done it -->
        <!-- Om det finns ett distributionevent med platform = "internet", type = "download" och receivingagentgroup = "avc" -->
        <!--<xsl:variable name="cond-deal">
            <xsl:if test="ims2:general/ims3:distributionevents/ims3:distributionevent[normalize-space(translate(ims3:platform, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'internet'
            and normalize-space(translate(ims3:type, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'download'
            and normalize-space(translate(ims3:receivingagentgroup, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'avc']">true</xsl:if>
        </xsl:variable>-->

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

        <!-- Används i record och df500 -->
        <xsl:variable name="case-durationstring" select="normalize-space(translate(ims2:technical/ims2:duration/ims2:description/ims2:langstring, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>

        <!-- Används i df500 -->
        <xsl:variable name="duration">
            <xsl:if test="normalize-space(ims2:technical/ims2:duration/ims2:datetime) and normalize-space(translate(ims2:technical/ims2:duration/ims2:description/ims2:langstring, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'speltid'">
                <!-- Anropa template för att formatera datum -->
                <xsl:call-template name="create-df500a">
                    <xsl:with-param name="duration-time" select="normalize-space(ims2:technical/ims2:duration/ims2:datetime)"/>
                </xsl:call-template>
            </xsl:if>
        </xsl:variable>

        <!-- Används som villkor för att posten ska läggas in -->
        <xsl:variable name="case-learningresourcetype">
            <xsl:if test="count(ims2:educational/ims2:learningresourcetype/ims2:value/ims2:langstring[normalize-space(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'program']) > 0">program</xsl:if>
        </xsl:variable>

        <!-- Villkor för transformationen  -->
        <xsl:if test="$title != '' and $case-status = 'final' and $case-learningresourcetype = 'program' and ($case-format = 'video' or $case-format = 'audio')">
        <!--<xsl:if test="$title != '' and $cond-deal != '' and $case-status = 'final' and $case-learningresourcetype = 'program' and ($case-format = 'video' or $case-format = 'audio')">-->
            <!--<xsl:variable name="datum" select="string(java:java.util.GregorianCalendar.getInstance())"/>
            <xsl:variable name="month" select="java:java.util.GregorianCalendar.get($datum, 2)"/>-->
            <!--<record type="Bibliographic" case-status="{$case-status}" case-format="{$case-format}" cond-deal="{$cond-deal}">-->
            <!--<xsl:variable name="datum2"><xsl:value-of select="string(java:java.util.Date.new())"/></xsl:variable>
            <xsl:variable name="formatted"><xsl:value-of select="string(java:java.text.DateFormat.getInstance().format($datum))"/></xsl:variable>-->
            <xsl:variable name="interval">
                <xsl:apply-templates select="ims2:general/ims3:distributionevents/ims3:distributionevent[normalize-space(translate(ims3:platform, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'internet'
            and normalize-space(translate(ims3:type, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'download'
            and normalize-space(translate(ims3:receivingagentgroup, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'avc'][1]" mode="getDates"/>
            </xsl:variable>
            <record type="Bibliographic">
                <!-- leader  -->
                <leader><xsl:call-template name="create-leader"><xsl:with-param name="case-format" select="$case-format"/></xsl:call-template></leader>

                <!-- 001 - Identifier attribute (bibid)  -->
                <controlfield tag="001"><xsl:value-of select="$id"/></controlfield>

                <!-- 003 -  -->
                <controlfield tag="003">UR</controlfield>

                <!-- 007  -->
                <controlfield tag="007"><xsl:text>cr||||||||||||</xsl:text></controlfield>
                <xsl:if test="$case-format = 'video'">
                    <controlfield tag="007"><xsl:text>v||||||||</xsl:text></controlfield>
                    <!--<xsl:if test="$case-format = 'video'">v||||||||</xsl:if>-->
                    <!--<controlfield tag="007"><xsl:call-template name="create-cf007"><xsl:with-param name="case-format" select="$case-format"/></xsl:call-template></controlfield>-->
                </xsl:if>

                <!-- 008 - Date, language m.m.  -->
                <xsl:variable name="pos00-05"><xsl:apply-templates select="ims2:metametadata/ims2:contribute[ims2:date/ims2:description/ims2:langstring[normalize-space(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'created']][1]"></xsl:apply-templates></xsl:variable>
                <xsl:variable name="pos07-10"><xsl:apply-templates select="ims2:lifecycle/ims2:contribute[ims2:role/ims2:value/ims2:langstring[normalize-space(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'producingcompany']][1]"></xsl:apply-templates></xsl:variable>
                <xsl:variable name="vcard" select="ims2:lifecycle/ims2:contribute[ims2:role/ims2:value/ims2:langstring[normalize-space(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'producingcompany']][1]/ims2:centity/ims2:vcard"/>
                <!-- Mellanslaget på pos 17 läggs till vid utredigeringen -->
                <xsl:variable name="pos15-17">
                    <xsl:choose>
                        <xsl:when test="contains(normalize-space(translate($vcard, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')), 'fn:ur')">sw</xsl:when>
                        <xsl:otherwise>xx</xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
                <xsl:variable name="pos18-34">
                    <xsl:choose>
                        <xsl:when test="$case-format='audio'"><xsl:text>||| cs||||||     </xsl:text></xsl:when>
                        <xsl:when test="$case-format='video'"><xsl:text>||| c     |s   v|</xsl:text></xsl:when>
                        <!--<xsl:otherwise><xsl:text>|||w|_____|O___|O</xsl:text></xsl:otherwise>-->
                    </xsl:choose>
                </xsl:variable>
                <xsl:variable name="pos35-37">
                    <xsl:choose>
                        <xsl:when test="normalize-space(ims2:general/ims2:language)"><xsl:value-of select="normalize-space(ims2:general/ims2:language[1])"/></xsl:when>
                        <xsl:otherwise>swe</xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
                <controlfield tag="008"><xsl:choose><xsl:when test="$pos00-05 != ''"><xsl:value-of select="$pos00-05"/></xsl:when><xsl:otherwise>000101</xsl:otherwise></xsl:choose>s<xsl:choose><xsl:when test="$pos07-10 != ''"><xsl:value-of select="$pos07-10"/></xsl:when><xsl:otherwise>nnnn</xsl:otherwise></xsl:choose><xsl:text>    </xsl:text><xsl:value-of select="$pos15-17"/><xsl:text> </xsl:text><xsl:value-of select="$pos18-34"/><xsl:value-of select="$pos35-37"/><xsl:text> c</xsl:text></controlfield>

                <!-- 035 - Identifier -->
                <xsl:if test="normalize-space(ims2:general/ims2:identifier)">
                    <datafield tag="035" ind1="" ind2="">
                        <subfield code="a">UR<xsl:value-of select="$id"/></subfield>
                    </datafield>
                </xsl:if>

                <!-- 040 -->
                <datafield tag="040" ind1="" ind2="">
                    <subfield code="a">UR</subfield>
                    <subfield code="9">UR</subfield>
                </datafield>

                <!-- 041 - Language  -->
                <xsl:if test="count(ims2:general/ims2:language) > 1">
                    <datafield tag="041" ind1="0" ind2="">
                        <xsl:for-each select="ims2:general/ims2:language">
                            <subfield code="a"><xsl:value-of select="normalize-space(.)"/></subfield>
                        </xsl:for-each>
                    </datafield>
                </xsl:if>

                <!-- 042 -->
                <datafield tag="042" ind1="" ind2="">
                    <subfield code="9">UR</subfield>
                </datafield>

                <!-- 046 - Date -->
                <datafield tag="046" ind1="" ind2="">
                    <subfield code="a">m</subfield>
                    <xsl:if test="substring-before($interval, '|') != ''"><subfield code="m"><xsl:value-of select="substring-before($interval, '|')"/></subfield></xsl:if>
                    <xsl:if test="substring-after($interval, '|') != ''"><subfield code="n"><xsl:value-of select="substring-after($interval, '|')"/></subfield></xsl:if>
                </datafield>

                <!-- 084 - Classification  -->
                <!--<xsl:variable name="format084">
                    <xsl:if test="$case-format='audio'"><xsl:text>/L</xsl:text></xsl:if>
                    <xsl:if test="$case-format='video'"><xsl:text>/V</xsl:text></xsl:if>
                </xsl:variable>-->
                <xsl:for-each select="ims2:classification/ims2:taxonpath[ims2:source/ims2:langstring[normalize-space(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'sab']]">
                    <datafield tag="084" ind1="" ind2="">
                        <subfield code="a"><xsl:value-of select="normalize-space(ims2:taxon/ims2:id)"/></subfield>
                        <!--<subfield code="a"><xsl:value-of select="normalize-space(ims2:taxon/ims2:id)"/><xsl:value-of select="$format084"/></subfield>-->
                        <subfield code="2">kssb/8</subfield>
                    </datafield>
                </xsl:for-each>

                <!-- 245 och 246 - Title  -->
                <xsl:variable name="fil-ind"><xsl:call-template name="fileringsindikator"><xsl:with-param name="title_string" select="$title"/><xsl:with-param name="lang_string" select="normalize-space(ims2:general/ims2:language[1])"/></xsl:call-template></xsl:variable>
                <datafield tag="245" ind1="1" ind2="{$fil-ind}">
                    <subfield code="a"><xsl:value-of select="$title"/></subfield>
                    <!--<xsl:if test="$case-format = 'web'">
                        <subfield code="h">[Elektronisk resurs] :</subfield>
                    </xsl:if>-->
                    <subfield code="h">[Elektronisk resurs]<xsl:if test="normalize-space(ims2:general/ims3:remainderoftitle[1]/ims2:langstring)"><xsl:text> :</xsl:text></xsl:if></subfield>
                    <!--<xsl:if test="$case-format = 'audio'">
                        <subfield code="h">[Ljudupptagning]<xsl:if test="normalize-space(ims2:general/ims3:remainderoftitle[1]/ims2:langstring)"><xsl:text> : </xsl:text></xsl:if></subfield>
                    </xsl:if>
                    <xsl:if test="$case-format = 'video'">
                        <subfield code="h">[Videoupptagning]<xsl:if test="normalize-space(ims2:general/ims3:remainderoftitle[1]/ims2:langstring)"><xsl:text> : </xsl:text></xsl:if></subfield>
                    </xsl:if>-->
                    <xsl:choose>
                        <xsl:when test="normalize-space(ims2:general/ims3:remainderoftitle[1]/ims2:langstring)">
                            <subfield code="b"><xsl:value-of select="normalize-space(ims2:general/ims3:remainderoftitle[1]/ims2:langstring)"/></subfield>
                        </xsl:when>
                    </xsl:choose>
                </datafield>

                <xsl:for-each select="ims2:general/ims3:maintitle[position() > 1]">
                    <datafield tag="246" ind1="1" ind2="1">
                        <xsl:variable name="maintitle" select="ims2:langstring"/>
                        <xsl:variable name="mainpos" select="position() + 1"/>
                        <xsl:variable name="remainderoftitle" select="../ims3:remainderoftitle[$mainpos]/ims2:langstring"/>
                        <subfield code="a"><xsl:value-of select="$maintitle"/><xsl:if test="$remainderoftitle"><xsl:text> : </xsl:text><xsl:value-of select="$remainderoftitle"/></xsl:if></subfield>
                    </datafield>
                </xsl:for-each>

                <!-- 260 - Company  -->
                <!--ims2:lifecycle/ims2:contribute[ims2:role/ims2:value/ims2:langstring[normalize-space(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'producingcompany']][1]-->
                <xsl:if test="ims2:lifecycle/ims2:contribute[ims2:role/ims2:value/ims2:langstring[normalize-space(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'producingcompany']]">
                    <xsl:variable name="case-vcard" select="ims2:lifecycle/ims2:contribute[ims2:role/ims2:value/ims2:langstring[normalize-space(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'producingcompany']][1]/ims2:centity/ims2:vcard"/>
                    <xsl:variable name="case-year" select="ims2:lifecycle/ims2:contribute[ims2:role/ims2:value/ims2:langstring[normalize-space(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'producingcompany']][1]/ims2:date/ims2:description/ims2:langstring"/>
                    <xsl:variable name="date" select="normalize-space(substring(ims2:lifecycle/ims2:contribute[ims2:role/ims2:value/ims2:langstring[normalize-space(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'producingcompany']][1]/ims2:date/ims2:datetime, 1, 4))"/>
                    <datafield tag="260" ind1="" ind2="">
                        <xsl:choose>
                            <xsl:when test="contains(normalize-space(translate($case-vcard, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')), 'fn:ur')">
                                <subfield code="b">Utbildningsradion,</subfield>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:variable name="vcard" select="normalize-space(ims2:lifecycle/ims2:contribute[ims2:role/ims2:value/ims2:langstring[normalize-space(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'producingcompany']][1]/ims2:centity/ims2:vcard)"/>
                                <!--<xsl:variable name="vcard" select="translate($vtmp, ' ', '')"/>-->
                                <xsl:variable name="after" select="substring-after($vcard, 'BEGIN:VCARD FN:')"/>
                                <xsl:variable name="before" select="substring-before($after, 'END:VCARD')"/>
                                <subfield code="b"><xsl:value-of select="normalize-space($before)"/>,</subfield>
                            </xsl:otherwise>
                        </xsl:choose>
                        <xsl:if test="$date">
                            <subfield code="c"><xsl:value-of select="$date"/></subfield>
                        </xsl:if>
                    </datafield>
                </xsl:if>

                <!-- 300 - Type  -->
                <xsl:for-each select="ims2:educational">
                    <xsl:variable name="value1" select="normalize-space(translate(ims2:learningresourcetype[1]/ims2:value/ims2:langstring, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>
                    <xsl:variable name="value2" select="normalize-space(translate(ims2:learningresourcetype[2]/ims2:value/ims2:langstring, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>
                    <xsl:if test="(($value1 = 'tv' or $value1 = 'radio') and $value2 = 'program') or (($value2 = 'tv' or $value2 = 'radio') and $value1 = 'program')">
                        <datafield tag="300" ind1="" ind2="">
                            <xsl:choose>
                                <xsl:when test="$value1 = 'tv' or $value2 = 'tv'">
                                    <subfield code="a">TV-program</subfield>
                                </xsl:when>
                                <xsl:otherwise>
                                    <subfield code="a">Radioprogram</subfield>
                                </xsl:otherwise>
                            </xsl:choose>
                        </datafield>
                    </xsl:if>
                </xsl:for-each>

                <!-- 500 - Duration  -->
                <xsl:if test="$duration != ''">
                    <datafield tag="500" ind1="" ind2="">
                        <subfield code="a"><xsl:value-of select="$duration"/></subfield>
                    </datafield>
                </xsl:if>

                <!-- 505 -->
                <!--<xsl:for-each select="ims2:general/ims3:chapters">
                    <datafield tag="505" ind1="" ind2="">
                        <subfield code="a">
                            <xsl:for-each select="ims3:chapter/ims3:title">
                                <xsl:value-of select="ims2:langstring"/>
                                <xsl:if test="position() != last()">(två bindestreck)</xsl:if>
                            </xsl:for-each>
                        </subfield>
                    </datafield>
                </xsl:for-each>-->
                <!-- 508 -->
                <!--<xsl:for-each select="ims2:lifecycle/ims2:contribute">
                    <xsl:if test="normalize-space(ims2:role/ims2:value/ims2:langstring) != 'producingcompany'">
                        <datafield tag="508" ind1="" ind2="">
                            <subfield code="a"><xsl:value-of select="ims2:centity/ims2:vcard"/>(<xsl:value-of select="ims2:role/ims2:source/ims2:langstring"/>)</subfield>
                        </datafield>
                    </xsl:if>
                </xsl:for-each>
                -->
                <!-- 520 - Description  -->
                <xsl:for-each select="ims2:general/ims2:description[ims2:langstring != '']">
                    <datafield tag="520" ind1="" ind2="">
                        <subfield code="a"><xsl:value-of select="normalize-space(ims2:langstring)"/></subfield>
                    </datafield>
                </xsl:for-each>

                <!-- 520 - Description - Wait with this -->
                <!--<xsl:if test="ims2:general/ims2:description/ims2:langstring or ims2:general/ims3:easytoreaddescription/ims2:langstring">
                    <datafield tag="520" ind1="" ind2="">
                    </datafield>
                </xsl:if>-->

                <!-- 521 Åldersgrupp, svårighetsgrad  -->
                <xsl:variable name="diff">
                    <xsl:if test="ims2:educational/ims2:difficulty/ims2:value[ims2:langstring != '']">
                        <xsl:call-template name="create-age-diff-range"><xsl:with-param name="agecode" select="normalize-space(translate(ims2:educational/ims2:difficulty/ims2:value/ims2:langstring, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/></xsl:call-template>
                    </xsl:if>
                </xsl:variable>

                <xsl:for-each select="ims2:educational/ims2:typicalagerange[ims2:langstring != '']">
                    <xsl:variable name="agerange"><xsl:call-template name="create-age-diff-range"><xsl:with-param name="agecode" select="normalize-space(translate(ims2:langstring, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/></xsl:call-template></xsl:variable>
                    <xsl:if test="$agerange != '' and $agerange != 'annat'">
                    <datafield tag="521" ind1="" ind2="">
                        <subfield code="a"><xsl:value-of select="$agerange"/><xsl:if test="$diff != '' and $diff != 'annat'"><xsl:text> (</xsl:text><xsl:value-of select="$diff"/>)</xsl:if></subfield>
                    </datafield>
                    </xsl:if>
                </xsl:for-each>

                <!-- 540 Rättigheter  -->
                <xsl:if test="ims2:rights/ims2:description and ims2:rights/ims2:copyrightandotherrestrictions/ims2:value/ims2:langstring[normalize-space(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'yes']">
                    <datafield tag="540" ind1="" ind2="">
                        <subfield code="a"><xsl:value-of select="ims2:rights/ims2:description/ims2:langstring"/></subfield>
                    </datafield>
                </xsl:if>

                <!-- 599 - Datetime  -->
                <xsl:for-each select="ims2:metametadata/ims2:contribute/ims2:date">
                    <xsl:if test="normalize-space(ims2:datetime)">
                        <datafield tag="599" ind1="" ind2="">
                            <xsl:choose>
                                <xsl:when test="normalize-space(ims2:description/ims2:langstring)">
                                    <subfield code="a">UR <xsl:value-of select="normalize-space(ims2:description/ims2:langstring)"/><xsl:text> </xsl:text><xsl:value-of select="normalize-space(ims2:datetime)"/></subfield>
                                </xsl:when>
                                <xsl:otherwise>
                                    <subfield code="a">UR <xsl:value-of select="normalize-space(ims2:datetime)"/></subfield>
                                </xsl:otherwise>
                            </xsl:choose>
                        </datafield>
                    </xsl:if>
                </xsl:for-each>

                <!-- 600 -->
                <!--<xsl:for-each select="ims2:educational/ims2:learningresourcetype">
                    <datafield tag="600" ind1="" ind2="">
                        <subfield code="a"><xsl:value-of select="ims2:value/ims2:langstring"/></subfield>
                    </datafield>
                </xsl:for-each>-->

                <!-- 650 - Classification  -->
                <xsl:for-each select="ims2:classification/ims2:taxonpath[ims2:source/ims2:langstring[normalize-space(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'svenska ämnesord']]">

                    <xsl:if test="normalize-space(ims2:taxon/ims2:entry/ims2:langstring) != ''">
                        <xsl:variable name="isHuman"><xsl:call-template name="check-name"><xsl:with-param name="kandidat" select="normalize-space(ims2:taxon/ims2:entry/ims2:langstring)"/></xsl:call-template></xsl:variable>
                        <xsl:choose>
                            <xsl:when test="$isHuman != ''">
                                <datafield tag="600" ind1="1" ind2="4">
                                    <xsl:call-template name="split-name"><xsl:with-param name="candy" select="normalize-space(ims2:taxon/ims2:entry/ims2:langstring)"/></xsl:call-template>
                                </datafield>
                            </xsl:when>
                            <xsl:otherwise>
                                <datafield tag="650" ind1="" ind2="7">
                                    <subfield code="a"><xsl:value-of select="normalize-space(ims2:taxon/ims2:entry/ims2:langstring)"/></subfield>
                                    <subfield code="2">sao</subfield>
                                </datafield>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:if>
                </xsl:for-each>

                <!-- 650 - Classification  -->
                <xsl:for-each select="ims2:classification/ims2:taxonpath[ims2:source/ims2:langstring[normalize-space(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')) = 'fiktiva ämnesord']]">
                    <xsl:if test="normalize-space(ims2:taxon/ims2:entry/ims2:langstring) != ''">
                        <datafield tag="650" ind1="" ind2="7">
                            <subfield code="a"><xsl:value-of select="normalize-space(ims2:taxon/ims2:entry/ims2:langstring)"/></subfield>
                            <subfield code="2">barn</subfield>
                        </datafield>
                    </xsl:if>
                </xsl:for-each>

                <!-- 710 - Company  -->
                <xsl:for-each select="ims2:lifecycle/ims2:contribute">
                    <xsl:variable name="case-role" select="normalize-space(translate(ims2:role/ims2:value/ims2:langstring, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>
                    <xsl:variable name="case-vcard" select="normalize-space(translate(ims2:centity/ims2:vcard, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>
                    <xsl:if test="$case-role = 'producingcompany' and contains($case-vcard, 'fn:ur')">
                        <datafield tag="710" ind1="2" ind2="">
                            <subfield code="a">Sveriges utbildningsradio</subfield>
                            <subfield code="4">pro</subfield>
                        </datafield>
                    </xsl:if>
                </xsl:for-each>

                <!-- 856 - Identifier  -->
                <xsl:variable name="identifier" select="normalize-space(ims2:general/ims2:identifier)"/>
                <datafield tag="856" ind1="4" ind2="8">
                    <subfield code="u"><xsl:value-of select="$identifier"/></subfield>
                    <subfield code="z">UR.se</subfield>
                    <xsl:variable name="has-related">
                        <xsl:for-each select="ims2:relation">
                            <xsl:variable name="kind" select="normalize-space(translate(ims2:kind/ims2:value/ims2:langstring, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>
                            <xsl:if test="$kind = 'haspart' or $kind = 'ispartof' or $kind = 'pcpart' or $kind = 'cover' or $kind = 'label' or $kind = 'related'">true</xsl:if>
                        </xsl:for-each>
                    </xsl:variable>
                    <xsl:if test="contains($has-related, 'true')">
                        <subfield code="z">Sammanhörande material finns</subfield>
                    </xsl:if>
                </datafield>

                <!-- 856 - Image  -->
                <xsl:for-each select="ims2:general/ims3:image">
                    <datafield tag="856" ind1="4" ind2="8">
                        <subfield code="3">Bild:</subfield>
                        <subfield code="u"><xsl:value-of select="normalize-space(.)"/></subfield>
                    </datafield>
                </xsl:for-each>
            </record>
            <!-- Holdings-poster (uraccess-adresserna hårdkodade, kan göras till variabler vid tillfälle) -->
            <!-- Mälardalens högskola -->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'Mde'"/><xsl:with-param name="proxy" select="'http://ep.bib.mdh.se/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Tillgänglig inom högskolan och hemifrån via proxy-server för användare vid Mälardalens högskola'"/></xsl:call-template>
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'Mdv'"/><xsl:with-param name="proxy" select="'http://ep.bib.mdh.se/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Tillgänglig inom högskolan och hemifrån via proxy-server för användare vid Mälardalens högskola'"/></xsl:call-template>

            <!-- Göteborgs universitet -->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'Gdig'"/><xsl:with-param name="proxy" select="'http://ezproxy.ub.gu.se/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Extern access endast forskare och studenter vid GU'"/></xsl:call-template>

            <!-- Högskolan i Gävle -->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'Hig'"/><xsl:with-param name="proxy" select="'http://webproxy.student.hig.se:2048/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Tillgänglig för studenter och anställda vid Högskolan i Gävle'"/></xsl:call-template>

            <!-- Högskolan Kristianstad -->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'Krh'"/><xsl:with-param name="proxy" select="'https://ezproxy.hkr.se/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Tillgänglig inom Högskolan Kristianstad'"/></xsl:call-template>

            <!-- Luleå tekniska universitet -->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'LTUe'"/><xsl:with-param name="proxy" select="'http://proxy.lib.ltu.se/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'(Endast för användare inom LTU)'"/></xsl:call-template>
            
            <!-- Högskolan Väst
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'Htu'"/><xsl:with-param name="proxy" select="'http://ezproxy.server.hv.se/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Tillgänglig för studenter och anställda vid Högskolan Väst'"/></xsl:call-template> removed 160223 KP -->

            <!-- Högskolan i Borås -->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'Hib'"/><xsl:with-param name="proxy" select="'http://costello.pub.hb.se/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'TV- och radioprogram'"/></xsl:call-template>

            <!-- Högskolan i Kalmar -->
            <!--<xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'Hkal'"/><xsl:with-param name="proxy" select="'http://proxy.hik.se:2048/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Tillgänglig inom högskolan och externt via proxy-server för användare vid HIK'"/></xsl:call-template>-->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'LnuK'"/><xsl:with-param name="proxy" select="'http://proxy.lnu.se/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Tillgänglig för studenter och anställda vid Linnéuniversitetet'"/></xsl:call-template>

            <!-- Blekinge tekniska högskola -->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'Bth'"/><xsl:with-param name="proxy" select="'http://miman.bib.bth.se/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Online access for BTH'"/></xsl:call-template>

            <!-- Mittuniversitetet -->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'Mh'"/><xsl:with-param name="proxy" select="'http://www.bib.miun.se/php/go.php?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'UR access. Endast tillgänglig för Mittuniversitetets forskare och studenter. (begränsad åtkomst)'"/></xsl:call-template>

            <!-- Uppsala Learning Lab -->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'Udig'"/><xsl:with-param name="proxy" select="'http://ezproxy.its.uu.se/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Tillgänglig för användare inom Uppsala universitet'"/></xsl:call-template>

            <!-- Växjö universitet -->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'LnuV'"/><xsl:with-param name="proxy" select="'http://proxy.lnu.se/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Tillgänglig för studenter och anställda vid Linnéuniversitetet'"/></xsl:call-template>
            <!--<xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'Vo'"/><xsl:with-param name="proxy" select="'http://databas.bib.vxu.se:2048/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Tillgänglig för studenter och anställda vid Växjö universitet'"/></xsl:call-template>-->

            <!-- Karlstads universitet -->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'Kd'"/><xsl:with-param name="proxy" select="'https://login.bibproxy.kau.se:8443/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'UR Access'"/></xsl:call-template>

            <!-- Högskolan i Jönköping -->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'Jon'"/><xsl:with-param name="proxy" select="'http://login.bibl.proxy.hj.se/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'UR access (Endast tillgänglig för forskare och studenter vid Högskolan i Jönköping)'"/></xsl:call-template>

            <!-- Linköpings universitet -->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'Li'"/><xsl:with-param name="proxy" select="'http://login.e.bibl.liu.se/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Extern access endast anställda och studenter vid Liu'"/></xsl:call-template>

            <!-- Örebro universitet -->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'O'"/><xsl:with-param name="proxy" select="'http://db.ub.oru.se/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Tillgänglig inom Örebro universitet och hemifrån via proxyserver'"/></xsl:call-template>

            <!-- Lunds universitet -->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'Ldig'"/><xsl:with-param name="proxy" select="'http://ludwig.lub.lu.se/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'(Endast för användare inom LU)'"/></xsl:call-template>

            <!-- Högskolan på Gotland
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'Hgot'"/><xsl:with-param name="proxy" select="'http://proxy.hgo.se:2048/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Tillgänglig inom Almedalsbiblioteket och Högskolan på Gotland och externt för användare vid Högskolan'"/></xsl:call-template> -->

            <!-- Sophiahemmet (UPPGIFTER SAKNAS) -->
            <!--<xsl:call-template name="create-holding"><xsl:with-param name="sigel" select="'SAKNAS'"/><xsl:with-param name="proxy" select="'SAKNAS'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'SAKNAS'"/></xsl:call-template>-->

            <!-- Stockholms universitetsbibliotek -->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'HdiE'"/><xsl:with-param name="proxy" select="'https://ezp.sub.su.se/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Endast tillgänglig inom SU:s nät'"/></xsl:call-template>

            <!-- Högskolan i Halmstad -->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'Hal'"/><xsl:with-param name="proxy" select="'http://ezproxy.bib.hh.se/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Extern access via proxy-server endast för anställda och studenter vid Högskolan i Halmstad'"/></xsl:call-template>

            <!-- Högskolan i Skövde -->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'Skov'"/><xsl:with-param name="proxy" select="'https://persefone.his.se/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Tillgänglig för användare inom Högskolan i Skövde'"/></xsl:call-template>

            <!-- Södertörns högskola -->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'D'"/><xsl:with-param name="proxy" select="'http://till.biblextern.sh.se/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Access for students/staff at Södertörns högskola, SH'"/></xsl:call-template>

            <!-- Malmö högskola -->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'Mah'"/><xsl:with-param name="proxy" select="'http://support.mah.se/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Tillgänglig för studenter och anställda vid Malmö högskola'"/></xsl:call-template>

            <!-- Chalmers -->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'Z'"/><xsl:with-param name="proxy" select="'http://proxy.lib.chalmers.se/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Online access for Chalmers'"/></xsl:call-template>

            <!-- Karolinska institutet -->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'Ch'"/><xsl:with-param name="proxy" select="'http://proxy.kib.ki.se/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Endast tillgänglig för användare inom Karolinska Institutet'"/></xsl:call-template>
            
            <!-- Sveriges lantbruksuniversitet  -->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'Jslu'"/><xsl:with-param name="proxy" select="'http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Online access for SLU'"/></xsl:call-template>
            
            <!-- KTH  -->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'T'"/><xsl:with-param name="proxy" select="'http://focus.lib.kth.se/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Online access for KTH'"/></xsl:call-template>
            
            <!-- Umeå UB  -->
            <xsl:call-template name="create-holding"><xsl:with-param name="interval" select="$interval"/><xsl:with-param name="sigel" select="'Q'"/><xsl:with-param name="proxy" select="'http://proxy.ub.umu.se/login?url=http://uraccess.se/products/'"/><xsl:with-param name="id" select="$id"/><xsl:with-param name="comment" select="'Online access for UmU'"/></xsl:call-template>
        </xsl:if>
    </xsl:template>

    <!-- Skapa Holding-post  -->
    <xsl:template name="create-holding">
        <xsl:param name="interval"/>
        <xsl:param name="sigel"/>
        <xsl:param name="proxy"/>
        <xsl:param name="id"/>
        <xsl:param name="comment"/>
        <!--<xsl:variable name="rawTimeStamp"><xsl:value-of select="java:format(java:java.text.SimpleDateFormat.new('yyyy-MM-dd'), java:java.util.Date.new())"/></xsl:variable>
        <xsl:variable name="timeStamp" select="translate($rawTimeStamp, '-', '')"/>-->
        <xsl:variable name="startDate" select="substring-before($interval, '|')"/>
        <xsl:variable name="endDate" select="substring-after($interval, '|')"/>
        <xsl:variable name="leader5">
            <xsl:choose>
                <!--<xsl:when test="$endDate != '' and $timeStamp != '' and $endDate &lt; $timeStamp">d</xsl:when>-->
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

    <!-- Leader  -->
    <xsl:template name="create-leader">
        <xsl:param name="case-format"/>
        <xsl:variable name="format">
            <xsl:if test="$case-format = 'video'">g</xsl:if>
            <xsl:if test="$case-format = 'audio'">i</xsl:if>
            <!--<xsl:if test="$case-format = 'web'">a</xsl:if>-->
        </xsl:variable>
        <xsl:text>#####n</xsl:text><xsl:value-of select="$format"/><xsl:text>m a22#####3a 4500</xsl:text>
    </xsl:template>

    <!-- 500 speltid  -->
    <xsl:template name="create-df500a">
        <xsl:param name="duration-time"/>
        <xsl:variable name="tmp" select="normalize-space(translate($duration-time, ':', ''))"/>
        <xsl:variable name="hours">
        <xsl:variable name="tmphours" select="substring($tmp, 1, 2)"/>
            <xsl:choose>
                <xsl:when test="starts-with($tmphours, '0')"><xsl:value-of select="substring($tmphours, 2)"/></xsl:when>
                <xsl:otherwise><xsl:value-of select="$tmphours"/></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <xsl:variable name="minutes">
        <xsl:variable name="tmpminutes" select="substring($tmp, 3, 2)"/>
            <xsl:choose>
                <xsl:when test="starts-with($tmpminutes, '0')"><xsl:value-of select="substring($tmpminutes, 2)"/></xsl:when>
                <xsl:otherwise><xsl:value-of select="$tmpminutes"/></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <xsl:choose>
            <xsl:when test="$hours = '0'">
                <xsl:value-of select="concat('Speltid: ', $minutes, ' min.')"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="concat('Speltid: ', $hours, ' tim. ', $minutes, ' min.')"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <!-- Åldersgrupp, svårighetsgrad  -->
    <xsl:template name="create-age-diff-range">
        <xsl:param name="agecode"/>
        <xsl:choose>
            <xsl:when test="$agecode = 'preschool'">Förskola</xsl:when>
            <xsl:when test="$agecode = 'primary0-3'">Grundskola 0-3</xsl:when>
            <xsl:when test="$agecode = 'primary4-6'">Grundskola 4-6</xsl:when>
            <xsl:when test="$agecode = 'primary7-9'">Grundskola 7-9</xsl:when>
            <xsl:when test="$agecode = 'schoolvux'">Vuxövergripande</xsl:when>
            <xsl:when test="$agecode = 'university'">Högskola</xsl:when>
            <xsl:when test="$agecode = 'komvuxgrundvux'">Komvux/Grundvux</xsl:when>
            <xsl:when test="$agecode = 'teachereducation'">Lärarfortbildning</xsl:when>
            <xsl:when test="$agecode = 'folkhighschool'">Folkhögskola/Studieförbund</xsl:when>
            <xsl:when test="$agecode = 'secondary'">Gymnasieskola</xsl:when>

            <xsl:when test="$agecode = 'very easy' or $agecode = 'veryeasy'">mycket lätt</xsl:when>
            <xsl:when test="$agecode = 'easy'">lätt</xsl:when>
            <xsl:when test="$agecode = 'medium' or $agecode = 'medium difficulty' or $agecode = 'mediumdifficulty'">medel</xsl:when>
            <xsl:when test="$agecode = 'difficult'">avancerad</xsl:when>
            <xsl:when test="$agecode = 'very difficult' or $agecode = 'verydifficult'">mycket avancerad</xsl:when>
            <xsl:otherwise>annat</xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <!-- Bestämmer filering utifrån artikel  -->
    <xsl:template name="fileringsindikator">
        <xsl:param name="title_string"/>
        <xsl:param name="lang_string"/>
        <xsl:variable name="tmp" select="substring-before(translate($title_string, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'), ' ')"/>
        <!--<xsl:variable name="tmp" select="translate($tmp0, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')"/>-->
        <xsl:choose>
            <xsl:when test="($tmp = 'a' and $lang_string = 'eng') or (starts-with($tmp, 'l') and $tmp != 'la' and $tmp != 'las' and $tmp != 'le' and $tmp != 'les' and $tmp != 'los' and $lang_string = 'fre')">2</xsl:when>
            <xsl:when test="($tmp = 'an' and $lang_string = 'eng') or ($tmp = 'la' and ($lang_string = 'fre' or $lang_string = 'spa')) or ($tmp = 'el' and $lang_string = 'spa') or ($tmp = 'en' and $lang_string = 'swe') or ($tmp = 'le' and $lang_string = 'fre')">3</xsl:when>
            <xsl:when test="(($tmp = 'das' or $tmp = 'der' or $tmp = 'die') and $lang_string = 'ger') or ($tmp = 'ett' and $lang_string = 'swe') or (($tmp = 'las' or $tmp = 'los') and $lang_string = 'spa') or ($tmp = 'les' and $lang_string = 'fre') or ($tmp = 'the' and $lang_string = 'eng')">4</xsl:when>
            <xsl:otherwise>0</xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <!-- Kollar om ett ämnesord är av typen personnamn (Efternamn, Förnamn) -->
    <xsl:template name="check-name">
        <xsl:param name="kandidat"/>
        <xsl:if test="contains($kandidat, ', ')">isPerson</xsl:if>
        <!--<xsl:variable name="before" select="substring-before($kandidat, ', ')"/>
        <xsl:variable name="after" select="substring-after($kandidat, ', ')"/>
        <xsl:if test="(starts-with($before, 'A') or starts-with($before, 'B') or starts-with($before, 'C') or starts-with($before, 'D') or starts-with($before, 'E') or starts-with($before, 'F') or starts-with($before, 'G') or
                starts-with($before, 'H') or starts-with($before, 'I') or starts-with($before, 'J') or starts-with($before, 'K') or starts-with($before, 'L') or starts-with($before, 'M') or starts-with($before, 'N') or
                starts-with($before, 'O') or starts-with($before, 'P') or starts-with($before, 'Q') or starts-with($before, 'R') or starts-with($before, 'S') or starts-with($before, 'T') or starts-with($before, 'U') or
                starts-with($before, 'V') or starts-with($before, 'W') or starts-with($before, 'X') or starts-with($before, 'Y') or starts-with($before, 'Z') or starts-with($before, 'Å') or starts-with($before, 'Ä') or starts-with($before, 'Ö'))
                and (starts-with($after, 'A') or starts-with($after, 'B') or starts-with($after, 'C') or starts-with($after, 'D') or starts-with($after, 'E') or starts-with($after, 'F') or starts-with($after, 'G') or
                starts-with($after, 'H') or starts-with($after, 'I') or starts-with($after, 'J') or starts-with($after, 'K') or starts-with($after, 'L') or starts-with($after, 'M') or starts-with($after, 'N') or
                starts-with($after, 'O') or starts-with($after, 'P') or starts-with($after, 'Q') or starts-with($after, 'R') or starts-with($after, 'S') or starts-with($after, 'T') or starts-with($after, 'U') or
        starts-with($after, 'V') or starts-with($after, 'W') or starts-with($after, 'X') or starts-with($after, 'Y') or starts-with($after, 'Z') or starts-with($after, 'Å') or starts-with($after, 'Ä') or starts-with($after, 'Ö'))">
            <xsl:text>isPerson</xsl:text>
        </xsl:if>-->
    </xsl:template>

    <!-- Delar upp ett namn i namn- och årtalsdel -->
    <xsl:template name="split-name">
        <xsl:param name="candy"/>
        <xsl:variable name="tmp1" select="substring-after($candy, ',')"/>
        <xsl:choose>
            <xsl:when test="contains($tmp1, ',')">
                <xsl:variable name="year" select="substring-after($tmp1, ',')"/>
                <xsl:variable name="name" select="substring-before($candy, $year)"/>
                <subfield code="a"><xsl:value-of select="normalize-space($name)"/></subfield>
                <subfield code="d"><xsl:value-of select="normalize-space($year)"/></subfield>
            </xsl:when>
            <xsl:otherwise>
                <subfield code="a"><xsl:value-of select="normalize-space($candy)"/></subfield>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
</xsl:stylesheet>
