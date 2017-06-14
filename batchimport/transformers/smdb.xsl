<?xml version="1.0" encoding="UTF-8" ?>

<!--
    Document   : smdb.xsl
    Created on : December 5, 2007, 7:50 AM
    Author     : pelle
    Updated    : November, 2010 (ny XML-struktur)
    Updated    : September, 2011 (ny logik för bp)
    Updated    : November, 2011 (first production version)
    Description:
        Purpose of transformation follows.
-->

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0"
                xmlns:oai="http://www.openarchives.org/OAI/2.0/" xmlns:smdb="http://smdb.kb.se/schema/smdb" xmlns:java="http://xml.apache.org/xslt/java" xmlns="http://www.loc.gov/MARC21/slim" exclude-result-prefixes="oai smdb">
    <xsl:output method="xml" indent="yes"/>

    <xsl:template match="/oai:OAI-PMH">
        <xsl:apply-templates select="oai:ListRecords"/>
    </xsl:template>

    <xsl:template match="oai:ListRecords">
        <collection xmlns="http://www.loc.gov/MARC21/slim">
            <xsl:apply-templates
                    select="oai:record/oai:metadata/smdb:record[smdb:part[@type = 'kp' and smdb:field[@id = '002' and smdb:value = 'monografi'] and smdb:field[@id = '003' and smdb:value = '3'] and smdb:field[@id = '043' and smdb:value = 'ljudbok']]]"/>
        </collection>
    </xsl:template>

    <xsl:template match="smdb:record">
        <!--<smdbrecord id="{@id}" bpcount="{count(smdb:part[@type='bp'])}">-->
            <xsl:apply-templates select="smdb:part[@type='bp' and not(smdb:field[@id='090' and smdb:value[@sub='b' and . = 'fil']])]"/>
        <!--</smdbrecord>-->

    </xsl:template>

    <xsl:template match="smdb:part[@type='bp']">
        <record type="Bibliographic">
            <!-- ***** Leader -->
            <leader><xsl:text>#####nim a22#####</xsl:text><xsl:choose><xsl:when test="../smdb:part[@type = 'kp' and smdb:field[@id = '029' and smdb:value = 'Suecana']]">7</xsl:when><xsl:otherwise><xsl:text> </xsl:text></xsl:otherwise></xsl:choose><xsl:text>a 4500</xsl:text></leader>

            <!-- ***** Controlfield 003 -->
            <controlfield tag="003">AVM</controlfield>

            <!-- ***** Controlfield 007 -->
            <xsl:choose>
                <xsl:when test="smdb:field[@id='090' and smdb:value[@sub='b' and . = 'kassett']]">
                    <controlfield tag="007">ss||||||||||||</controlfield>
                </xsl:when>
                <xsl:when test="smdb:field[@id='090' and smdb:value[@sub='b' and . = 'skiva']]">
                    <controlfield tag="007">sd||||||||||||</controlfield>
                </xsl:when>
                <xsl:otherwise>
                    <controlfield tag="007">su||||||||||||</controlfield>
                </xsl:otherwise>
            </xsl:choose>
            <!--<xsl:if test="smdb:field[@id='090' and smdb:value[@sub='b' and . = 'skiva']] and smdb:field[@id='097' and smdb:value]">
                <controlfield tag="007">co||||||||||||</controlfield>
            </xsl:if>-->


            <!-- ***** Controlfield 008 -->
            <xsl:variable name="cf008_00-05" select="substring(normalize-space(../smdb:part[@type = 'sys']/smdb:field[@id = '125']/smdb:value), 3)"/>
            <xsl:variable name="cf008_06" select="normalize-space(../smdb:part[@type = 'kp']/smdb:field[@id='006']/@ind)"/>

            <xsl:variable name="cf008_07-10">
                <xsl:choose>
                    <xsl:when test="string-length(../smdb:part[@type = 'kp']/smdb:field[@id = '006']/smdb:value[@sub='a']) = 4">
                        <xsl:call-template name="replace-character"><xsl:with-param name="word" select="../smdb:part[@type = 'kp']/smdb:field[@id = '006']/smdb:value[@sub='a']"/></xsl:call-template>
                    </xsl:when>
                    <xsl:otherwise>uuuu</xsl:otherwise>
                </xsl:choose>
            </xsl:variable>

            <xsl:variable name="cf008_11-14">
                <xsl:choose>
                    <xsl:when test="string-length(../smdb:part[@type = 'kp']/smdb:field[@id = '006']/smdb:value[@sub='b']) = 4">
                        <xsl:call-template name="replace-character"><xsl:with-param name="word" select="../smdb:part[@type = 'kp']/smdb:field[@id = '006']/smdb:value[@sub='b']"/></xsl:call-template>
                    </xsl:when>
                    <xsl:otherwise><xsl:text>    </xsl:text></xsl:otherwise>
                </xsl:choose>
            </xsl:variable>

            <xsl:variable name="cf008_15-17">
                <xsl:variable name="temp" select="normalize-space(../smdb:part[@type = 'kp']/smdb:field[@id = '017']/smdb:value)"/>
                <xsl:choose>
                    <xsl:when test="string-length($temp) = 2"><xsl:value-of select="concat($temp, ' ')"/></xsl:when>
                        <xsl:when test="string-length($temp) = 3"><xsl:value-of select="$temp"/></xsl:when>
                    <xsl:otherwise><xsl:text>   </xsl:text></xsl:otherwise>
                </xsl:choose>
            </xsl:variable>

            <xsl:variable name="cf008_18-34">
                <xsl:text>nnn|  |||||||| n </xsl:text>
                <!--<xsl:choose>
                    <xsl:when test="smdb:field[@id='090' and smdb:value[@sub='b' and . = 'skiva']] and smdb:field[@id='097' and smdb:value]">
                        <xsl:text>nnn| q|||||||| n </xsl:text>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:text>nnn|  |||||||| n </xsl:text>
                    </xsl:otherwise>
                </xsl:choose>-->
            </xsl:variable>

            <xsl:variable name="cf008_35-37">
                <xsl:variable name="temp" select="normalize-space(../smdb:part[@type = 'kp']/smdb:field[@id = '038' and string-length(smdb:value[@sub='a']) = 3][1]/smdb:value)"/>
                    <xsl:choose>
                        <xsl:when test="$temp != ''"><xsl:value-of select="$temp"/></xsl:when>
                    <xsl:otherwise>und</xsl:otherwise>
                </xsl:choose>
            </xsl:variable>

            <xsl:variable name="cf008_38-39">
                <xsl:choose>
                    <xsl:when test="../smdb:part[@type = 'kp' and smdb:field[@id = '029' and smdb:value = 'Suecana']]"><xsl:text> c</xsl:text></xsl:when>
                    <xsl:otherwise><xsl:text>  </xsl:text></xsl:otherwise>
                </xsl:choose>
            </xsl:variable>

            <controlfield tag="008"><xsl:value-of select="$cf008_00-05"/><xsl:value-of select="$cf008_06"/><xsl:value-of select="$cf008_07-10"/><xsl:value-of select="$cf008_11-14"/><xsl:value-of select="$cf008_15-17"/><xsl:value-of select="$cf008_18-34"/><xsl:value-of select="$cf008_35-37"/><xsl:value-of select="$cf008_38-39"/></controlfield>

            <!-- ***** Variabel för titeltyp -->
            <xsl:variable name="df007" select="normalize-space(translate(../smdb:part[@type = 'kp']/smdb:field[@id = '007']/smdb:value, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>

            <!-- ***** Titel -->
            <xsl:apply-templates select="../smdb:part[@type = 'kp']/smdb:field[@id = '008']"/>

            <!-- ***** Uniform titel -->
            <xsl:apply-templates select="../smdb:part[@type = 'kp']/smdb:field[@id = '010']"><xsl:with-param name="df007" select="$df007"/></xsl:apply-templates>

            <!-- ***** Annan (eller uniform) titel -->
            <xsl:apply-templates select="../smdb:part[@type = 'kp']/smdb:field[@id = '009' and @ind != 'o']"/>

            <!-- ***** Om 010 saknas, använd 009 istället -->
            <xsl:if test="not(../smdb:part[@type = 'kp']/smdb:field[@id = '010'])">
                <xsl:apply-templates select="../smdb:part[@type = 'kp']/smdb:field[@id = '009' and @ind = 'o']"><xsl:with-param name="df007" select="$df007"/></xsl:apply-templates>
            </xsl:if>

            <!-- ***** Serie/Box, tag 011 -->
            <xsl:apply-templates select="../smdb:part[@type = 'kp']/smdb:field[@id = '011']"/>

            <!-- ***** Person/institution (012, 013) -->
            <xsl:apply-templates select="../smdb:part[@type = 'kp']/smdb:field[@id = '012']"><xsl:with-param name="df007" select="$df007"/></xsl:apply-templates>
            <xsl:apply-templates select="../smdb:part[@type = 'kp']/smdb:field[@id = '013']"><xsl:with-param name="df007" select="$df007"/></xsl:apply-templates>

            <!-- ***** Upplaga (tag 014) -->
            <xsl:apply-templates select="../smdb:part[@type = 'kp']/smdb:field[@id = '014']"/>

            <!-- ***** Utgivning/år/anmärkning (tag 015, 016) -->
            <!--<xsl:apply-templates select="../smdb:part[@type = 'kp']/smdb:field[@id = '015']"><xsl:with-param name="bppos" select="position()"/></xsl:apply-templates>-->
            <xsl:if test="../smdb:part[@type = 'kp']/smdb:field[@id = '015']">
                <datafield ind1=" " ind2=" " tag="260">
                    <xsl:apply-templates select="../smdb:part[@type = 'kp']/smdb:field[@id = '015']"/>        
                </datafield>
            </xsl:if>
            
            <!-- ***** Inspelningsummer (tag 023) -->
            <xsl:apply-templates select="../smdb:part[@type = 'kp']/smdb:field[@id = '023' and (@ind = 'a' or @ind = 'm' or @ind = 'p' or @ind = 'r')]"/>

            <!-- ***** Anmärkning (tag 024) -->
            <xsl:apply-templates select="../smdb:part[@type = 'kp']/smdb:field[@id = '024']"/>

            <!-- ***** Innehåll (tag 025) -->
            <xsl:apply-templates select="../smdb:part[@type = 'kp']/smdb:field[@id = '025']"/>

            <!-- ***** Sammanfattning, tag 026 -->
            <xsl:apply-templates select="../smdb:part[@type = 'kp']/smdb:field[@id = '026']"/>

            <!-- ***** Språk, tag 038 -->
            <xsl:if test="count(../smdb:part[@type = 'kp']/smdb:field[@id = '038' and smdb:value[@sub = 'a' and string-length(.) = 3]]) > 1">
                <datafield ind1=" " ind2=" " tag="041">
                    <xsl:apply-templates select="../smdb:part[@type = 'kp']/smdb:field[@id = '038' and smdb:value[@sub = 'a' and string-length(.) = 3]][position() > 1]"/>
                </datafield>
            </xsl:if>

            <!-- ***** SAB, tag 040 -->
            <xsl:apply-templates select="../smdb:part[@type = 'kp']/smdb:field[@id = '040']"/>

            <!-- ***** Hårdkoda in ett 040a S -->
            <datafield ind1=" " ind2=" " tag="040">
                <subfield code="a">S</subfield>
            </datafield>

            <!-- ***** Anmärkning, tag 029 -->
            <xsl:apply-templates select="../smdb:part[@type = 'kp']/smdb:field[@id = '029' and not(smdb:value = 'Suecana')]"/>
            
            <!-- ***** Hårdkoda in ett 0429 NB/SUECANA -->
            <datafield ind1=" " ind2=" " tag="042">
                <xsl:choose>
                    <xsl:when test="../smdb:part[@type = 'kp' and smdb:field[@id = '029' and smdb:value = 'Suecana']]">
                        <subfield code="9">SUEC</subfield>
                    </xsl:when>
                    <xsl:otherwise>
                        <subfield code="9">NB</subfield>
                    </xsl:otherwise>
                </xsl:choose>
            </datafield>

            <!-- ***** Genre, tag 042 -->
            <xsl:apply-templates select="../smdb:part[@type = 'kp']/smdb:field[@id = '042' and smdb:value]"/>

            <!-- ***** Publikationstyp, tag 043 -->
            <xsl:apply-templates select="../smdb:part[@type = 'kp']/smdb:field[@id = '043' and smdb:value]"/>

            <!-- ***** Arkivnummer, tag 130 -->
            <!--<xsl:apply-templates select="smdb:part[@type = 'kp']/smdb:field[@id = '130' and smdb:value[@sub = 'a']]"/>-->

            <!-- ***** Titel, DP tag 010
            <xsl:apply-templates select="smdb:part[@type = 'dp']/smdb:field[@id = '010' and subfield != '']"/> -->

            <!-- ***** Inspelningsummer (DP tag 023) -->
            <xsl:apply-templates select="../smdb:part[@type = 'dp']/smdb:field[@id = '023' and (@ind = 'a' or @ind = 'm' or @ind = 'r')]"/>

            <!-- ***** Arkivnummer, DP tag 130 -->
            <!--<xsl:apply-templates select="smdb:part[@type = 'dp']/smdb:field[@id = '130' and smdb:value[@sub = 'a']]"/>-->

            <!-- ***** Övrigt, DP tag 050, 010, 024, 012, 013 -->
            <xsl:apply-templates select="../smdb:part[@type = 'dp']"/>

            <!-- ***** Utgivningssummer (BP tag 077) -->
            <xsl:apply-templates select="smdb:field[@id = '077' and smdb:value]"/>

            <!-- ***** Annan utgåva (BP tag 016) -->
            <xsl:if test="position() > 1">
                <xsl:apply-templates select="smdb:field[@id = '016' and smdb:value[@sub = 'a']]"/>
            </xsl:if>
            
            <!-- ***** Fysisk beskrivning, BP tag 089, 090, 097, 093 -->
            <xsl:apply-templates select="smdb:field[@id = '090' and smdb:value[@sub = 'e']]"/>

            <!-- ***** Manifestationsspecifika anmärkningar, BP tag 110 -->
            <xsl:apply-templates select="smdb:field[(@id = '110' or @id = '111') and smdb:value]"/>
            
            <!-- ***** Manifestationsspecifika anmärkningar, BP tag 111 -->
            <!--<xsl:apply-templates select="smdb:field[@id = '111' and smdb:value]"/>-->
            
            <!-- ***** Senast ändrad, SYS tag 126 -->
            <xsl:apply-templates select="../smdb:part[@type = 'sys']/smdb:field[@id = '126' and smdb:value]"/>

            <!-- ***** Post-ID, SYS tag 901 -->
            <xsl:apply-templates select="../smdb:part[@type = 'sys']/smdb:field[@id = '901' and smdb:value]"><xsl:with-param
                    name="pos" select="position()"/></xsl:apply-templates>

        </record>
        <record type="Holdings">
            <!-- leader -->
            <leader>#####nx  a22#####1n 4500</leader>
            <!-- Timestamp -->
            <xsl:variable name="rawTimeStamp"><xsl:value-of select="java:format(java:java.text.SimpleDateFormat.new('yy-MM-dd'), java:java.util.Date.new())"/></xsl:variable>
            <xsl:variable name="timeStamp" select="translate($rawTimeStamp, '-', '')"/>
            
            <!-- ***** Controlfield 008 -->
            <!--<xsl:variable name="cf008_00-05" select="substring(normalize-space(../smdb:part[@type = 'sys']/smdb:field[@id = '125']/smdb:value), 3)"/>-->
            <!--<controlfield tag="008"><xsl:value-of select="$cf008_00-05"/>||0000|||||000||||||000000</controlfield>-->
            <controlfield tag="008"><xsl:value-of select="$timeStamp"/>||0000|||||000||||||000000</controlfield>
            <!-- ***** Post_ID (SYS tag 901) -->
            <xsl:if test="../smdb:part[@type = 'sys']/smdb:field[@id = '901' and smdb:value]">
                <datafield ind1=" " ind2=" " tag="035">
                    <subfield code="a">AVM<xsl:value-of select="../smdb:part[@type = 'sys']/smdb:field[@id = '901' and smdb:value][1]/smdb:value"/><xsl:if
                            test="position() > 1">bp<xsl:value-of select="position()"/></xsl:if></subfield>
                </datafield>
            </xsl:if>

            <!-- ***** Förvärv (BP tag 082) -->
            <xsl:apply-templates select="smdb:field[@id = '082' and smdb:value]"/>

            <!-- ***** AVM-flagga -->
            <datafield ind1=" " ind2=" " tag="599">
                <subfield code="a">AVM</subfield>
            </datafield>

            <!-- ***** Sigel/signum -->
            <datafield ind1=" " ind2=" " tag="852">
                <subfield code="b">S</subfield>
                <subfield code="h">AVM</subfield>
            </datafield>

            <!-- ***** Fulltext-URL, SYS tag 901 -->
            <xsl:if test="smdb:field[@id = '090' and smdb:value[. = 'fil']]">
                <xsl:apply-templates select="../smdb:part[@type = 'sys']/smdb:field[@id = '901' and smdb:value]" mode="holdings"/>
            </xsl:if>
        </record>
    </xsl:template>

    <!-- KP -->
    <!-- ***** Datafield 245 (titel) -->
    <xsl:template match="smdb:part[@type = 'kp']/smdb:field[@id = '008']">
        <xsl:variable name="ind2"><xsl:choose><xsl:when test="smdb:value[@sub = 'a' and @ind]"><xsl:value-of select="smdb:value[@sub = 'a' and @ind]/@ind"/></xsl:when><xsl:otherwise>0</xsl:otherwise></xsl:choose></xsl:variable>
        <datafield ind1="1" ind2="{$ind2}" tag="245">
            <xsl:for-each select="smdb:value[@sub]">
                <xsl:if test="@sub = 'a'">
                    <subfield code="a"><xsl:value-of select="."/><xsl:choose><xsl:when test="../smdb:value[@sub = 'd' or @sub = 'e'] and following-sibling::smdb:value[1][@sub = 'b']"><xsl:text> :</xsl:text></xsl:when><xsl:when test="../smdb:value[@sub = 'd' or @sub = 'e']"><xsl:text>.</xsl:text></xsl:when></xsl:choose></subfield>
                    <xsl:if test="not(../smdb:value[@sub = 'd' or @sub = 'e'])">
                        <subfield code="h">[Ljudupptagning]<xsl:choose><xsl:when test="following-sibling::smdb:value[1][@sub = 'b']"><xsl:text> :</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[1][@sub = 'c']"><xsl:text> /</xsl:text></xsl:when></xsl:choose></subfield>
                    </xsl:if>
                </xsl:if>
                <xsl:if test="@sub = 'b'">
                    <subfield code="b"><xsl:value-of select="."/><xsl:choose><xsl:when test="following-sibling::smdb:value[1][@sub = 'd' or @sub = 'e']"><xsl:text>.</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[1][@sub = 'c']"><xsl:text> /</xsl:text></xsl:when></xsl:choose></subfield>
                </xsl:if>
                <xsl:if test="@sub = 'd'">
                    <subfield code="n"><xsl:value-of select="."/><xsl:choose><xsl:when test="following-sibling::smdb:value[1][@sub = 'd']"><xsl:text>.</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[1][@sub = 'e']"><xsl:text>,</xsl:text></xsl:when></xsl:choose></subfield>
                    <xsl:if test="not(following-sibling::smdb:value[@sub = 'd' or @sub = 'e'])">
                        <subfield code="h">[Ljudupptagning]<xsl:if test="following-sibling::smdb:value[1][@sub = 'c']"><xsl:text> /</xsl:text></xsl:if></subfield>
                    </xsl:if>
                </xsl:if>
                <xsl:if test="@sub = 'e'">
                    <subfield code="p"><xsl:value-of select="."/><xsl:if test="following-sibling::smdb:value[1][@sub = 'e']"><xsl:text>.</xsl:text></xsl:if></subfield>
                    <xsl:if test="not(following-sibling::smdb:value[@sub = 'd' or @sub = 'e'])">
                        <subfield code="h">[Ljudupptagning]<xsl:if test="following-sibling::smdb:value[1][@sub = 'c']"><xsl:text> /</xsl:text></xsl:if></subfield>
                    </xsl:if>
                </xsl:if>
                <xsl:if test="@sub = 'c'">
                    <subfield code="c"><xsl:value-of select="."/></subfield>
                </xsl:if>
            </xsl:for-each>
        </datafield>
    </xsl:template>

        <!-- ***** Uniform titel, tag 010 -->
        <xsl:template match="smdb:part[@type = 'kp']/smdb:field[@id = '010']">
        <xsl:param name="df007"/>
        <xsl:variable name="tag"><xsl:call-template name="get-unitag"><xsl:with-param name="df007" select="$df007"/></xsl:call-template></xsl:variable>
        <xsl:variable name="ind1"><xsl:choose><xsl:when test="$tag = '240'">1</xsl:when><xsl:otherwise>0</xsl:otherwise></xsl:choose></xsl:variable>
        <xsl:variable name="ind2"><xsl:choose><xsl:when test="smdb:value[@sub = 'a' and @ind]"><xsl:value-of select="smdb:value[@sub = 'a' and @ind]/@ind"/></xsl:when><xsl:otherwise>0</xsl:otherwise></xsl:choose></xsl:variable>

        <datafield ind1="{$ind1}" ind2="{$ind2}" tag="{$tag}">
            <xsl:for-each select="smdb:value[@sub]">
                <xsl:if test="@sub = 'a'">
                    <subfield code="a"><xsl:value-of select="."/><xsl:choose><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'd' or @sub = 'h' or @sub = 'j' or @sub = 'c']"><xsl:text>.</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'n' or @sub = 'e' or @sub = 'f' or @sub = 'o' or @sub = 'i']"><xsl:text>,</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'g']"><xsl:text>;</xsl:text></xsl:when></xsl:choose></subfield>
                </xsl:if>
                <xsl:if test="@sub = 'd'">
                    <subfield code="l"><xsl:value-of select="."/><xsl:choose><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'd' or @sub = 'h' or @sub = 'j' or @sub = 'c']"><xsl:text>.</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'n' or @sub = 'e' or @sub = 'f' or @sub = 'o' or @sub = 'i']"><xsl:text>,</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'g']"><xsl:text>;</xsl:text></xsl:when></xsl:choose></subfield>
                </xsl:if>
                <xsl:if test="@sub = 'h'">
                    <subfield code="p"><xsl:value-of select="."/><xsl:choose><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'd' or @sub = 'j' or @sub = 'c']"><xsl:text>.</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'n' or @sub = 'e' or @sub = 'f' or @sub = 'h' or @sub = 'o' or @sub = 'i']"><xsl:text>,</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'g']"><xsl:text>;</xsl:text></xsl:when></xsl:choose></subfield>
                </xsl:if>
                <xsl:if test="@sub = 'n' or @sub = 'f' or @sub = 'o'">
                    <subfield code="n"><xsl:value-of select="."/><xsl:choose><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'd' or @sub = 'h' or @sub = 'j' or @sub = 'c']"><xsl:text>.</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'n' or @sub = 'e' or @sub = 'f' or @sub = 'o' or @sub = 'i']"><xsl:text>,</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'g']"><xsl:text>;</xsl:text></xsl:when></xsl:choose></subfield>
                </xsl:if>
                <xsl:if test="@sub = 'e'">
                    <subfield code="m"><xsl:value-of select="."/><xsl:choose><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'd' or @sub = 'h' or @sub = 'j' or @sub = 'c']"><xsl:text>.</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'n' or @sub = 'e' or @sub = 'f' or @sub = 'o' or @sub = 'i']"><xsl:text>,</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'g']"><xsl:text>;</xsl:text></xsl:when></xsl:choose></subfield>
                </xsl:if>
                <xsl:if test="@sub = 'i'">
                    <subfield code="r"><xsl:value-of select="."/><xsl:choose><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'd' or @sub = 'h' or @sub = 'j' or @sub = 'c']"><xsl:text>.</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'n' or @sub = 'e' or @sub = 'f' or @sub = 'o' or @sub = 'i']"><xsl:text>,</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'g']"><xsl:text>;</xsl:text></xsl:when></xsl:choose></subfield>
                </xsl:if>
                <xsl:if test="@sub = 'j'">
                    <subfield code="s"><xsl:value-of select="."/><xsl:choose><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'd' or @sub = 'h' or @sub = 'j' or @sub = 'c']"><xsl:text>.</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'n' or @sub = 'e' or @sub = 'f' or @sub = 'o' or @sub = 'i']"><xsl:text>,</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'g']"><xsl:text>;</xsl:text></xsl:when></xsl:choose></subfield>
                </xsl:if>
                <xsl:if test="@sub = 'c'">
                    <subfield code="k"><xsl:value-of select="."/><xsl:choose><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'd' or @sub = 'h' or @sub = 'j' or @sub = 'c']"><xsl:text>.</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'n' or @sub = 'e' or @sub = 'f' or @sub = 'o' or @sub = 'i']"><xsl:text>,</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'g']"><xsl:text>;</xsl:text></xsl:when></xsl:choose></subfield>
                </xsl:if>
                <xsl:if test="@sub = 'g'">
                    <subfield code="o"><xsl:value-of select="."/><xsl:choose><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'd' or @sub = 'h' or @sub = 'j' or @sub = 'c']"><xsl:text>.</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'n' or @sub = 'e' or @sub = 'f' or @sub = 'o' or @sub = 'i']"><xsl:text>,</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[@sub != 'k'][1][@sub = 'g']"><xsl:text>;</xsl:text></xsl:when></xsl:choose></subfield>
                </xsl:if>
            </xsl:for-each>
        </datafield>
    </xsl:template>

<!-- ***** Annan titel, tag 009 -->
    <xsl:template match="smdb:part[@type = 'kp']/smdb:field[@id = '009' and @ind != 'o']">
        <xsl:variable name="ind2"><xsl:choose><xsl:when test="@ind = 'k'">4</xsl:when><xsl:when test="@ind = 'p'">1</xsl:when><xsl:when test="@ind = 'r'">8</xsl:when><xsl:when test="@ind = 't'">2</xsl:when><xsl:when test="@ind = 'ö'">3</xsl:when><xsl:otherwise><xsl:text> </xsl:text></xsl:otherwise></xsl:choose></xsl:variable>
        <datafield ind1="1" ind2="{$ind2}" tag="246">
            <xsl:if test="@ind = 's'"><subfield code="i"><xsl:text>Svensk titel: </xsl:text></subfield></xsl:if>
            <xsl:for-each select="smdb:value[@sub]">
                <xsl:if test="@sub = 'a'">
                    <subfield code="a"><xsl:value-of select="."/><xsl:choose><xsl:when test="following-sibling::smdb:value[1][@sub = 'b']"><xsl:text> :</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[1][@sub = 'c' or @sub = 'd']"><xsl:text>.</xsl:text></xsl:when></xsl:choose></subfield>
                </xsl:if>
                <xsl:if test="@sub = 'b'">
                    <subfield code="b"><xsl:value-of select="."/><xsl:choose><xsl:when test="following-sibling::smdb:value[1][@sub = 'b']"><xsl:text> :</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[1][@sub = 'c' or @sub = 'd']"><xsl:text>.</xsl:text></xsl:when></xsl:choose></subfield>
                </xsl:if>
                <xsl:if test="@sub = 'c'">
                    <subfield code="n"><xsl:value-of select="."/><xsl:choose><xsl:when test="following-sibling::smdb:value[1][@sub = 'b']"><xsl:text> :</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[1][@sub = 'c']"><xsl:text>.</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[1][@sub = 'd']"><xsl:text>,</xsl:text></xsl:when></xsl:choose></subfield>
                </xsl:if>
                <xsl:if test="@sub = 'd'">
                    <subfield code="p"><xsl:value-of select="."/><xsl:choose><xsl:when test="following-sibling::smdb:value[1][@sub = 'b']"><xsl:text> :</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[1][@sub = 'c' or @sub = 'd']"><xsl:text>.</xsl:text></xsl:when></xsl:choose></subfield>
                </xsl:if>
            </xsl:for-each>
        </datafield>
    </xsl:template>

    <!-- ***** Uniform titel om 010 saknas, tag 009 -->
    <xsl:template match="smdb:part[@type = 'kp']/smdb:field[@id = '009' and @ind = 'o']">
        <xsl:param name="df007"/>
        <xsl:variable name="tag"><xsl:call-template name="get-unitag"><xsl:with-param name="df007" select="$df007"/></xsl:call-template></xsl:variable>
        <xsl:variable name="ind1"><xsl:choose><xsl:when test="$tag = '240'">1</xsl:when><xsl:otherwise>0</xsl:otherwise></xsl:choose></xsl:variable>
        <xsl:variable name="ind2"><xsl:choose><xsl:when test="smdb:value[@sub = 'a' and @ind]"><xsl:value-of select="smdb:value[@sub = 'a' and @ind]/@ind"/></xsl:when><xsl:otherwise>0</xsl:otherwise></xsl:choose></xsl:variable>
        <datafield ind1="{$ind1}" ind2="{$ind2}" tag="{$tag}">
            <xsl:if test="@ind = 's'"><subfield code="i"><xsl:text>Svensk titel: </xsl:text></subfield></xsl:if>
            <xsl:for-each select="smdb:value[@sub]">
                <xsl:if test="@sub = 'a'">
                    <subfield code="a"><xsl:value-of select="."/><xsl:choose><xsl:when test="following-sibling::smdb:value[1][@sub = 'b']"><xsl:text> :</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[1][@sub = 'c' or @sub = 'd']"><xsl:text>.</xsl:text></xsl:when></xsl:choose></subfield>
                </xsl:if>
                <xsl:if test="@sub = 'b'">
                    <subfield code="b"><xsl:value-of select="."/><xsl:choose><xsl:when test="following-sibling::smdb:value[1][@sub = 'b']"><xsl:text> :</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[1][@sub = 'c' or @sub = 'd']"><xsl:text>.</xsl:text></xsl:when></xsl:choose></subfield>
                </xsl:if>
                <xsl:if test="@sub = 'c'">
                    <subfield code="n"><xsl:value-of select="."/><xsl:choose><xsl:when test="following-sibling::smdb:value[1][@sub = 'b']"><xsl:text> :</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[1][@sub = 'c']"><xsl:text>.</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[1][@sub = 'd']"><xsl:text>,</xsl:text></xsl:when></xsl:choose></subfield>
                </xsl:if>
                <xsl:if test="@sub = 'd'">
                    <subfield code="p"><xsl:value-of select="."/><xsl:choose><xsl:when test="following-sibling::smdb:value[1][@sub = 'b']"><xsl:text> :</xsl:text></xsl:when><xsl:when test="following-sibling::smdb:value[1][@sub = 'c' or @sub = 'd']"><xsl:text>.</xsl:text></xsl:when></xsl:choose></subfield>
                </xsl:if>
            </xsl:for-each>
        </datafield>
    </xsl:template>

    <!-- ***** Serie/Box, tag 011 -->
    <xsl:template match="smdb:part[@type = 'kp']/smdb:field[@id = '011']">
        <xsl:choose>
            <xsl:when test="@ind = 'a'">
                <datafield ind1="0" ind2=" " tag="773">
                    <xsl:for-each select="smdb:value[@sub]">
                        <xsl:if test="@sub='a'"><subfield code="t"><xsl:value-of select="translate(., '*', '')"/></subfield></xsl:if>
                        <xsl:if test="@sub='c'"><subfield code="x"><xsl:value-of select="."/></subfield></xsl:if>
                        <xsl:if test="@sub='d'"><subfield code="g"><xsl:value-of select="."/></subfield></xsl:if>
                        <xsl:if test="@sub='e'"><subfield code="g"><xsl:value-of select="."/></subfield></xsl:if>
                        <xsl:if test="@sub='f'"><subfield code="n"><xsl:value-of select="."/></subfield></xsl:if>
                    </xsl:for-each>
                </datafield>
            </xsl:when>
            <xsl:when test="@ind = 'b'">
                <datafield ind1="0" ind2="8" tag="787">
                    <subfield code="i"><xsl:text>Utgiven i box: </xsl:text></subfield>
                    <xsl:for-each select="smdb:value[@sub]">
                        <xsl:if test="@sub='a'"><subfield code="t"><xsl:value-of select="translate(., '*', '')"/></subfield></xsl:if>
                        <xsl:if test="@sub='d'"><subfield code="d"><xsl:value-of select="."/></subfield></xsl:if>
                        <xsl:if test="@sub='e'"><subfield code="h"><xsl:value-of select="."/></subfield></xsl:if>
                        <xsl:if test="@sub='f'"><subfield code="n"><xsl:value-of select="."/></subfield></xsl:if>
                    </xsl:for-each>
                </datafield>
            </xsl:when>
            <xsl:otherwise>
                <datafield ind1="0" ind2=" " tag="490">
                    <xsl:for-each select="smdb:value[@sub]">
                        <xsl:if test="@sub='a'"><subfield code="a"><xsl:value-of select="translate(., '*', '')"/><xsl:if test="following-sibling::smdb:value[1][@sub = 'b']">;</xsl:if><xsl:if test="following-sibling::smdb:value[1][@sub = 'c']">,</xsl:if></subfield></xsl:if>
                        <xsl:if test="@sub='b'"><subfield code="v"><xsl:value-of select="."/><xsl:if test="following-sibling::smdb:value[1][@sub = 'b']">;</xsl:if><xsl:if test="following-sibling::smdb:value[1][@sub = 'c']">,</xsl:if></subfield></xsl:if>
                        <xsl:if test="@sub='c'"><subfield code="x"><xsl:value-of select="."/><xsl:if test="following-sibling::smdb:value[1][@sub = 'b']">;</xsl:if><xsl:if test="following-sibling::smdb:value[1][@sub = 'c']">,</xsl:if></subfield></xsl:if>
                    </xsl:for-each>
                </datafield>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <!-- ***** Person, tag 012 -->
    <xsl:template match="smdb:part[@type = 'kp']/smdb:field[@id = '012']">
        <xsl:param name="df007"/>
        <xsl:variable name="ind1"><xsl:call-template name="nameform"/></xsl:variable>
        <xsl:variable name="tag">
            <xsl:choose>
                <xsl:when test="position() = 1 and contains($df007, 'namn')">100</xsl:when>
                <xsl:otherwise>700</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <datafield ind1="{$ind1}" ind2=" " tag="{$tag}">
            <xsl:for-each select="smdb:value[@sub = 'a' or @sub = 'b' or @sub = 'c' or @sub = 'd']">
                <xsl:if test="@sub = 'a'">
                    <subfield code="a"><xsl:value-of select="."/><xsl:if test="following-sibling::smdb:value[1][@sub = 'b' or @sub = 'c']">,</xsl:if></subfield>
                </xsl:if>
                <xsl:if test="@sub = 'b'">
                    <subfield code="c"><xsl:value-of select="."/><xsl:if test="following-sibling::smdb:value[1][@sub = 'b' or @sub = 'c']">,</xsl:if></subfield>
                </xsl:if>
                <xsl:if test="@sub = 'c'">
                    <subfield code="d"><xsl:value-of select="."/><xsl:if test="following-sibling::smdb:value[1][@sub = 'b' or @sub = 'c']">,</xsl:if></subfield>
                </xsl:if>
                <xsl:if test="@sub = 'd'">
                    <subfield code="4"><xsl:value-of select="."/></subfield>
                </xsl:if>
            </xsl:for-each>
        </datafield>
    </xsl:template>

    <!-- ***** Institution, tag 013 -->
    <xsl:template match="smdb:part[@type = 'kp']/smdb:field[@id = '013']">
        <xsl:param name="df007"/>
        <xsl:variable name="tag">
            <xsl:choose>
                <xsl:when test="not($df007)">700</xsl:when>
                <xsl:when test="position() = 1 and contains($df007, 'grupp')">110</xsl:when>
                <xsl:otherwise>710</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>

        <datafield ind1="2" ind2=" " tag="{$tag}">
            <xsl:for-each select="smdb:value[@sub = 'a' or @sub = 'b' or @sub = 'd']">
                <xsl:if test="@sub = 'a' or @sub = 'b'">
                    <subfield code="{@sub}"><xsl:value-of select="."/><xsl:if test="following-sibling::smdb:value[1][@sub = 'b']">.</xsl:if></subfield>
                </xsl:if>
                <xsl:if test="@sub = 'd'">
                    <subfield code="4"><xsl:value-of select="."/></subfield>
                </xsl:if>
            </xsl:for-each>
        </datafield>
    </xsl:template>

    <!-- ***** Upplaga, tag 014 -->
    <xsl:template match="smdb:part[@type = 'kp']/smdb:field[@id = '014']">
        <datafield ind1=" " ind2=" " tag="250">
            <subfield code="a"><xsl:value-of select="smdb:value"/></subfield>
        </datafield>
    </xsl:template>

    <!-- ***** Utgivning/år/anmärkning, tag 015, 016 -->
    <xsl:template match="smdb:part[@type = 'kp']/smdb:field[@id = '015']">
        <xsl:for-each select="smdb:value[@sub = 'a' or @sub = 'b']">
                <subfield code="{@sub}"><xsl:value-of select="."/><xsl:if test="following-sibling::smdb:value[1][@sub = 'a']"><xsl:text> ;</xsl:text></xsl:if><xsl:if test="following-sibling::smdb:value[1][@sub = 'b']"><xsl:text> :</xsl:text></xsl:if>
                    <xsl:if test="position() = last() and ../../smdb:field[@id = '016' and smdb:value[@sub = 'a']]"><xsl:text> ,</xsl:text></xsl:if>
                </subfield>
                <!--<subfield code="{@sub}"><xsl:value-of select="."/><xsl:if test="following-sibling::smdb:value[1][@sub = 'a']"><xsl:text> ;</xsl:text></xsl:if><xsl:if test="following-sibling::smdb:value[1][@sub = 'b']"><xsl:text> :</xsl:text></xsl:if><xsl:if test="position() = last() and number($count016) >= number($pos015)">,</xsl:if></subfield>-->
            </xsl:for-each>
        <xsl:if test="position() = last() and ../smdb:field[@id = '016' and smdb:value[@sub = 'a']]">
            <subfield code="c"><xsl:value-of select="../smdb:field[@id = '016' and smdb:value[@sub = 'a']][1]/smdb:value[@sub = 'a']"/></subfield>
            </xsl:if>
        <!--<datafield ind1=" " ind2=" " tag="{$tag}" countdf016a="{$count016}" bppos="{$bppos}">
            <xsl:choose>
                <xsl:when test="$tag = '500'">
                    <subfield code="a"><xsl:text>Även utgiven: </xsl:text><xsl:for-each select="smdb:value[@sub = 'a' or @sub = 'b']"><xsl:value-of select="."/><xsl:if test="following-sibling::smdb:value[1][@sub = 'a']"><xsl:text> : </xsl:text></xsl:if><xsl:if test="following-sibling::smdb:value[1][@sub = 'b']"><xsl:text> : </xsl:text></xsl:if><xsl:if test="position() = last() and ../../smdb:field[@id = '016' and smdb:value[@sub = 'a']][number($pos015)]"><xsl:text>, </xsl:text></xsl:if></xsl:for-each><xsl:if test="../smdb:field[@id = '016' and smdb:value[@sub = 'a']][number($pos015)]"><xsl:value-of select="../smdb:field[@id = '016'][number($pos015)]/smdb:value[@sub = 'a']"/></xsl:if></subfield>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:for-each select="smdb:value[@sub = 'a' or @sub = 'b']">
                    <subfield code="{@sub}"><xsl:value-of select="."/><xsl:if test="following-sibling::smdb:value[1][@sub = 'a']"><xsl:text> ;</xsl:text></xsl:if><xsl:if test="following-sibling::smdb:value[1][@sub = 'b']"><xsl:text> :</xsl:text></xsl:if></subfield>
                        <subfield code="{@sub}"><xsl:value-of select="."/><xsl:if test="following-sibling::smdb:value[1][@sub = 'a']"><xsl:text> ;</xsl:text></xsl:if><xsl:if test="following-sibling::smdb:value[1][@sub = 'b']"><xsl:text> :</xsl:text></xsl:if><xsl:if test="position() = last() and number($count016) >= number($pos015)">,</xsl:if></subfield>
                    </xsl:for-each>
                    <xsl:if test="number($bppos) = 1 and ../smdb:field[@id = '016' and smdb:value[@sub = 'a']][number($pos015)]">
                        <subfield code="c"><xsl:value-of select="../smdb:field[@id = '016'][number($pos015)]/smdb:value[@sub = 'a']"/></subfield>
                    </xsl:if>
                    <xsl:if test="number($bppos) > 1 and ../smdb:field[@id = '016' and smdb:value[@sub = 'a']][number($pos015)]">
                        <subfield code="c"><xsl:value-of select="../smdb:field[@id = '016'][number($pos015)]/smdb:value[@sub = 'a']"/></subfield>
                    </xsl:if>
                </xsl:otherwise>
            </xsl:choose>-->
        
    </xsl:template>

    <!-- ***** Inspelningsummer (tag 023) -->
    <xsl:template match="smdb:part[@type = 'kp']/smdb:field[@id = '023']">
        <xsl:if test="@ind = 'a'">
            <datafield ind1="7" ind2=" " tag="024">
                <subfield code="a"><xsl:value-of select="smdb:value"/></subfield>
                <subfield code="2">isan</subfield>
            </datafield>
        </xsl:if>
        <xsl:if test="@ind = 'm'">
            <datafield ind1="1" ind2="1" tag="028">
                <subfield code="a"><xsl:value-of select="smdb:value"/></subfield>
            </datafield>
        </xsl:if>
        <xsl:if test="@ind = 'p'">
            <datafield ind1="0" ind2="1" tag="028">
                <subfield code="a"><xsl:value-of select="smdb:value"/></subfield>
            </datafield>
        </xsl:if>
        <xsl:if test="@ind = 'r'">
            <datafield ind1="0" ind2=" " tag="024">
                <subfield code="a"><xsl:value-of select="smdb:value"/></subfield>
            </datafield>
        </xsl:if>
    </xsl:template>

    <!-- ***** Anmärkning, tag 024 -->
    <xsl:template match="smdb:part[@type = 'kp']/smdb:field[@id = '024']">
        <datafield ind1=" " ind2=" " tag="500">
            <subfield code="a"><xsl:value-of select="smdb:value"/></subfield>
        </datafield>
    </xsl:template>
    
    <!-- ***** Anmärkning, tag 029 -->
    <xsl:template match="smdb:part[@type = 'kp']/smdb:field[@id = '029' and not(smdb:value = 'Suecana')]">
        <datafield ind1=" " ind2=" " tag="500">
            <subfield code="a"><xsl:value-of select="smdb:value"/></subfield>
        </datafield>
    </xsl:template>

    <!-- ***** Innehåll, tag 025 -->
    <xsl:template match="smdb:part[@type = 'kp']/smdb:field[@id = '025']">
        <datafield ind1="8" ind2=" " tag="505">
            <subfield code="a"><xsl:value-of select="smdb:value"/></subfield>
        </datafield>
    </xsl:template>

    <!-- ***** Sammanfattning, tag 026 -->
    <xsl:template match="smdb:part[@type = 'kp']/smdb:field[@id = '026']">
        <datafield ind1=" " ind2=" " tag="520">
            <subfield code="a"><xsl:value-of select="smdb:value"/></subfield>
        </datafield>
    </xsl:template>

    <!-- ***** Språk, tag 038 -->
    <xsl:template match="smdb:part[@type = 'kp']/smdb:field[@id = '038']">
        <subfield code="a"><xsl:value-of select="smdb:value"/></subfield>
    </xsl:template>

    <!-- ***** SAB, tag 040 -->
    <xsl:template match="smdb:part[@type = 'kp']/smdb:field[@id = '040']">
        <datafield ind1=" " ind2=" " tag="084">
            <subfield code="a"><xsl:value-of select="smdb:value"/></subfield>
            <subfield code="2">kssb/8</subfield>
        </datafield>
    </xsl:template>

    <!-- ***** Genre, tag 042-->
    <xsl:template match="smdb:part[@type = 'kp']/smdb:field[@id = '042']">
        <xsl:variable name="versal" select="translate(substring(normalize-space(smdb:value), 1, 1), 'abcdefghijklmnopqrstuvwxyzåäö', 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ')"/>
        <xsl:variable name="gemener" select="substring(normalize-space(smdb:value), 2)"/>
        <datafield ind1=" " ind2="4" tag="655">
            <subfield code="a"><xsl:value-of select="concat($versal, $gemener)"/></subfield>
        </datafield>
    </xsl:template>

    <!-- ***** Publikationstyp, tag 043 -->
    <xsl:template match="smdb:part[@type = 'kp']/smdb:field[@id = '043']">
        <xsl:variable name="versal" select="translate(substring(normalize-space(smdb:value), 1, 1), 'abcdefghijklmnopqrstuvwxyzåäö', 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ')"/>
        <xsl:variable name="gemener" select="substring(normalize-space(smdb:value), 2)"/>
        <!--<datafield ind1=" " ind2=" " tag="500">
            <subfield code="a"><xsl:value-of select="concat($versal, $gemener)"/></subfield>
        </datafield>-->
        <xsl:if test=" $gemener = 'judbok'">
            <datafield ind1=" " ind2="7" tag="655">
                <subfield code="a">Ljudböcker</subfield>
                <subfield code="2">saogf</subfield>
            </datafield>
        </xsl:if>
    </xsl:template>

    <!-- ***** Arkivnummer, tag 130 -->
    <xsl:template match="smdb:part[@type = 'kp']/smdb:field[@id = '130']">
        <datafield ind1="7" ind2=" " tag="024">
            <subfield code="a"><xsl:value-of select="smdb:value[@sub = 'a']"/></subfield>
            <subfield code="2">Arkivnummer</subfield>
        </datafield>
    </xsl:template>
    
    <!-- DP -->
    <!-- ***** Inspelningsnummer, DP tag 023 -->
    <xsl:template match="smdb:part[@type = 'dp']/smdb:field[@id = '023']">
        <xsl:if test="@ind = 'a'">
            <datafield ind1="7" ind2=" " tag="024">
                <subfield code="a"><xsl:value-of select="smdb:value"/><xsl:if test="not(contains(smdb:value, '(del)'))"><xsl:text> (del)</xsl:text></xsl:if></subfield>
                <subfield code="2">isan</subfield>
            </datafield>
        </xsl:if>
        <xsl:if test="@ind = 'm'">
            <datafield ind1="1" ind2="1" tag="028">
                <subfield code="a"><xsl:value-of select="smdb:value"/><xsl:if test="not(contains(smdb:value, '(del)'))"><xsl:text> (del)</xsl:text></xsl:if></subfield>
            </datafield>
        </xsl:if>
        <xsl:if test="@ind = 'r'">
            <datafield ind1="0" ind2=" " tag="024">
                <subfield code="a"><xsl:value-of select="smdb:value"/><xsl:if test="not(contains(smdb:value, '(del)'))"><xsl:text> (del)</xsl:text></xsl:if></subfield>
            </datafield>
        </xsl:if>
    </xsl:template>

    <!-- ***** Arkivnummer, DP tag 130 -->
    <xsl:template match="smdb:part[@type = 'dp']/smdb:field[@id = '130']">
        <datafield ind1="7" ind2=" " tag="024">
            <subfield code="a"><xsl:value-of select="smdb:value[@sub = 'a']"/><xsl:if test="not(contains(smdb:value, '(del)'))"><xsl:text> (del)</xsl:text></xsl:if></subfield>
            <subfield code="2">Arkivnummer</subfield>
        </datafield>
    </xsl:template>

    <!-- ***** Övrigt, DP tag 050, 010, 024, 012, 013 -->
    <xsl:template match="smdb:part[@type = 'dp']">
        <xsl:variable name="ind1"><xsl:choose><xsl:when test="@index = '1'">0</xsl:when><xsl:otherwise>8</xsl:otherwise></xsl:choose></xsl:variable>
        <datafield ind1="{$ind1}" ind2="0" tag="505">
            <xsl:apply-templates select="smdb:field[@id = '050']" mode="build505"/>
            <xsl:apply-templates select="smdb:field[@id = '010']" mode="build505"/>
            <xsl:apply-templates select="smdb:field[@id = '024' and smdb:value]" mode="build505"/>
            <xsl:apply-templates select="smdb:field[@id = '012']" mode="build505"/>
            <xsl:apply-templates select="smdb:field[@id = '013']" mode="build505"/>
        </datafield>
    </xsl:template>

    <xsl:template match="smdb:field[@id = '050']" mode="build505">
        <subfield code="t">
            <xsl:for-each select="smdb:value[@sub = 'a' or @sub = 'b' or @sub = 'd' or @sub = 'e']">
                <xsl:value-of select="."/>
                <xsl:if test="following-sibling::smdb:value[1][@sub = 'a' or @sub = 'd' or (@sub = 'e' and not(../smdb:value[@sub = 'd']))]"><xsl:text>. </xsl:text></xsl:if>
                <xsl:if test="following-sibling::smdb:value[1][@sub = 'b']"><xsl:text> : </xsl:text></xsl:if>
                <xsl:if test="following-sibling::smdb:value[1][@sub = 'c']"><xsl:text> /</xsl:text></xsl:if>
                <xsl:if test="following-sibling::smdb:value[1][@sub = 'e'] and ../smdb:value[@sub = 'd']"><xsl:text>, </xsl:text></xsl:if>
            </xsl:for-each>
            <xsl:if test="not(smdb:value[@sub = 'c'])">.</xsl:if>
        </subfield>
        <xsl:for-each select="smdb:value[@sub = 'c']">
            <subfield code="r"><xsl:value-of select="."/>.</subfield>
        </xsl:for-each>
    </xsl:template>

    <xsl:template match="smdb:field[@id = '010']" mode="build505">
        <subfield code="t">
            <xsl:for-each select="smdb:value[@sub = 'a' or @sub = 'd' or @sub = 'h' or @sub = 'n' or @sub = 'e' or @sub = 'f' or @sub = 'o' or @sub = 'i' or @sub = 'j' or @sub = 'k']">
                <xsl:value-of select="."/>
                <xsl:if test="following-sibling::smdb:value[1][@sub = 'a' or @sub = 'd' or @sub = 'j']"><xsl:text>. </xsl:text></xsl:if>
                <xsl:if test="following-sibling::smdb:value[1][@sub = 'h' or @sub = 'n' or @sub = 'e' or @sub = 'f' or @sub = 'o' or @sub = 'i' or @sub = 'k']"><xsl:text>. </xsl:text></xsl:if>
                <xsl:if test="following-sibling::smdb:value[1][@sub = 'c']"><xsl:text>.</xsl:text></xsl:if>
                <xsl:if test="following-sibling::smdb:value[1][@sub = 'g']"><xsl:text> ; </xsl:text></xsl:if>
            </xsl:for-each>
            <xsl:if test="not(smdb:value[@sub = 'c' or @sub = 'g'])">.</xsl:if>
        </subfield>
        <xsl:for-each select="smdb:value[@sub = 'c']">
            <subfield code="k"><xsl:value-of select="."/><xsl:choose><xsl:when test="following-sibling::smdb:value[1][@sub = 'g']"><xsl:text> ; </xsl:text></xsl:when><xsl:otherwise>.</xsl:otherwise></xsl:choose></subfield>
        </xsl:for-each>
        <xsl:for-each select="smdb:value[@sub = 'g']">
            <subfield code="o"><xsl:value-of select="."/><xsl:choose><xsl:when test="following-sibling::smdb:value[1][@sub = 'g']"><xsl:text> ; </xsl:text></xsl:when><xsl:otherwise>.</xsl:otherwise></xsl:choose></subfield>
        </xsl:for-each>
    </xsl:template>

    <xsl:template match="smdb:field[@id = '024']" mode="build505">
        <subfield code="g">(<xsl:value-of select="smdb:value"/>).</subfield>
    </xsl:template>

    <xsl:template match="smdb:field[@id = '012']" mode="build505">
        <subfield code="r"><xsl:for-each select="smdb:value[@sub = 'a' or @sub = 'b' or @sub = 'c' or @sub = 'd']"><xsl:choose><xsl:when test="@sub != 'd'"><xsl:value-of select="."/><xsl:if test="following-sibling::smdb:value[1][@sub = 'b' or @sub = 'c']"><xsl:text>, </xsl:text></xsl:if></xsl:when><xsl:otherwise><xsl:text> (</xsl:text><xsl:value-of select="."/>)</xsl:otherwise></xsl:choose></xsl:for-each>.</subfield>
    </xsl:template>

    <xsl:template match="smdb:field[@id = '013']" mode="build505">
        <subfield code="r"><xsl:for-each select="smdb:value[@sub = 'a' or @sub = 'b' or @sub = 'd']"><xsl:choose><xsl:when test="@sub != 'd'"><xsl:value-of select="."/><xsl:if test="following-sibling::smdb:value[1][@sub = 'b']"><xsl:text>, </xsl:text></xsl:if></xsl:when><xsl:otherwise><xsl:text> (</xsl:text><xsl:value-of select="."/>)</xsl:otherwise></xsl:choose></xsl:for-each>.</subfield>
    </xsl:template>

    <!-- BP -->
    <!-- ***** Denna utgåva, tag 016 -->
    <xsl:template match="smdb:part[@type = 'bp']/smdb:field[@id = '016']">
        <datafield ind1=" " ind2=" " tag="500">
            <subfield code = "a"><xsl:value-of select="smdb:value[@sub = 'a']"/><xsl:text> (denna utgåva)</xsl:text></subfield>
        </datafield>
    </xsl:template>
    <!-- ***** Utgivningsnummer marc tag 020, tag 077 -->
    <xsl:template match="smdb:part[@type = 'bp']/smdb:field[@id = '077' and @ind = 'i']">
        <xsl:for-each select="smdb:value[@sub = 'b']">
            <datafield ind1=" " ind2=" " tag="020">
                <subfield code="a"><xsl:value-of select="normalize-space(translate(., ' -', ''))"/></subfield>
            </datafield>
        </xsl:for-each>
    </xsl:template>
    
    <!-- ***** Utgivningsnummer marc tag 028, tag 077 -->
    <xsl:template match="smdb:part[@type = 'bp']/smdb:field[@id = '077' and (@ind = 'm' or @ind = 'v')]">
        <xsl:variable name="ind1">
            <xsl:if test="@ind = 'm'">5</xsl:if>
            <xsl:if test="@ind = 'v'">4</xsl:if>
        </xsl:variable>

        <xsl:for-each select="smdb:value[@sub = 'b']">
            <datafield ind1="{$ind1}" ind2="1" tag="028">
                <subfield code="a"><xsl:value-of select="."/></subfield>
            </datafield>
        </xsl:for-each>
    </xsl:template>
    
    <!-- ***** Utgivningsnummer marc tag 028, tag 077 -->
    <xsl:template match="smdb:part[@type = 'bp']/smdb:field[@id = '077' and @ind = 's']">
        <xsl:for-each select="smdb:value[@sub = 'b']">
            <datafield ind1="0" ind2="1" tag="028">
                <subfield code="a"><xsl:value-of select="."/><xsl:if test="../smdb:value[@sub = 'a']"><xsl:text> :</xsl:text></xsl:if></subfield>
                <xsl:if test="../smdb:value[@sub = 'a']">
                    <subfield code="b"><xsl:for-each select="../smdb:value[@sub = 'a']"><xsl:value-of select="."/><xsl:if test="position() != last()"><xsl:text>, </xsl:text></xsl:if></xsl:for-each></subfield>
                </xsl:if>
            </datafield>
        </xsl:for-each>
    </xsl:template>

    <!-- ***** Formatbärare, tag 090 -->
    <xsl:template match="smdb:part[@type = 'bp']/smdb:field[@id = '090']">
        <datafield ind1=" " ind2=" " tag="300">
            <subfield code = "a"><xsl:if test="../smdb:field[@id = '089' and smdb:value[@sub = 'a']]"><xsl:value-of select="../smdb:field[@id = '089' and smdb:value[@sub = 'a']][1]/smdb:value[@sub = 'a']"/><xsl:text> </xsl:text></xsl:if><xsl:value-of select="smdb:value[@sub = 'e']"/><xsl:if test="normalize-space(smdb:value[@sub = 'b']) = 'skiva' and ../smdb:field[@id='097' and smdb:value]"><xsl:text> (</xsl:text><xsl:value-of select="../smdb:field[@id = '097' and smdb:value][1]/smdb:value"/>)</xsl:if><xsl:if test="../../smdb:part[@type='kp']/smdb:field[@id = '039' and smdb:value]"><xsl:text> (</xsl:text><xsl:value-of select="../../smdb:part[@type='kp']/smdb:field[@id = '039' and smdb:value][1]/smdb:value"/>)</xsl:if><xsl:if test="../smdb:field[@id = '093' and smdb:value]"><xsl:text> :</xsl:text></xsl:if></subfield>
            <xsl:if test="../smdb:field[@id = '093' and smdb:value]"><subfield code = "b"><xsl:value-of select="../smdb:field[@id = '093' and smdb:value][1]/smdb:value"/></subfield></xsl:if>
        </datafield>
    </xsl:template>
    
    <!-- ***** Manifestationsspecifika anmärkningar, tag 110, 111 -->
    <xsl:template match="smdb:part[@type = 'bp']/smdb:field[@id = '110' or @id = '111']">
        <xsl:variable name="versal" select="translate(substring(normalize-space(smdb:value), 1, 1), 'abcdefghijklmnopqrstuvwxyzåäö', 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ')"/>
        <xsl:variable name="gemener" select="substring(normalize-space(smdb:value), 2)"/>
        <!--
            <xsl:variable name="versal" select="translate(substring(normalize-space(smdb:value), 1, 1), 'abcdefghijklmnopqrstuvwxyzåäö', 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ')"/>
            <xsl:variable name="gemener" select="substring(normalize-space(smdb:value), 2)"/>
            <datafield ind1=" " ind2="4" tag="655">
            <subfield code="a"><xsl:value-of select="concat($versal, $gemener)"/></subfield>
            </datafield>
        -->
        <datafield ind1=" " ind2=" " tag="500">
            <subfield code="a"><xsl:value-of select="concat($versal, $gemener)"/></subfield>
        </datafield>
    </xsl:template>

    <!-- SYS -->
    <!-- ***** Senast ändrad, SYS tag 126 -->
    <xsl:template match="smdb:part[@type = 'sys']/smdb:field[@id = '126']">
        <datafield ind1=" " ind2=" " tag="599">
            <subfield code="a"><xsl:text>Posten ändrad i SMDB </xsl:text><xsl:value-of select="smdb:value"/></subfield>
        </datafield>
    </xsl:template>

    <!-- ***** Post-ID, SYS tag 901 -->
    <xsl:template match="smdb:part[@type = 'sys']/smdb:field[@id = '901']">
        <xsl:param name="pos"/>
        <datafield ind1=" " ind2=" " tag="035">
            <subfield code="a">AVM<xsl:value-of select="smdb:value"/><xsl:if
                            test="number($pos) > 1">bp<xsl:value-of select="$pos"/></xsl:if></subfield>
        </datafield>
        <datafield ind1="4" ind2="2" tag="856">
            <subfield code="u"><xsl:value-of select="concat('http://smdb.kb.se/resource/id/', normalize-space(smdb:value))"/></subfield>
            <subfield code="z">Svensk mediedatabas</subfield>
        </datafield>
    </xsl:template>

    <!-- ***** HOLDINGS, förvärv (BP tag 082) -->
    <xsl:template match="smdb:part[@type = 'bp']/smdb:field[@id = '082']">
        <datafield ind1=" " ind2=" " tag="541">
            <xsl:for-each select="smdb:value[@sub = 'a']">
                <subfield code="a"><xsl:value-of select="."/></subfield>
            </xsl:for-each>
            <xsl:for-each select="smdb:value[@sub = 'b']">
                <subfield code="d"><xsl:value-of select="."/></subfield>
            </xsl:for-each>
            <xsl:for-each select="smdb:value[@sub = 'c']">
                <subfield code="c"><xsl:value-of select="."/></subfield>
            </xsl:for-each>
        </datafield>
    </xsl:template>

    <!-- ***** HOLDINGS, fulltext-URL, SYS tag 901 -->
    <xsl:template match="smdb:part[@type = 'sys']/smdb:field[@id = '901']" mode="holdings">
        <datafield ind1="4" ind2="0" tag="856">
            <!--<xsl:if test="../../smdb:part[@type = 'bp' and smdb:field[@id = '090' and smdb:value[. = 'fil']]]">-->
                <subfield code="u"><xsl:value-of select="concat('http://smdb.kb.se/catalog/id/', normalize-space(smdb:value), '/play')"/></subfield>
            <!--</xsl:if>-->
            <subfield code="z">Tillgänglig på KB för inloggade forskare</subfield>
        </datafield>
    </xsl:template>

    <!-- ***** UTILITY TEMPLATES -->
        <!-- ***** Returnerar vilket av datafält 130/240/730 som ska genereras utifrån värdet i 007 -->
    <xsl:template name="get-unitag">
        <xsl:param name="df007"/>
        <xsl:choose>
            <xsl:when test="$df007 = 'uniform titel' and position() = 1">130</xsl:when>
            <xsl:when test="(contains($df007, 'namn') or contains($df007, 'grupp')) and position() = 1">240</xsl:when>
            <xsl:otherwise>730</xsl:otherwise>
         </xsl:choose>
    </xsl:template>

        <!-- ***** Namnform -->
     <xsl:template name="nameform">
        <xsl:if test="@ind = 'i'">1</xsl:if>
        <xsl:if test="@ind = 'r'">0</xsl:if>
     </xsl:template>

    <!-- ***** Ersätt icke-numeriska tecken i årtal med 'u' -->
    <xsl:template name="replace-character">
        <xsl:param name="word"/>

        <xsl:variable name="char"><xsl:value-of select="substring($word,1,1)"/></xsl:variable>
        <xsl:choose>
            <xsl:when test="$char != '0' and $char != '1' and $char != '2' and $char != '3' and $char != '4' and $char != '5' and $char != '6' and $char != '7' and $char != '8' and $char != '9'">u</xsl:when>
            <xsl:otherwise><xsl:value-of select="$char"/></xsl:otherwise>
        </xsl:choose>
        <xsl:if test="string-length($word) >= 2">
            <xsl:call-template name="replace-character">
                <xsl:with-param name="word" select="substring($word, 2)"/>
            </xsl:call-template>
        </xsl:if>
    </xsl:template>
</xsl:stylesheet>
