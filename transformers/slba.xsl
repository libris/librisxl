<?xml version="1.0" encoding="UTF-8" ?>

<!--
    Document   : slba.xsl
    Created on : December 5, 2007, 7:50 AM
    Author     : pelle
    Description:
        Purpose of transformation follows.
-->

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
    
    <xsl:output method="xml" indent="yes"/>
    
    <xsl:template match="/SLBA">
        <collection xmlns="http://www.loc.gov/MARC21/slim">
            <xsl:apply-templates select="collection"/>
        </collection>
    </xsl:template>
    
    <xsl:template match="collection">
        <xsl:variable name="cond-type" select="normalize-space(translate(record[@type = 'KP']/datafield[@tag = '002']/subfield, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>
        <xsl:if test="$cond-type != 'förvärv'">
            <xsl:variable name="media-type" select="normalize-space(translate(record[@type = 'KP']/datafield[@tag = '001']/subfield, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>
            <record type="Bibliographic">
                <!-- ***** Leader -->
                <xsl:variable name="leader06">
                    <xsl:choose>
                        <xsl:when test="record[@type = 'KP']/datafield[@tag = '005']"><xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '005'][1]" mode="leader"/></xsl:when>
                        <xsl:otherwise><xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '001'][1]" mode="leader"/></xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
                
                <xsl:variable name="leader07">
                    <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '002'][1]"/>
                </xsl:variable>
                
                <xsl:variable name="leader17">
                    <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '003'][1]"/>
                </xsl:variable>
                
                <leader>#####n<xsl:value-of select="$leader06"/><xsl:value-of select="$leader07"/><xsl:text> a22#####</xsl:text><xsl:value-of select="$leader17"/><xsl:text>a 4500</xsl:text></leader>
                <xsl:variable name="l67" select="concat($leader06, $leader07)"/>
                
                <!-- ***** Controlfield 003 -->
                <controlfield tag="003">SLBA</controlfield>
                
                <!-- ***** Controlfield 006 -->
                <xsl:if test="$l67 = 'gs' or $l67 = 'is' or $l67 = 'js' or $l67 = 'ks' or $l67 = 'ms' or $l67 = 'os'">
                    <controlfield tag="006">s|||||||||||||||||</controlfield>
                </xsl:if>
                
                <!-- ***** Controlfield 007 -->
                <xsl:choose>
                    <xsl:when test="record[@type = 'KP']/datafield[@tag = '005']"><xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '005'][1]" mode="cf007"/></xsl:when>
                    <xsl:otherwise><xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '001'][1]" mode="cf007"/></xsl:otherwise>
                </xsl:choose>
                
                <!-- ***** Controlfield 008 -->
                <xsl:variable name="cf008_00-05" select="substring(normalize-space(record[@type = 'SYS']/datafield[@tag = '125']/subfield), 3)"/>
                <xsl:variable name="cf008_06" select="normalize-space(record[@type = 'KP']/datafield[@tag = '006']/@ind1)"/>
                
                <xsl:variable name="cf008_07-10">
                    <xsl:choose>
                        <xsl:when test="string-length(record[@type = 'KP']/datafield[@tag = '006']/subfield[@code='a']) = 4">
                            <xsl:call-template name="replace-character"><xsl:with-param name="word" select="record[@type = 'KP']/datafield[@tag = '006']/subfield[@code='a']"/></xsl:call-template>
                        </xsl:when>
                        <xsl:otherwise>uuuu</xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
                <xsl:variable name="cf008_11-14">
                    <xsl:choose>
                        <xsl:when test="$cf008_06 = 'c'">9999</xsl:when>
                        <xsl:when test="$cf008_06 = 's'"><xsl:text>    </xsl:text></xsl:when>
                        <xsl:otherwise>
                            <xsl:choose>
                                <xsl:when test="normalize-space(record[@type = 'KP']/datafield[@tag = '006']/subfield[@code='b'])">
                                    <xsl:call-template name="replace-character"><xsl:with-param name="word" select="normalize-space(record[@type = 'KP']/datafield[@tag = '006']/subfield[@code='b'])"/></xsl:call-template>
                                </xsl:when>
                                <xsl:otherwise>uuuu</xsl:otherwise>
                            </xsl:choose>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
                
                <xsl:variable name="cf008_15-17" select="concat(normalize-space(record[@type = 'KP']/datafield[@tag = '017']/subfield), ' ')"/>
                
                <xsl:variable name="cf008_18-34"><xsl:call-template name="create_cf008_18-34"><xsl:with-param name="leader06-07" select="$l67"/></xsl:call-template></xsl:variable>
                <xsl:variable name="cf008_35-37">
                <xsl:variable name="temp" select="normalize-space(record[@type = 'KP']/datafield[@tag = '038' and string-length(subfield) = 3][1]/subfield)"/>
                    <xsl:choose>
                        <xsl:when test="$temp != ''"><xsl:value-of select="$temp"/></xsl:when>
                        <xsl:otherwise>und</xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
                
                <controlfield tag="008"><xsl:value-of select="$cf008_00-05"/><xsl:value-of select="$cf008_06"/><xsl:value-of select="$cf008_07-10"/><xsl:value-of select="$cf008_11-14"/><xsl:value-of select="$cf008_15-17"/><xsl:value-of select="$cf008_18-34"/><xsl:value-of select="$cf008_35-37"/><xsl:text> c</xsl:text></controlfield>
                
                <!--<xsl:choose>
                <xsl:when test="datafield[@tag = '005']"><xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '005'][1]" mode="cf007"/></xsl:when>
                <xsl:otherwise><xsl:apply-templates select="datafield[@tag = '001'][1]" mode="cf007"/></xsl:otherwise>
                </xsl:choose>-->
                                
                <!-- ***** Titel -->
                <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '008']"/>
                
                <!-- ***** Person/institution -->
                <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '012']"><xsl:with-param name="df007" select="normalize-space(translate(record[@type = 'KP']/datafield[@tag = '007']/subfield, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/></xsl:apply-templates>
                <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '013']"><xsl:with-param name="df007" select="normalize-space(translate(record[@type = 'KP']/datafield[@tag = '007']/subfield, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/></xsl:apply-templates>
                
                <!-- ***** Uniform titel -->
                <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '010']"><xsl:with-param name="df007" select="normalize-space(translate(record[@type = 'KP']/datafield[@tag = '007']/subfield, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/></xsl:apply-templates>
                
                <!-- ***** Annan (eller uniform) titel -->
                <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '009']"><xsl:with-param name="df007" select="normalize-space(translate(record[@type = 'KP']/datafield[@tag = '007']/subfield, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/></xsl:apply-templates>
                
                <!-- ***** Serie eller box (tag 011) -->
                <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '011']"/>
                
                <!-- ***** Upplaga (tag 014) -->
                <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '014']"/>
                
                <!-- ***** Utgivning / år (tag 015, 016) -->
                <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '015'][1]"/>
                <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '015'][position() > 1]"/>
                
                <!-- ***** ISSN (tag 022) -->
                <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '022']"/>
                
                <!-- ***** Inspelningsummer (tag 023) -->
                <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '023']"/>
                
                <!-- ***** Anmärkning (tag 024) -->
                <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '024']"/>
                
                <!-- ***** Innehåll (tag 025) -->
                <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '025']"/>
                
                <!-- ***** Sammanfattning, tag 026 -->
                <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '026']"/>
                
                <!-- ***** Anm.: Upplaga/Utgivning, tag 029 -->
                <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '029']"/>
                
                <!-- ***** Hårdkoda in ett 040a SLBA -->
                <datafield ind1=" " ind2=" " tag="040">
                    <subfield code="a">SLBA</subfield>
                </datafield>
                
                <!-- ***** Språk, tag 038 -->
                <xsl:if test="count(record[@type = 'KP']/datafield[@tag = '038' and string-length(subfield) = 3]) > 0">
                    <datafield ind1=" " ind2=" " tag="41">
                        <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '038' and string-length(subfield) = 3]"/>
                    </datafield>
                </xsl:if>
                
                <!-- ***** Speltid, tag 039 -->
                <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '039']"/>
                
                <!-- ***** SAB, tag 040 -->
                <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '040']"/>
                
                <!-- ***** Ämnesord, tag 041 -->
                <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '041' and @ind1 = 'p']"/>
                <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '041' and @ind1 = 'i']"/>
                <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '041' and @ind1 = 't']"/>
                <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '041' and @ind1 = 'j']"/>
                <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '041' and (@ind1 = 'q' or @ind1 = 'g' or @ind1 = 'n')]"/>
                
                <!-- ***** Genre, tag 042 -->
                <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '042' and subfield != '']"/>
                
                <!-- ***** Publikationstyp, tag 043 -->
                <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '043' and subfield != '']"/>
                
                <!-- (MAPPNING B) ***** Innehållsanmärkning, tag 061 -->
                <xsl:apply-templates select="record[@type = 'KP']/datafield[@tag = '061' and subfield != '']"/>
                
                
                <!-- ***** Titel, DP tag 010 -->
                <xsl:apply-templates select="record[@type = 'DP']/datafield[@tag = '010' and subfield != '']"/>
                
                <!-- ***** Titel, DP tag 050 -->
                <xsl:apply-templates select="record[@type = 'DP']/datafield[@tag = '050' and subfield != '']"/>
                
                <!-- ***** Inspelningsummer (DP tag 023) -->
                <xsl:apply-templates select="record[@type = 'DP']/datafield[@tag = '023' and subfield != '']"/>
                
                <!-- ***** Utgivningssummer (BP tag 077) -->
                <xsl:apply-templates select="record[@type = 'BP']/datafield[@tag = '077' and subfield != '']"/>
                
                <!-- ***** Systemkrav (BP tag 085) -->
                <xsl:apply-templates select="record[@type = 'BP']/datafield[@tag = '085' and subfield != '']"/>
                
                <!-- ***** Fysisk beskrivning, tag 103 -->
                <xsl:if test="$media-type = 'stillbild' or $media-type = 'text' or $media-type = 'övrigt'">
                    <xsl:apply-templates select="record[@type = 'BP']/datafield[@tag = '103']"/>
                </xsl:if>
                
                <!-- ***** Fysisk beskrivning, tag 090 -->
                <xsl:if test="$media-type = 'ljud-fonogram' or $media-type = 'ljud-radio' or $media-type = 'bild-video' or $media-type = 'bild-film' or $media-type = 'bild-tv' or $media-type = 'multimedier' or $media-type = 'flermedier' or $media-type = 'samling'">
                    <xsl:apply-templates select="record[@type = 'BP']/datafield[@tag = '090']"/>
                </xsl:if>
                
                <!-- ***** Senast ändrad, tag 126 -->
                <xsl:apply-templates select="record[@type = 'SYS']/datafield[@tag = '126' and subfield[@code = 'a']]"/>
                
                <!-- ***** Post-ID, tag 901 -->
                <xsl:apply-templates select="record[@type = 'SYS']/datafield[@tag = '901' and subfield != '']"/>
                
                <!-- ***** Utgivningssummer (DBP tag 077) -->
                <xsl:apply-templates select="record[@type = 'DBP']/datafield[@tag = '077' and subfield != '']"/>
                
                <!-- ***** Systemkrav (DBP tag 085) -->
                <xsl:apply-templates select="record[@type = 'DBP']/datafield[@tag = '085' and subfield != '']"/>
                
                <!-- ***** Fysisk beskrivning, DPB tag 103 -->
                <xsl:if test="$media-type = 'stillbild' or $media-type = 'text' or $media-type = 'övrigt'">
                    <xsl:apply-templates select="record[@type = 'DBP']/datafield[@tag = '103']"/>
                </xsl:if>
                
                <!-- ***** Fysisk beskrivning, DBP tag 090 -->
                <xsl:if test="$media-type = 'ljud-fonogram' or $media-type = 'ljud-radio' or $media-type = 'bild-video' or $media-type = 'bild-film' or $media-type = 'bild-tv' or $media-type = 'multimedier' or $media-type = 'flermedier' or $media-type = 'samling'">
                    <xsl:apply-templates select="record[@type = 'DBP']/datafield[@tag = '090']"/>
                </xsl:if>
            </record>
            
            <record type="Holdings">
                <leader><xsl:text>*****nu  a22*****1n 4500</xsl:text></leader>
                <xsl:variable name="cf008_00-05" select="substring(normalize-space(record[@type = 'SYS']/datafield[@tag = '125']/subfield), 3)"/>
                <controlfield tag="008"><xsl:value-of select="$cf008_00-05"/>||0000|||||000||||||000000</controlfield>
                <datafield ind1=" " ind2=" " tag="852">
                    <subfield code="b">SLBA</subfield>
                </datafield>
            </record>
        </xsl:if>
    </xsl:template>
    
    <!--
    
    <xsl:template match="record[@type = 'BP']">
        <xsl:apply-templates select="record[@type = 'BP']/datafield[@tag = '077']"/>
        <xsl:variable name="type" select="normalize-space(translate(../record[@type = 'KP']/datafield[@tag = '001']/subfield, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>
        <xsl:if test="$type = 'ljud-fonogram' or $type = 'ljud-radio' or $type = 'bild-video' or $type = 'bild-film' or $type = 'bild-tv' or $type = 'multimedier' or $type = 'flermedier' or $type = 'samling'">
            <xsl:apply-templates select="record[@type = 'BP']/datafield[@tag = '090']"/>
        </xsl:if>
    </xsl:template>
    
    <xsl:template match="record[@type = 'SYS']">
        <xsl:apply-templates select="record[@type = 'SYS']/datafield[@tag = '126']"/>
        <xsl:apply-templates select="record[@type = 'SYS']/datafield[@tag = '901']"/>
    </xsl:template>
    
    <xsl:template match="record[@type = 'DP']">
        <xsl:if test="count(record[@type = 'DP']/datafield[@tag = '010']) = 0">
            <xsl:apply-templates select="record[@type = 'DP']/datafield[@tag = '050']"/>
        </xsl:if>
    </xsl:template>
    -->
    
    <!-- KP -->
    <!-- ***** Leader 06 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '005']" mode="leader">
        <xsl:variable name="tmp" select="normalize-space(translate(subfield, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>
        <xsl:choose>
            <xsl:when test="$tmp = 'el. resurs' or $tmp = 'el. resurs-pgm' or $tmp = 'el. resurs-mult' or $tmp = 'el. resurs-spel'">m</xsl:when>
            <xsl:when test="$tmp = 'film' or $tmp = 'stillbild (dia)' or $tmp = 'video'">g</xsl:when>
            <xsl:when test="$tmp = 'komb'">o</xsl:when>
            <xsl:when test="$tmp = 'ljud-ej musik'">i</xsl:when>
            <xsl:when test="$tmp = 'ljud-musik'">j</xsl:when>
            <xsl:when test="$tmp = 'stillbild (ppr)'">k</xsl:when>
            <xsl:otherwise>a</xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    
    <!-- ***** Leader 06 Om df005  saknas -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '001']" mode="leader">
        <xsl:variable name="tmp" select="normalize-space(translate(subfield, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>
        <xsl:choose>
            <xsl:when test="$tmp = 'bild-film' or $tmp = 'bild-tv' or $tmp = 'bild-video'">g</xsl:when>
            <xsl:when test="$tmp = 'flermedier' or $tmp = 'samling'">o</xsl:when>
            <xsl:when test="$tmp = 'ljud-fonogram'">j</xsl:when>
            <xsl:when test="$tmp = 'ljud-radio'">i</xsl:when>
            <xsl:when test="$tmp = 'multimedier'">m</xsl:when>
            <xsl:when test="$tmp = 'stillbild'">k</xsl:when>
            <xsl:otherwise>a</xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    
    <!-- ***** Leader 07 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '002']">
        <xsl:variable name="tmp" select="normalize-space(translate(subfield, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>
        <xsl:choose>
            <xsl:when test="$tmp = 'periodika'">s</xsl:when>
            <xsl:when test="$tmp = 'samling'">c</xsl:when>
            <xsl:otherwise>m</xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    
    <!-- ***** Leader 13 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '003']">
        <xsl:variable name="tmp" select="normalize-space(subfield)"/>
        <xsl:choose>
            <xsl:when test="$tmp = '2'">7</xsl:when>
            <xsl:when test="$tmp = '3'"><xsl:text> </xsl:text></xsl:when>
            <xsl:otherwise>3</xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    
    <!-- ***** Controlfield 007 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '005']" mode="cf007">
        <xsl:variable name="tmp" select="normalize-space(translate(subfield, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>
        <xsl:variable name="content">
            <xsl:if test="$tmp = 'el. resurs'">c|||||||||||||</xsl:if>
            <xsl:if test="$tmp = 'el. mult' or $tmp = 'el. resurs-spel' or $tmp = 'el. resurs-pgm'">co||||||||||||</xsl:if>
            <xsl:if test="$tmp = 'film'">m||||||||||||||||||||||</xsl:if>
            <xsl:if test="$tmp = 'video'">v||||||||</xsl:if>
        </xsl:variable>
        <xsl:if test="$content != ''">
            <controlfield tag="007"><xsl:value-of select="$content"/></controlfield>
        </xsl:if>
    </xsl:template>
    
    <!-- ***** Controlfield 007 Om df005  saknas -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '001']" mode="cf007">
        <xsl:variable name="tmp" select="normalize-space(translate(subfield, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>
        <xsl:variable name="content">
            <xsl:if test="$tmp = 'bild-film'">m||||||||||||||||||||||</xsl:if>
            <xsl:if test="$tmp = 'bild-tv' or $tmp = 'bild-video'">v||||||||</xsl:if>
            <xsl:if test="$tmp = 'multimedier'">c|||||||||||||</xsl:if>
        </xsl:variable>
        <xsl:if test="$content != ''">
            <controlfield tag="007"><xsl:value-of select="$content"/></controlfield>
        </xsl:if>
    </xsl:template>
    
    <!-- ***** Datafield 245 (titel) -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '008']">
        <xsl:variable name="mtype"><xsl:call-template name="mediatype-title"/></xsl:variable>
        <datafield ind1="1" ind2="{@ind2}" tag="245">
            <xsl:copy-of select="subfield[@code = 'a']"/>
            <xsl:variable name="case"><xsl:call-template name="sf-order"/></xsl:variable>
            <xsl:choose>
                <xsl:when test="$case = '2'">
                    <xsl:copy-of select="subfield[@code = 'b']"/>
                    <xsl:copy-of select="subfield[@code = 'd']"/>
                    <xsl:copy-of select="subfield[@code = 'e']"/>
                    <xsl:if test="$mtype != ''"><subfield code="h"><xsl:value-of select="$mtype"/></subfield></xsl:if>
                    <xsl:copy-of select="subfield[@code = 'c']"/>
                </xsl:when>
                <xsl:when test="$case = '3'">
                    <xsl:if test="$mtype != ''"><subfield code="h"><xsl:value-of select="$mtype"/></subfield></xsl:if>
                    <xsl:copy-of select="subfield[@code = 'b']"/>
                    <xsl:copy-of select="subfield[@code = 'c']"/>
                    <xsl:if test="subfield[@code = 'd'] or subfield[@code = 'e']">
                        <subfield code="c">
                            <xsl:for-each select="subfield[@code = 'd']">
                                <xsl:value-of select="."/><xsl:text> </xsl:text>
                            </xsl:for-each>
                            <xsl:for-each select="subfield[@code = 'e']">
                                <xsl:value-of select="."/><xsl:text> </xsl:text>
                            </xsl:for-each>
                        </subfield>
                    </xsl:if>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:for-each select="subfield[@code = 'd']">
                        <subfield code="n"><xsl:value-of select="."/></subfield>
                    </xsl:for-each>
                    <xsl:for-each select="subfield[@code = 'e']">
                        <subfield code="e"><xsl:value-of select="."/></subfield>
                    </xsl:for-each>
                    <xsl:if test="$mtype != ''">
                        <subfield code="h"><xsl:value-of select="$mtype"/></subfield>
                    </xsl:if>
                    <xsl:copy-of select="subfield[@code = 'b']"/>
                    <xsl:copy-of select="subfield[@code = 'c']"/>
                </xsl:otherwise>
            </xsl:choose>
        </datafield>
    </xsl:template>
    
    <!-- ***** Person, tag 012 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '012']">
        <xsl:param name="df007"/>
        <xsl:variable name="ind1"><xsl:call-template name="nameform"/></xsl:variable>
        <xsl:variable name="tag">
            <xsl:choose>
                <xsl:when test="position() = 1 and contains($df007, 'namn')">100</xsl:when>
                <xsl:otherwise>700</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <datafield ind1="{$ind1}" ind2=" " tag="{$tag}">
            <xsl:for-each select="subfield">
                <xsl:if test="@code = 'a'">
                    <!--<subfield code="a"><xsl:value-of select="."/></subfield>-->
                    <subfield code="a"><xsl:call-template name="map-authority"><xsl:with-param name="slba_name" select="normalize-space(.)"/></xsl:call-template></subfield>
                </xsl:if>
                <xsl:if test="@code = 'b'">
                    <subfield code="c"><xsl:value-of select="."/></subfield>
                </xsl:if>
                <xsl:if test="@code = 'c'">
                    <subfield code="d"><xsl:value-of select="."/></subfield>
                </xsl:if>
                <xsl:if test="@code = 'd'">
                    <subfield code="4"><xsl:value-of select="."/></subfield>
                </xsl:if>
            </xsl:for-each>
        </datafield>
    </xsl:template>
    
    <!-- ***** Institution, tag 013 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '013']">
        <xsl:param name="df007"/>
        <xsl:variable name="tag">
            <xsl:choose>
                <xsl:when test="not($df007)">700</xsl:when>
                <xsl:when test="position() = 1 and contains($df007, 'grupp')">110</xsl:when>
                <xsl:otherwise>710</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        
        <datafield ind1="2" ind2=" " tag="{$tag}">
            <xsl:for-each select="subfield">
                <xsl:if test="@code = 'a' or @code = 'b'">
                    <subfield code="{@code}"><xsl:value-of select="."/></subfield>
                </xsl:if>
                <xsl:if test="@code = 'd'">
                    <subfield code="4"><xsl:value-of select="."/></subfield>
                </xsl:if>
            </xsl:for-each>
        </datafield>
    </xsl:template>
    
    <!-- ***** Uniform titel, tag 010 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '010']">
        <xsl:param name="df007"/>
        <xsl:variable name="tag">
            <xsl:choose>
                <xsl:when test="$df007 = 'uniform titel'">130</xsl:when>
                <xsl:when test="contains($df007, 'namn') or contains($df007, 'grupp')">240</xsl:when>
                <xsl:otherwise>730</xsl:otherwise>
            </xsl:choose>    
        </xsl:variable>
        
        <xsl:variable name="ind1">
            <xsl:choose>
                <xsl:when test="contains($df007, 'namn') or contains($df007, 'grupp')">1</xsl:when>
                <xsl:otherwise><xsl:value-of select="@ind2"/></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        
        <xsl:variable name="ind2">
            <xsl:choose>
                <xsl:when test="contains($df007, 'namn') or contains($df007, 'grupp')"><xsl:value-of select="@ind2"/></xsl:when>
                <xsl:otherwise><xsl:text> </xsl:text></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        
        <!--
            Busenkel logik:
            SLBA-fältens code mappas till motsvarannde LIBRIS code.
            Första SLBA $i och $g ska placeras i LIBRIS $r resp. $o.
            Övriga SLBA $i/$g ska läggas sist i föregående fält. Multipla $i ska separeras med BLANK, $g med SEMIKOLON + BLANK
            SLBA $k ska läggas sist i föregående fält. Multipla $k ska separeras med BLANK.
            Om t.ex. fältföljden i SLBA är i1 k1 o1 i2
            så ska i1 och k1 in i r, medan i2 ska läggas sist i o (som i sin tur läggs i LIBRIS $n)
            SLBA $l ska slängas, utom då SLBA $a saknas, i vilket fall $l läggs i LIBRIS $a.
            -->
        <datafield ind1="{$ind1}" ind2="{$ind2}" tag="{$tag}">
            <xsl:variable name="sfa" select="count(subfield[@code = 'a'])"/>
            <xsl:if test="subfield[@code = 'l'] and $sfa = 0">
                <subfield code="a"><xsl:value-of select="subfield[@code = 'l'][1]"/></subfield>
            </xsl:if>
            <xsl:variable name="first_i" select="normalize-space(subfield[@code = 'i'][1])"/>
            <xsl:variable name="first_g" select="normalize-space(subfield[@code = 'g'][1])"/>
            <xsl:for-each select="subfield[@code != 'l']">
                <xsl:variable name="sf-code"><xsl:call-template name="subfield-code"><xsl:with-param name="sfcode" select="@code"/></xsl:call-template></xsl:variable>
                <xsl:choose>
                    <xsl:when test="@code = 'i' and count(preceding-sibling::subfield[@code = 'i']) = 0">
                        <subfield code="r">
                            <xsl:value-of select="."/>
                            <xsl:call-template name="following">
                                <xsl:with-param name="followers" select="following-sibling::subfield"/>
                                <xsl:with-param name="i_first" select="$first_i"/>
                                <xsl:with-param name="g_first" select="$first_g"/>
                            </xsl:call-template>
                        </subfield>    
                    </xsl:when>
                    
                    <xsl:when test="@code = 'g' and count(preceding-sibling::subfield[@code = 'g']) = 0">
                        <subfield code="o">
                            <xsl:value-of select="."/>
                            <xsl:call-template name="following">
                                <xsl:with-param name="followers" select="following-sibling::subfield"/>
                                <xsl:with-param name="i_first" select="$first_i"/>
                                <xsl:with-param name="g_first" select="$first_g"/>
                            </xsl:call-template>
                        </subfield>    
                    </xsl:when>
                    
                    <xsl:when test="$sf-code != ''">
                        <subfield code="{$sf-code}">
                            <xsl:value-of select="."/>
                            <xsl:call-template name="following">
                                <xsl:with-param name="followers" select="following-sibling::subfield"/>
                                <xsl:with-param name="i_first" select="$first_i"/>
                                <xsl:with-param name="g_first" select="$first_g"/>
                                <xsl:with-param name="add_semicolon" select="'true'"/>
                            </xsl:call-template>
                        </subfield>
                    </xsl:when>
                </xsl:choose>
                
            </xsl:for-each>
        </datafield>
    </xsl:template>
    
    <!-- ***** Annan eller uniform titel, tag 009 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '009']">
        <xsl:param name="df007"/>
        
        <xsl:variable name="tag">
            <xsl:choose>
                <xsl:when test="@ind1 != 'o'">246</xsl:when>
                <xsl:when test="@ind1 = 'o' and count(../datafield[@tag = '010']) > 0">730</xsl:when>
                <xsl:when test="@ind1 = 'o' and count(preceding-sibling::datafield[@tag = '009' and @ind1 = 'o']) = 0 and $df007 = 'titel'">130</xsl:when>
                <xsl:when test="@ind1 = 'o' and count(preceding-sibling::datafield[@tag = '009' and @ind1 = 'o']) = 0 and (contains($df007, 'namn') or contains($df007, 'grupp'))">240</xsl:when>
                <xsl:otherwise>730</xsl:otherwise>
            </xsl:choose>    
        </xsl:variable>
        
        <xsl:variable name="ind1">
            <xsl:choose>
                <xsl:when test="$tag = '240'">1</xsl:when>
                <xsl:when test="@ind1 = 'o'"><xsl:value-of select="@ind2"/></xsl:when>
                <xsl:otherwise>1</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <xsl:variable name="ind2">
            <xsl:choose>
                <xsl:when test="$tag = '240'"><xsl:value-of select="@ind2"/></xsl:when>
                <xsl:when test="@ind1 = 'o'"><xsl:text> </xsl:text></xsl:when>
                <xsl:otherwise><xsl:call-template name="ind1-009"/></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        
        <datafield ind1="{$ind1}" ind2="{$ind2}" tag="{$tag}">
            <xsl:if test="@ind1 = 's'"><subfield code="i"><xsl:text>Svensk titel: </xsl:text></subfield></xsl:if>
            <xsl:for-each select="subfield">
                <xsl:choose>
                    <xsl:when test="@code='b'"><subfield code="a"><xsl:value-of select="."/></subfield></xsl:when>
                    <xsl:when test="@code='c'"><subfield code="n"><xsl:value-of select="."/></subfield></xsl:when>
                    <xsl:when test="@code='d'"><subfield code="p"><xsl:value-of select="."/></subfield></xsl:when>
                    <xsl:otherwise><subfield code="{@code}"><xsl:value-of select="."/></subfield></xsl:otherwise>
                </xsl:choose>
            </xsl:for-each>
        </datafield>
    </xsl:template>
    
    <!-- ***** Serie/Box, tag 011 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '011']">
        <xsl:choose>
            <xsl:when test="@ind1 = 'b'">
                <datafield ind1="0" ind2="8" tag="787">
                    <subfield code="i"><xsl:text>Utgiven i box: </xsl:text></subfield>
                    <xsl:for-each select="subfield">
                        <xsl:if test="@code='a'"><subfield code="t"><xsl:value-of select="translate(., '*', '')"/></subfield></xsl:if>
                        <xsl:if test="@code='d'"><subfield code="d"><xsl:value-of select="."/></subfield></xsl:if>
                        <xsl:if test="@code='e'"><subfield code="h"><xsl:value-of select="."/></subfield></xsl:if>
                        <xsl:if test="@code='f'"><subfield code="n"><xsl:value-of select="."/></subfield></xsl:if>
                    </xsl:for-each>
                </datafield>
            </xsl:when>
            <xsl:otherwise>
                <datafield ind1="0" ind2=" " tag="490">
                    <xsl:for-each select="subfield">
                        <xsl:if test="@code='a'"><subfield code="a"><xsl:value-of select="translate(., '*', '')"/></subfield></xsl:if>
                        <xsl:if test="@code='b'"><subfield code="v"><xsl:value-of select="."/></subfield></xsl:if>
                        <xsl:if test="@code='c'"><subfield code="x"><xsl:value-of select="."/></subfield></xsl:if>
                    </xsl:for-each>
                </datafield>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    
    <!-- ***** Upplaga, tag 014 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '014']">
        <datafield ind1=" " ind2=" " tag="250">
            <subfield code="a"><xsl:value-of select="subfield"/></subfield>
        </datafield>
    </xsl:template>
    
    <!-- ***** Utgivning/år, tag 015, 016 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '015'][1]">
        <datafield ind1=" " ind2=" " tag="260">
            <xsl:copy-of select="subfield[@code = 'a']"/>
            <xsl:copy-of select="subfield[@code = 'b']"/>
            <xsl:if test="../datafield[@tag = '016']">
                <subfield code="c"><xsl:value-of select="../datafield[@tag = '016']/subfield[@code = 'a']"/></subfield>
            </xsl:if>
        </datafield>
    </xsl:template>
    
    <!-- ***** Anmärkning, tag 015 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '015'][position() > 1]">
        <datafield ind1=" " ind2=" " tag="500">
            <subfield code="a">
                <xsl:text>Även utgiven: </xsl:text>
                <xsl:for-each select="subfield[@code = 'a']">
                    <xsl:value-of select="."/><xsl:if test="position() != last()"><xsl:text>, </xsl:text></xsl:if>
                </xsl:for-each>
            </subfield>
        </datafield>
    </xsl:template>
    
    <!-- ***** ISSN, tag 022 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '022']">
        <datafield ind1=" " ind2=" " tag="022">
            <subfield code="a"><xsl:value-of select="subfield"/></subfield>
        </datafield>
    </xsl:template>
    
    <!-- ***** Inspelningsnummer, tag 023 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '023']">
        <xsl:if test="@ind1 = 'a'">
            <datafield ind1="7" ind2=" " tag="024">
                <subfield code="a"><xsl:value-of select="subfield"/></subfield>
                <subfield code="2">isan</subfield>
            </datafield>
        </xsl:if>
        <xsl:if test="@ind1 = 'm'">
            <datafield ind1="1" ind2="1" tag="028">
                <subfield code="a"><xsl:value-of select="subfield"/></subfield>
            </datafield>
        </xsl:if>
        
        <xsl:if test="@ind1 = 'p'">
            <datafield ind1="0" ind2="1" tag="028">
                <subfield code="a"><xsl:value-of select="subfield"/></subfield>
            </datafield>
        </xsl:if>
        
        <xsl:if test="@ind1 = 'r'">
            <datafield ind1="2" ind2=" " tag="024">
                <subfield code="a"><xsl:value-of select="subfield"/></subfield>
            </datafield>
        </xsl:if>
    </xsl:template>
    
    <!-- ***** Anmärkning, tag 024 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '024']">
        <datafield ind1=" " ind2=" " tag="500">
            <subfield code="a"><xsl:value-of select="subfield"/></subfield>
        </datafield>
    </xsl:template>
    
    <!-- ***** Innehåll, tag 025 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '025']">
        <datafield ind1="0" ind2=" " tag="505">
            <subfield code="a"><xsl:value-of select="subfield"/></subfield>
        </datafield>
    </xsl:template>
    
    <!-- ***** Sammanfattning, tag 026 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '026']">
        <datafield ind1="8" ind2=" " tag="520">
            <subfield code="a"><xsl:value-of select="subfield"/></subfield>
        </datafield>
    </xsl:template>
    
    <!-- ***** Anm.; Upplaga/Utgivning, tag 029 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '029']">
        <datafield ind1=" " ind2=" " tag="500">
            <subfield code="a"><xsl:value-of select="subfield"/></subfield>
        </datafield>
    </xsl:template>
    
    <!-- ***** Språk, tag 038 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '038']">
        <subfield code="a"><xsl:value-of select="subfield"/></subfield>
    </xsl:template>
    
    <!-- ***** Speltid, tag 039 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '039']">
        <datafield ind1="" ind2="" tag="500">
            <xsl:for-each select="subfield">
                <subfield code="a"><xsl:value-of select="."/></subfield>
            </xsl:for-each>
        </datafield>
    </xsl:template>
    
    <!-- ***** SAB, tag 040 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '040']">
        <xsl:variable name="mediatype"><xsl:call-template name="mediatype"/></xsl:variable>
        <datafield ind1="" ind2="" tag="084">
            <subfield code="a"><xsl:value-of select="concat(subfield, $mediatype)"/></subfield>
            <subfield code="2">kssb/8</subfield>
        </datafield>
    </xsl:template>
    
    <!-- ***** Ämnesord, personnamn, tag 041 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '041' and @ind1 = 'p']">
        <xsl:variable name="tag">
            <xsl:choose>
                <xsl:when test="count(subfield[@code != 'a']) = 0 or (count(subfield[@code = 'a']) > 0 and count(subfield[@code = 'h']) = 1)">600</xsl:when>
                <xsl:otherwise>653</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <xsl:variable name="ind1">
            <xsl:choose>
                <xsl:when test="$tag = '600'">
                    <xsl:call-template name="check-name"><xsl:with-param name="kandidat" select="subfield[@code = 'a']"/></xsl:call-template>
                </xsl:when>
                <xsl:otherwise><xsl:text> </xsl:text></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        
        <datafield ind1="{$ind1}" ind2="4" tag="{$tag}">
            <xsl:choose>
                <xsl:when test="$tag = '600'">
                    <xsl:for-each select="subfield">
                        <xsl:if test="@code = 'a'">
                            <subfield code="a"><xsl:value-of select="."/></subfield>
                        </xsl:if>
                        <xsl:if test="@code = 'h'">
                            <subfield code="d">
                                <xsl:value-of select="."/>
                                <xsl:if test="substring(., 1 + string-length(.) - 1) != ','">,</xsl:if>
                            </subfield>
                        </xsl:if>
                    </xsl:for-each>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:if test="count(subfield[. != '']) > 0">
                        <subfield code="a">
                            <xsl:for-each select="subfield[@code != 'x']">
                                <xsl:value-of select="."/><xsl:if test="position() != last()"><xsl:text> :</xsl:text></xsl:if>
                            </xsl:for-each>
                        </subfield>
                    </xsl:if>
                </xsl:otherwise>
            </xsl:choose>
        </datafield>
    </xsl:template>
    
    <!-- ***** Ämnesord, institutionsnamn, tag 041 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '041' and @ind1 = 'i']">
        <xsl:variable name="tag">
            <xsl:choose>
                <xsl:when test="count(subfield[@code != 'a']) = 0 or (count(subfield[@code = 'a']) > 0 and count(subfield[@code = 'h']) > 0)">610</xsl:when>
                <xsl:otherwise>653</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <xsl:variable name="ind1">
            <xsl:choose>
                <xsl:when test="$tag = '600'">2</xsl:when>
                <xsl:otherwise><xsl:text> </xsl:text></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        
        <datafield ind1="{$ind1}" ind2="4" tag="{$tag}">
            <xsl:choose>
                <xsl:when test="$tag = '610' and subfield[@code = 'a' or @code = 'h']">
                    <subfield code="a">
                        <xsl:if test="subfield[@code = 'a']"><xsl:value-of select="subfield[@code = 'a']"/></xsl:if>
                        <xsl:if test="subfield[@code = 'h']">
                            <xsl:text> (</xsl:text><xsl:for-each select="subfield[@code = 'h']"><xsl:value-of select="."/><xsl:if test="position() != last()"><xsl:text> </xsl:text></xsl:if></xsl:for-each><xsl:text>)</xsl:text>
                        </xsl:if>
                    </subfield>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:if test="count(subfield[. != '']) > 0">
                        <subfield code="a">
                            <xsl:for-each select="subfield[@code != 'x']">
                                <xsl:value-of select="."/><xsl:if test="position() != last()"><xsl:text> : </xsl:text></xsl:if>
                            </xsl:for-each>
                        </subfield>
                    </xsl:if>
                </xsl:otherwise>
            </xsl:choose>
        </datafield>
    </xsl:template>
    
    <!-- ***** Ämnesord, titel, tag 041 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '041' and @ind1 = 't']">
        <datafield ind1="630" ind2="0" tag="4">
            <xsl:for-each select="subfield">
                <xsl:if test="@code = 'a'">
                    <subfield code="a"><xsl:value-of select="."/>.</subfield>
                </xsl:if>
                <xsl:if test="@code = 'h'">
                    <subfield code="p"><xsl:value-of select="."/>.</subfield>
                </xsl:if>
            </xsl:for-each>
        </datafield>
    </xsl:template>
    
    <!-- ***** Ämnesord, kontrollerade, geografiskt namn, genre, tag 041 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '041' and (@ind1 = 'q' or @ind1 = 'g' or @ind1 = 'n')]">
        <xsl:variable name="tag">
            <xsl:choose>
                <xsl:when test="@ind1='q'">650</xsl:when>
                <xsl:when test="@ind1='g'">651</xsl:when>
                <xsl:otherwise>655</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <xsl:variable name="ind2">
            <xsl:choose>
                <xsl:when test="subfield[@code = 'x']">7</xsl:when>
                <xsl:otherwise>4</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <datafield ind1=" " ind2="{$ind2}" tag="{$tag}">
            <xsl:for-each select="subfield">
                <xsl:if test="@code = 'a'">
                    <subfield code="a"><xsl:value-of select="."/></subfield>
                </xsl:if>
                <xsl:if test="@code = 'h'">
                    <subfield code="x"><xsl:value-of select="."/></subfield>
                </xsl:if>
                <xsl:if test="@code = 'i'">
                    <subfield code="z"><xsl:value-of select="."/></subfield>
                </xsl:if>
                <xsl:if test="@code = 'j'">
                    <subfield code="y"><xsl:value-of select="."/></subfield>
                </xsl:if>
                <xsl:if test="@code = 'k'">
                    <subfield code="v"><xsl:value-of select="."/></subfield>
                </xsl:if>
                <xsl:if test="@code = 'x'">
                    <subfield code="2"><xsl:value-of select="."/></subfield>
                </xsl:if>
            </xsl:for-each>
        </datafield>
    </xsl:template>
    
    <!-- ***** Ämnesord, icke kontrollerade, tag 041 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '041' and @ind1 = 'j']">
        <xsl:if test="subfield[@code != 'x']">
            <datafield ind1="653" ind2=" " tag="4">
                <subfield code="a">
                    <xsl:for-each select="subfield"><xsl:value-of select="."/><xsl:if test="position() != last()"><xsl:text> -- </xsl:text></xsl:if></xsl:for-each>
                </subfield>
            </datafield>
        </xsl:if>
    </xsl:template>
    
    <!-- ***** Genre, tag 042-->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '042']">
        <xsl:variable name="first" select="translate(substring(normalize-space(subfield), 1, 1), 'abcdefghijklmnopqrstuvwxyzåäö', 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ')"/>
        <xsl:variable name="the_rest" select="substring(normalize-space(subfield), 2)"/>
        <datafield ind1=" " ind2="4" tag="655">
            <subfield code="a"><xsl:value-of select="concat($first, $the_rest)"/></subfield>
        </datafield>
    </xsl:template>
    
    <!-- ***** Publikationstyp, tag 043-->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '043']">
        <datafield ind1=" " ind2=" " tag="500">
            <subfield code="a"><xsl:value-of select="subfield"/></subfield>
        </datafield>
    </xsl:template>
    
    <!-- ***** Innehållsanmärkning, tag 061 -->
    <xsl:template match="record[@type = 'KP']/datafield[@tag = '061']">
        <datafield ind1="0" ind2=" " tag="505">
            <subfield code="a"><xsl:value-of select="subfield"/></subfield>
        </datafield>
    </xsl:template>
    
    <!-- DP -->
    <!-- ***** Innehållsanmärkning, tag 010 -->
    <xsl:template match="record[@type = 'DP']/datafield[@tag = '010']">
        <datafield ind1=" " ind2=" " tag="505">
            <subfield code="a">
                <xsl:for-each select="subfield">
                    <xsl:value-of select="."/><xsl:if test="position() != last()"><xsl:text> -- </xsl:text></xsl:if>
                </xsl:for-each>
                <xsl:if test="../datafield[@tag = '050' and subfield[@code = 'c'] and not(subfield[@code != 'c'])]">
                    <xsl:text>/ </xsl:text><xsl:value-of select="../datafield[@tag = '050' and subfield[@code = 'c'] and not(subfield[@code != 'c'])]/subfield[@code = 'c']"/>
                </xsl:if>
            </subfield>
        </datafield>
    </xsl:template>
    
    <!-- ***** Innehållsanmärkning, tag 050 -->
    <xsl:template match="record[@type = 'DP']/datafield[@tag = '050']">
        <xsl:if test="count(../datafield[@tag = '010']) = 0">
            <datafield ind1="" ind2="" tag="505">
                <subfield code="a">
                    <xsl:for-each select="subfield">
                        <xsl:value-of select="."/><xsl:if test="position() != last()"><xsl:text> -- </xsl:text></xsl:if>
                    </xsl:for-each>
                </subfield>
            </datafield>
        </xsl:if>
    </xsl:template>
    
    <!-- ***** Inspelningsnummer, DP tag 023 -->
    <xsl:template match="record[@type = 'DP']/datafield[@tag = '023']">
        <xsl:variable name="tag">
            <xsl:choose>
                <xsl:when test="@ind1 = 'a'">024</xsl:when>
                <xsl:when test="@ind1 = 'm'">028</xsl:when>
                <xsl:when test="@ind1 = 'r'">024</xsl:when>
                <xsl:otherwise>na</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        
        <xsl:variable name="ind1">
            <xsl:if test="@ind1 = 'a'">7</xsl:if>
            <xsl:if test="@ind1 = 'm'">1</xsl:if>
            <xsl:if test="@ind1 = 'r'">0</xsl:if>
        </xsl:variable>
        
        <xsl:variable name="ind2">
            <xsl:if test="@ind1 = 'a'"><xsl:text> </xsl:text></xsl:if>
            <xsl:if test="@ind1 = 'm'">1</xsl:if>
            <xsl:if test="@ind1 = 'r'"><xsl:text> </xsl:text></xsl:if>
        </xsl:variable>
        
        <xsl:if test="$tag != 'na'">
            <datafield ind1="{$ind1}" ind2="{$ind2}" tag="{$tag}">
                <subfield code="a"><xsl:value-of select="."/></subfield>
                <xsl:if test="@ind1 = 'a'">
                    <subfield code="2">isan</subfield>
                </xsl:if>
            </datafield>
        </xsl:if>
    </xsl:template>
    
    <!-- BP -->
    <!-- ***** Utgivningsnummer, tag 077 -->
    <xsl:template match="record[@type = 'BP']/datafield[@tag = '077']">
        <xsl:variable name="tag">
            <xsl:choose>
                <xsl:when test="@ind1 = 'i'">020</xsl:when>
                <xsl:when test="@ind1 = 'm' or @ind1 = 's' or @ind1 = 'v'">028</xsl:when>
                <xsl:otherwise>na</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        
        <xsl:variable name="ind1">
            <xsl:if test="@ind1 = 'i'"><xsl:text> </xsl:text></xsl:if>
            <xsl:if test="@ind1 = 'm'">5</xsl:if>
            <xsl:if test="@ind1 = 's'">0</xsl:if>
            <xsl:if test="@ind1 = 'v'">4</xsl:if>
        </xsl:variable>
        
        <xsl:variable name="ind2">
            <xsl:if test="@ind1 = 'i'"><xsl:text> </xsl:text></xsl:if>
            <xsl:if test="@ind1 = 'm'">1</xsl:if>
            <xsl:if test="@ind1 = 's'">1</xsl:if>
            <xsl:if test="@ind1 = 'v'"><xsl:text> </xsl:text></xsl:if>
        </xsl:variable>
        
        <xsl:if test="$tag != 'na'">
            <datafield ind1="{$ind1}" ind2="{$ind2}" tag="{$tag}">
                <xsl:for-each select="subfield">
                    <xsl:if test="@code = 'a' and ../@ind1 = 's'">
                        <subfield code="b"><xsl:value-of select="."/></subfield>
                    </xsl:if>
                    <xsl:if test="@code = 'b'">
                        <subfield code="a"><xsl:value-of select="."/></subfield>
                    </xsl:if>
                </xsl:for-each>
            </datafield>
        </xsl:if>
    </xsl:template>
    
    <!-- ***** Systemkrav, tag 085 -->
    <xsl:template match="record[@type = 'BP']/datafield[@tag = '085']">
        <subfield code="a"><xsl:text>Finns för: </xsl:text><xsl:value-of select="subfield[@code='a']"/></subfield>
    </xsl:template>
    
    <!-- ***** Formatbärare, tag 090 -->
    <xsl:template match="record[@type = 'BP']/datafield[@tag = '090']">
        <datafield ind1="1" ind2=" " tag="300">
            <xsl:for-each select="subfield[@code = 'a']">
                <subfield code = "a"><xsl:call-template name="mediatype-090"/></subfield>
            </xsl:for-each>
        </datafield>
        
        <datafield ind1=" " ind2=" " tag="500">
            <xsl:for-each select="subfield[@code = 'e']">
                <subfield code = "a"><xsl:text> Finns som: </xsl:text><xsl:value-of select="."/></subfield>
            </xsl:for-each>
        </datafield>
    </xsl:template>
    
    <!-- ***** Fysisk beskrivning, tag 103 -->
    <xsl:template match="record[@type = 'BP']/datafield[@tag = '103']">
        <datafield ind1=" " ind2=" " tag="300">
            <xsl:copy-of select="subfield[@code = 'a']"/>
            <xsl:copy-of select="subfield[@code = 'b']"/>
            <xsl:copy-of select="subfield[@code = 'c']"/>
            <xsl:copy-of select="subfield[@code = 'e']"/>
        </datafield>
    </xsl:template>
    
    <!-- SYS -->
      
    <!-- ***** Senast ändrad, tag 126 -->
    <!-- FÖRKLARING BEHÖVS -->
    <xsl:template match="record[@type = 'SYS']/datafield[@tag = '126']">
        <datafield ind1=" " ind2=" " tag="599">
            <subfield code="a"><xsl:text>SLBA. </xsl:text><xsl:value-of select="subfield[@code = 'a']"/></subfield>
            <!--<subfield code="a">SLBA &lt;SYS TAG=&quot;126&quot;></subfield>-->
        </datafield>
    </xsl:template>
    
    <!-- ***** Post-ID, tag 901 -->
    <xsl:template match="record[@type = 'SYS']/datafield[@tag = '901']">
        <controlfield tag="001"><xsl:value-of select="subfield"/></controlfield>
        <datafield ind1="4" ind2="8" tag="856">
            <subfield code="u"><xsl:value-of select="concat('http://www.slba.se/slbaweb/slbadoc.jsp?searchpage=slbasearch.jsp&amp;search_all=', normalize-space(subfield), '.id.&amp;sort=&amp;from=1&amp;toc_length=15&amp;doctype=KP&amp;toctype=KP&amp;currdoc=1')"/></subfield>
            <subfield code="z">Sesam</subfield>
        </datafield>
    </xsl:template>
    
    <!-- DBP -->
    <!-- ***** Utgivningsnummer, DBP tag 077 -->
    <xsl:template match="record[@type = 'DBP']/datafield[@tag = '077']">
        <xsl:variable name="tag">
            <xsl:choose>
                <xsl:when test="@ind1 = 'i'">020</xsl:when>
                <xsl:when test="@ind1 = 'm' or @ind1 = 's' or @ind1 = 'v'">028</xsl:when>
                <xsl:otherwise>na</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        
        <xsl:variable name="ind1">
            <xsl:if test="@ind1 = 'i'"><xsl:text> </xsl:text></xsl:if>
            <xsl:if test="@ind1 = 'm'">5</xsl:if>
            <xsl:if test="@ind1 = 's'">0</xsl:if>
            <xsl:if test="@ind1 = 'v'">4</xsl:if>
        </xsl:variable>
        
        <xsl:variable name="ind2">
            <xsl:if test="@ind1 = 'i'"><xsl:text> </xsl:text></xsl:if>
            <xsl:if test="@ind1 = 'm'">1</xsl:if>
            <xsl:if test="@ind1 = 's'">1</xsl:if>
            <xsl:if test="@ind1 = 'v'"><xsl:text> </xsl:text></xsl:if>
        </xsl:variable>
        
        <xsl:if test="$tag != 'na'">
            <datafield ind1="{$ind1}" ind2="{$ind2}" tag="{$tag}">
                <xsl:for-each select="subfield">
                    <xsl:if test="@code = 'a' and ../@ind1 = 's'">
                        <subfield code="b"><xsl:value-of select="."/></subfield>
                    </xsl:if>
                    <xsl:if test="@code = 'b'">
                        <subfield code="a"><xsl:value-of select="."/></subfield>
                    </xsl:if>
                </xsl:for-each>
            </datafield>
        </xsl:if>
    </xsl:template>
    
    <!-- ***** Systemkrav, DBP tag 085 -->
    <xsl:template match="record[@type = 'DBP']/datafield[@tag = '085']">
        <subfield code="a"><xsl:text>Finns för: </xsl:text><xsl:value-of select="subfield[@code='a']"/></subfield>
    </xsl:template>
    
    <!-- ***** Formatbärare, DBP tag 090 -->
    <xsl:template match="record[@type = 'DBP']/datafield[@tag = '090']">
        <datafield ind1="1" ind2=" " tag="300">
            <xsl:for-each select="subfield[@code = 'a']">
                <subfield code = "a"><xsl:call-template name="mediatype-090"/></subfield>
            </xsl:for-each>
        </datafield>
        
        <datafield ind1=" " ind2=" " tag="500">
            <xsl:for-each select="subfield[@code = 'e']">
                <subfield code = "a"><xsl:text> Finns som: </xsl:text><xsl:value-of select="."/></subfield>
            </xsl:for-each>
        </datafield>
    </xsl:template>
    
    <!-- ***** Fysisk beskrivning, DBP tag 103 -->
    <xsl:template match="record[@type = 'DBP']/datafield[@tag = '103']">
        <datafield ind1=" " ind2=" " tag="300">
            <xsl:copy-of select="subfield[@code = 'a']"/>
            <xsl:copy-of select="subfield[@code = 'b']"/>
            <xsl:copy-of select="subfield[@code = 'c']"/>
            <xsl:copy-of select="subfield[@code = 'e']"/>
        </datafield>
    </xsl:template>
    
    <!-- NAME TEMPLATES -->
    
    <!-- ***** Namnform -->
    <xsl:template name="nameform">
        <xsl:if test="@ind1 = 'i'">1</xsl:if>
        <xsl:if test="@ind1 = 'r'">0</xsl:if>
    </xsl:template>
    
    <!-- ***** Indikator KP tag = 009 -->
    <xsl:template name="ind1-009">
        <xsl:choose>
            <xsl:when test="@ind1 = 'k'">4</xsl:when>
            <xsl:when test="@ind1 = 'p'">1</xsl:when>
            <xsl:when test="@ind1 = 'r'">8</xsl:when>
            <xsl:when test="@ind1 = 't'">2</xsl:when>
            <xsl:when test="@ind1 = 'ö'">3</xsl:when>
            <xsl:otherwise><xsl:text> </xsl:text></xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    
    <!-- Medietyp -->
    <xsl:template name="mediatype">
        <xsl:variable name="type" select="normalize-space(translate(../datafield[@tag = '001']/subfield, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>
        <xsl:if test="$type = 'bild-film' or $type = 'bild-tv' or $type = 'bild-video'">/V</xsl:if>
        <xsl:if test="$type = 'ljud-fonogram' or $type = 'ljud-radio'">/L</xsl:if>
        <xsl:if test="$type = 'multimedier'">/D</xsl:if>
        <xsl:if test="$type = 'stillbild'">/B</xsl:if>
    </xsl:template>
    
    <!-- ***** Medietyp, för 245 -->
    <xsl:template name="mediatype-title">
        <xsl:variable name="type" select="normalize-space(translate(../datafield[@tag = '001']/subfield, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>
        <xsl:if test="$type = 'bild-film'">[Film]</xsl:if>
        <xsl:if test="$type = 'bild-tv' or $type = 'bild-video'">[Videoupptagning]</xsl:if>
        <xsl:if test="$type = 'ljud-fonogram' or $type = 'ljud-radio'">[Ljudupptagning]</xsl:if>
        <xsl:if test="$type = 'multimedier'">[Elektronisk resurs]</xsl:if>
        <xsl:if test="$type = 'flermedier'">[Kombinerat material]</xsl:if>
        <xsl:if test="$type = 'stillbild'">[Bild]</xsl:if>
    </xsl:template>
    
    <!-- ***** Medietyp, för BP 090 till 300 -->
    <xsl:template name="mediatype-090">
        <xsl:variable name="type" select="normalize-space(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>
        <xsl:if test="$type = 'film'">Film</xsl:if>
        <xsl:if test="$type = 'ljud'">Ljudupptagning</xsl:if>
        <xsl:if test="$type = 'ljud-video-multimedier'">Ljud-/videoupptagning</xsl:if>
        <xsl:if test="$type = 'multimedier'">Multimedium</xsl:if>
        <xsl:if test="$type = 'video'">Video</xsl:if>
    </xsl:template>
    
    <!-- ***** Controlfield 008, pos 18-34 -->
    <xsl:template name="create_cf008_18-34">
        <xsl:param name="leader06-07"/>
        <xsl:if test="$leader06-07 = 'ac' or $leader06-07 = 'am'"><xsl:text>||||      |00| ||</xsl:text></xsl:if>
        <xsl:if test="$leader06-07 = 'as'"><xsl:text>|||p|     |0   |0</xsl:text></xsl:if>
        <xsl:if test="$leader06-07 = 'gc' or $leader06-07 = 'gm' or $leader06-07 = 'gs' or $leader06-07 = 'kc' or $leader06-07 = 'km' or $leader06-07 = 'ks' or $leader06-07 = 'oc' or $leader06-07 = 'om' or $leader06-07 = 'os'"><xsl:text>|||       |    ||</xsl:text></xsl:if>
        <xsl:if test="$leader06-07 = 'ic' or $leader06-07 = 'im' or $leader06-07 = 'is' or $leader06-07 = 'jc' or $leader06-07 = 'jm' or $leader06-07 = 'js'"><xsl:text>|||   ||||||     </xsl:text></xsl:if>
        <xsl:if test="$leader06-07 = 'mc' or $leader06-07 = 'mm' or $leader06-07 = 'ms'"><xsl:text>        | |      </xsl:text></xsl:if>
    </xsl:template>
    
    <!-- ***** Avgör ordningen i SLBA tag=008/subfield för bearbetning i MARC21 tag=245 -->
    <xsl:template name="sf-order">
        <xsl:choose>
            <xsl:when test="subfield[@code='a']/following-sibling::subfield[1]/@code = 'd'">1</xsl:when>
            <xsl:when test="subfield[@code='a']/following-sibling::subfield[1]/@code = 'e'">1</xsl:when>
            <xsl:when test="subfield[@code='b']/following-sibling::subfield[1]/@code = 'd'">2</xsl:when>
            <xsl:when test="subfield[@code='b']/following-sibling::subfield[1]/@code = 'e'">2</xsl:when>
            <xsl:when test="subfield[@code='c']/following-sibling::subfield[1]/@code = 'd'">3</xsl:when>
            <xsl:when test="subfield[@code='c']/following-sibling::subfield[1]/@code = 'e'">3</xsl:when>
        </xsl:choose>
    </xsl:template>
    
    <!-- ***** Översätter subfield code för transf. 010 till 130/730/240 -->
    <xsl:template name="subfield-code">
        <xsl:param name="sfcode"/>
        <xsl:if test="$sfcode = 'a'">a</xsl:if>
        <xsl:if test="$sfcode = 'd'">l</xsl:if>
        <xsl:if test="$sfcode = 'h'">p</xsl:if>
        <xsl:if test="$sfcode = 'n'">n</xsl:if>
        <xsl:if test="$sfcode = 'e'">m</xsl:if>
        <xsl:if test="$sfcode = 'f'">n</xsl:if>
        <xsl:if test="$sfcode = 'o'">n</xsl:if>
        <xsl:if test="$sfcode = 'j'">s</xsl:if>
        <xsl:if test="$sfcode = 'c'">k</xsl:if>
    </xsl:template>
    
    <!-- ***** Lägger ihop i- g- och k-subfält för inläggning i 130/730/240 -->
    <!--<xsl:template name="followi">
        <xsl:param name="followers"/>
        <xsl:param name="g_first"/>
        <xsl:if test="$followers[1]/@code = 'i' or $followers[1]/@code = 'k' or ($followers[1]/@code = 'g' and $followers[1] != string($g_first))">
            <xsl:text> </xsl:text><xsl:value-of select="$followers[1]"/>
            <xsl:if test="$followers[2]">
                <xsl:call-template name="followi">
                    <xsl:with-param name="followers" select="$followers[position() > 1]"/>
                    <xsl:with-param name="g_first" select="$g_first"/>
                </xsl:call-template>
            </xsl:if>
        </xsl:if>
    </xsl:template>-->
    
    <!-- ***** Lägger ihop g- i- och k-subfält för inläggning i 130/730/240 -->
    <!--<xsl:template name="followg">
        <xsl:param name="followers"/>
        <xsl:param name="i_first"/>
        <xsl:if test="$followers[1]/@code = 'g' or $followers[1]/@code = 'k' or ($followers[1]/@code = 'i' and $followers[1] != string($i_first))">
            <xsl:text> </xsl:text><xsl:value-of select="$followers[1]"/>
            <xsl:if test="$followers[2]">
                <xsl:call-template name="followg">
                    <xsl:with-param name="followers" select="$followers[position() > 1]"/>
                    <xsl:with-param name="i_first" select="$i_first"/>
                </xsl:call-template>
            </xsl:if>
        </xsl:if>
    </xsl:template>-->
    
    <!-- ***** Lägger ihop i- g- och k-subfält för inläggning i 130/730/240 -->
    <xsl:template name="following">
        <xsl:param name="followers"/>
        <xsl:param name="i_first"/>
        <xsl:param name="g_first"/>
        <xsl:param name="add_semicolon"/>
        
        <xsl:if test="($followers[1]/@code = 'i' and $followers[1] != string($i_first)) or ($followers[1]/@code = 'g' and $followers[1] != string($g_first)) or $followers[1]/@code = 'k'">
            <xsl:if test="$followers[1]/@code = 'g' and string($add_semicolon) != ''">;</xsl:if><xsl:text> </xsl:text><xsl:value-of select="$followers[1]"/>
            <xsl:if test="$followers[2]">
                <xsl:call-template name="following">
                    <xsl:with-param name="followers" select="$followers[position() > 1]"/>
                    <xsl:with-param name="i_first" select="$i_first"/>
                    <xsl:with-param name="g_first" select="$g_first"/>
                </xsl:call-template>
            </xsl:if>
        </xsl:if>
    </xsl:template>
    
    <!-- ***** Lägger ihop i- g- och k-subfält för inläggning i 130/730/240 -->
    <!--<xsl:template name="follow-subfields">
        <xsl:param name="followers"/>
        <xsl:param name="i_first"/>
        <xsl:param name="g_first"/>
                <xsl:if test="($followers[1]/@code = 'i' and $followers[1] != string($i_first)) or ($followers[1]/@code = 'g' and $followers[1] != string($g_first)) or $followers[1]/@code = 'k'">
            <xsl:text> </xsl:text><xsl:value-of select="$followers[1]"/>
            <xsl:if test="$followers[2]">
                <xsl:call-template name="follow-subfields">
                    <xsl:with-param name="followers" select="$followers[position() > 1]"/>
                    <xsl:with-param name="i_first" select="$i_first"/>
                    <xsl:with-param name="g_first" select="$g_first"/>
                </xsl:call-template>
            </xsl:if>
        </xsl:if>
    </xsl:template>-->
    
    <!-- ***** Skapar ett 130/730/240-fält -->
    <!--<xsl:template name="create-uniform">
        
    </xsl:template>-->
        
    <!-- Översätter ett subfield till ett annat -->
    <xsl:template name="translate-subfield">
        <xsl:param name="oldcode"/>
        <xsl:param name="newcode"/>
        <xsl:for-each select="subfield[@code = $oldcode]">
            <subfield code="{$newcode}"><xsl:value-of select="."/></subfield>
        </xsl:for-each>
    </xsl:template>
    
    <!-- Hämtar auktoritetsform -->
    <xsl:template name="map-authority">
        <xsl:param name="slba_name"/>
        <xsl:choose>
            <xsl:when test="$slba_name = 'Lindgren, Astrid'">Lindgren, Astrid, 1907-2002</xsl:when>
            <xsl:otherwise><xsl:value-of select="$slba_name"/></xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    
    <xsl:template name="replace-character">
        <xsl:param name="word"/>
        
        <xsl:variable name="char"><xsl:value-of select="substring($word,1,1)"/></xsl:variable>
        <xsl:variable name="char2"><xsl:value-of select="substring($word,2,1)"/></xsl:variable>
        <xsl:choose>
            <xsl:when test="$char != '0' and $char != '1' and $char != '2' and $char != '3' and $char != '4' and $char != '5' and $char != '6' and $char != '7' and $char != '8' and $char != '9'">u</xsl:when>
            <xsl:otherwise><xsl:value-of select="$char"/></xsl:otherwise>
        </xsl:choose>
        <xsl:if test="string-length($word) >= 2">
            <xsl:call-template name="replace-character">
                <xsl:with-param name="word" select="substring($word, 2)"/>
            </xsl:call-template>
        </xsl:if>
        <!--<xsl:choose>
            <xsl:when test="string-length($word) &lt; 2"></xsl:when>
            <xsl:otherwise>
                <xsl:call-template name="replace-character">
                    <xsl:with-param name="word" select="substring($word, 2)"/>
                </xsl:call-template>
            </xsl:otherwise>
        </xsl:choose>-->
    </xsl:template>
    
    <!-- ***** Kollar om ett ämnesord är av typen personnamn (Efternamn, Förnamn) -->
    <xsl:template name="check-name">
        <xsl:param name="kandidat"/>
        <xsl:choose>
            <xsl:when test="contains($kandidat, ', ')"><xsl:text> </xsl:text></xsl:when>
            <xsl:otherwise>0</xsl:otherwise>
        </xsl:choose>
    </xsl:template>
</xsl:stylesheet>
