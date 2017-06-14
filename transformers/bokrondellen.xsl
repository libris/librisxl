<?xml version="1.0" encoding="UTF-8" ?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:java="http://xml.apache.org/xslt/java" xmlns:lang="http://data.lang" exclude-result-prefixes="java">
    
    <xsl:output method="xml" indent="yes" encoding="UTF-8"/>
    
    <!-- transformation date -->
    <xsl:variable name="rawdatestamp"><xsl:value-of select="java:format(java:java.text.SimpleDateFormat.new('yyyy-MM-dd'), java:java.util.Date.new())"/></xsl:variable>
    <xsl:variable name="datestamp"><xsl:value-of select="substring(translate($rawdatestamp, '-', ''), 3, 6)"/></xsl:variable>
    
    <xsl:template match="/artikelregister">
        <collection xmlns="http://www.loc.gov/MARC21/slim">
            <xsl:apply-templates select="artikel"/>
        </collection>
    </xsl:template>
    
    <xsl:template match="artikel">
        <xsl:variable name="mediatyp" select="translate(normalize-space(mediatyp), 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')"/>
        <xsl:if test="$mediatyp != 'marknadsföringsmaterial' and $mediatyp != 'tidskrift'">        
        <record type="Bibliographic">
            <!-- language code -->
            <xsl:variable name="sprak_kod" select="normalize-space(sprak_kod)"/>
            <xsl:variable name="sprak" select="normalize-space(translate(sprak, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>            
            <xsl:variable name="langcodes" select="document('')/*/lang:langcodes/lang"/>
            <xsl:variable name="lang">
                <xsl:choose>
                    <xsl:when test="string-length($sprak_kod) = 3"><xsl:value-of select="$sprak_kod"/></xsl:when>
                    <xsl:when test="string-length($langcodes[@name = $sprak]) = 3"><xsl:value-of select="$langcodes[@name = $sprak]"/></xsl:when>
                    <xsl:otherwise>und</xsl:otherwise>
                </xsl:choose>
            </xsl:variable>
            <xsl:variable name="bandtyp" select="translate(normalize-space(bandtyp), 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')"/>
            <xsl:variable name="bandtyp_short">
                <xsl:choose>
                    <xsl:when test="$mediatyp = 'bok' and ($bandtyp = 'halvfranskt' or $bandtyp = 'halvklotband' or $bandtyp = 'inbunden' or $bandtyp = 'kartonnage' or $bandtyp = 'klotband')"><xsl:text> (inb.)</xsl:text></xsl:when>
                    <xsl:when test="$mediatyp = 'bok' and $bandtyp = 'spiral'"><xsl:text> (spiral)</xsl:text></xsl:when>
                    <xsl:when test="$mediatyp = 'bok'"></xsl:when>
                    <xsl:when test="bandtyp"><xsl:text> </xsl:text>(<xsl:value-of select="bandtyp"/>)</xsl:when>
                </xsl:choose>
            </xsl:variable>
            <xsl:variable name="katalogsignumContent">
                <xsl:choose>
                    <xsl:when test="katalogsignum[starts-with(normalize-space(.), 'X')]"><xsl:value-of select="katalogsignum[starts-with(., 'X')]"/></xsl:when>
                    <xsl:when test="katalogsignum[starts-with(normalize-space(.), 'uX')]"><xsl:value-of select="katalogsignum[starts-with(., 'uX')]"/></xsl:when>
                    <xsl:when test="katalogsignum[starts-with(normalize-space(.), 'H')]"><xsl:value-of select="katalogsignum[starts-with(., 'H')]"/></xsl:when> 
                    <xsl:when test="katalogsignum[starts-with(normalize-space(.), 'uH')]"><xsl:value-of select="katalogsignum[starts-with(., 'uH')]"/></xsl:when>
                    <xsl:otherwise><xsl:value-of select="katalogsignum[1]"/></xsl:otherwise>
                </xsl:choose>
            </xsl:variable>
            <xsl:variable name="leader_06">
                <xsl:choose>
                    <xsl:when test="$mediatyp = 'ljudbok' or ($mediatyp = 'nedladdningsbar bok' and $bandtyp = 'mp3-fil')">i</xsl:when>
                    <xsl:when test="$mediatyp = 'multimedia'">m</xsl:when>
                    <xsl:when test="$mediatyp = 'övrigt' and (starts-with($bandtyp, 'dvd') or starts-with($bandtyp, 'video'))">g</xsl:when>
                    <xsl:when test="$mediatyp = 'övrigt' and (starts-with($bandtyp, 'cd') or $bandtyp = 'ljud-cd')">i</xsl:when>
                    <xsl:when test="$mediatyp = 'övrigt' and (starts-with($bandtyp, 'ask') or starts-with($bandtyp, 'kort') or starts-with($bandtyp, 'pussel') or starts-with($bandtyp, 'spel') or starts-with($bandtyp, 'sällskapsspel') or starts-with($bandtyp, 'skyltställ') or starts-with($bandtyp, 'plastkasse'))">r</xsl:when>
                    <xsl:when test="$mediatyp = 'karta'">e</xsl:when>
                    <xsl:when test="$mediatyp = 'merchandise' and not($bandtyp = 'korsord' or starts-with($bandtyp, 'målarbok') or starts-with($bandtyp, 'pysselbok'))">r</xsl:when>
                    <xsl:otherwise>a</xsl:otherwise>
                </xsl:choose>
            </xsl:variable>
            <xsl:variable name="has020"><xsl:apply-templates select="artikelnummer" mode="check020"/></xsl:variable>
            <leader>*****n<xsl:value-of select="$leader_06"/>m a22*****8a 4500</leader>
            <xsl:call-template name="controlfield008">
                <xsl:with-param name="lang" select="$lang"/>
                <xsl:with-param name="mediatyp" select="$mediatyp"/>
                <xsl:with-param name="bandtyp" select="$bandtyp"/>
                <xsl:with-param name="katalogsignumContent" select="$katalogsignumContent"/>
                <xsl:with-param name="genre" select="normalize-space(translate(genre, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>
            </xsl:call-template>
            <xsl:apply-templates select="mediatyp"><xsl:with-param name="bandtyp" select="$bandtyp"/></xsl:apply-templates>
            <xsl:if test="not(contains($has020, 'True'))">
                <xsl:apply-templates select="isbn10[string-length(normalize-space(.)) = 10]"/>
            </xsl:if>
            <xsl:apply-templates select="artikelnummer"><xsl:with-param name="mediatyp" select="$mediatyp"/><xsl:with-param name="bandtyp" select="$bandtyp"/><xsl:with-param name="bandtyp_short" select="$bandtyp_short"/><xsl:with-param name="katalogsignumContent" select="$katalogsignumContent"/></xsl:apply-templates>
            <xsl:apply-templates select="ean[string-length(normalize-space(.)) = 13 and not(starts-with(normalize-space(.), '978')) and not(starts-with(normalize-space(.), '979'))]"><xsl:with-param name="bandtyp_short" select="$bandtyp_short"/></xsl:apply-templates>
            <xsl:apply-templates select="titel"><xsl:with-param name="mediatyp" select="$mediatyp"/><xsl:with-param name="bandtyp" select="$bandtyp"/><xsl:with-param name="katalogsignumContent" select="$katalogsignumContent"/></xsl:apply-templates>
            <xsl:if test="not(titel)">
                <xsl:apply-templates select="arbetstitel"/>
            </xsl:if>
            <xsl:apply-templates select="upplagenummer[string(number(normalize-space(.))) !='NaN' and number(normalize-space(.)) > 1]"/>
            <xsl:apply-templates select="forlag"/>
            <xsl:apply-templates select="utgivningsdatum[string-length(normalize-space(.)) = 8]"/>
            <xsl:variable name="hojd">
                <xsl:if test="hojd[string(number(normalize-space(.))) != 'NaN' and normalize-space(.) != '0']"><xsl:value-of select="round(number(hojd) div 10)"/></xsl:if>
            </xsl:variable>
            <xsl:variable name="illusterad" select="normalize-space(translate(illustrerad, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>
            <xsl:if test="omfang[normalize-space(.) != ''] or omfang[@enhet != ''] or $illusterad = 'ja' or komponent[normalize-space(.) != ''] or ($hojd != '' and $hojd != '0')">
                <xsl:call-template name="create_300"><xsl:with-param name="illustrerad" select="$illusterad"/><xsl:with-param name="hojd" select="$hojd"/></xsl:call-template>
            </xsl:if>
            <!--<xsl:apply-templates select="uppdaterad"/>-->
            <xsl:apply-templates select="originalforlag"/>
            <xsl:apply-templates select="medarbetare"><xsl:with-param name="mediatyp" select="$mediatyp"/><xsl:with-param name="bandtyp" select="$bandtyp"></xsl:with-param></xsl:apply-templates>
            <xsl:apply-templates select="originaltitel[normalize-space(.) != '']"></xsl:apply-templates>
            <!--<xsl:apply-templates select="katalogsignum"/> -->
            <xsl:apply-templates select="genre[normalize-space(.) != 'Övrigt']"/>
            <xsl:apply-templates select="serie"/>
            <xsl:apply-templates select="innehall"/>
            <xsl:apply-templates select="kommentarfalt"/>
           <!-- <xsl:apply-templates select="utbildningsniva/beskrivning"/>-->
            <xsl:call-template name="create_040"/>
            <xsl:call-template name="create_amnesord"><xsl:with-param name="mediatyp" select="$mediatyp"/><xsl:with-param name="bandtyp" select="$bandtyp"/></xsl:call-template>
            <xsl:call-template name="create_599"/>
       </record>
       <record type="Holdings">
           <leader>*****nx  a22*****1n 4500</leader>
           <controlfield tag="008"><xsl:value-of select="$datestamp"/>||0000|||||000||||||000000</controlfield>
            <datafield ind1=" " ind2=" " tag="852">
                <subfield code="b">BOKR</subfield>
            </datafield>
            <!--<xsl:apply-templates select="artikelnummer" mode="create035holdings"/>-->
       </record>
        </xsl:if>
    </xsl:template>
    
    <!-- controlfield 007 -->
    <xsl:template name="controlfield007">
        <xsl:param name="controlfield_007_value"/>
        <controlfield tag="007"><xsl:value-of select="$controlfield_007_value"/></controlfield>
    </xsl:template>
    
    <!-- controlfield 008 -->
    <xsl:template name="controlfield008">
        <xsl:param name="lang"/>
        <xsl:param name="mediatyp"/>
        <xsl:param name="bandtyp"/>
        <xsl:param name="genre"/>
        <xsl:variable name="mb">
            <xsl:choose>
                <xsl:when test="$mediatyp='nedladdningsbar bok' and $bandtyp='e-bok'">ne</xsl:when>
                <xsl:when test="$mediatyp='nedladdningsbar bok' and $bandtyp='mp3-fil'">nm</xsl:when>
            </xsl:choose>
        </xsl:variable>
        <xsl:variable name="isBU">
            <xsl:if test="contains($genre, 'barn') or contains($genre, 'ungdom')">true</xsl:if>
        </xsl:variable>
        <!--<xsl:param name="katalogsignumContent"/>-->
        <xsl:variable name="controlfield_008_07-10" select="substring(utgivningsdatum, 1, 4)"/> 
        <xsl:variable name="controlfield_008_15-16">
                <xsl:choose>
                    <xsl:when test="starts-with(artikelnummer, '91') or starts-with(artikelnummer, '97891') or starts-with(artikelnummer, '9791') or starts-with(artikelnummer, '9801')">sw</xsl:when>
                    <xsl:otherwise>xx</xsl:otherwise>
                </xsl:choose>
        </xsl:variable>
        <xsl:variable name="controlfield_008_22">
            <xsl:choose>
                <xsl:when test="$isBU = 'true' and ($mb = 'ne' or $mb = 'nm' or $mediatyp = 'bok' or $mediatyp = 'ljudbok')">j</xsl:when>
                <xsl:otherwise><xsl:text> </xsl:text></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <!-- More 008 logic will be too hefty to implement, so let's skip this for now.
        <xsl:variable name="controlfield_008_29">
            <xsl:choose>
                <xsl:when test="$mediatyp = 'bok'">0</xsl:when>
                <xsl:otherwise><xsl:text> </xsl:text></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>-->
        <xsl:variable name="controlfield_008_23">
            <xsl:choose>
                <xsl:when test="$mediatyp = 'ljudbok' and (starts-with($bandtyp, 'minneskort') or $bandtyp = 'mp3-spelare med fil')">q</xsl:when>
                <xsl:when test="$mediatyp = 'multimedia'">q</xsl:when>
                <xsl:when test="$mediatyp = 'nedladdningsbar bok'">o</xsl:when>
                <xsl:when test="$mediatyp = 'övrigt' and $bandtyp = 'onlineprodukt'">o</xsl:when>
                <xsl:when test="$mediatyp = 'onlineprodukt'">o</xsl:when>
                <xsl:when test="$mediatyp = 'karta'">|</xsl:when>
                <xsl:otherwise><xsl:text> </xsl:text></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <xsl:variable name="controlfield_008_24-34">
            <xsl:choose>
                <xsl:when test="$mediatyp = 'bok'"><xsl:text>    |00| 0 </xsl:text></xsl:when>
                <xsl:otherwise><xsl:text>    |||| ||</xsl:text></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <!--<xsl:variable name="controlfield_008_33">
            <xsl:choose>
                <xsl:when test="starts-with($katalogsignumContent, 'H') or starts-with($katalogsignumContent, 'uH')">1</xsl:when>
                <xsl:otherwise>0</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>-->
        <!--<xsl:variable name="inline_pos30-31">
            <xsl:choose>
                <xsl:when test="$mediatyp = 'bok' and starts-with($katalogsignumContent, 'X')">nn</xsl:when>
                <xsl:when test="$mediatyp = 'ljudbok' and $controlfield_008_33 = 1">f|</xsl:when>
                <xsl:otherwise>||</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>-->
        <xsl:variable name="controlfield_008_18-34">
                <xsl:choose>
                    <xsl:when test="$mediatyp = 'ljudbok' or ($mediatyp = 'nedladdningsbar bok' and $bandtyp = 'mp3-fil')"><xsl:text>||||</xsl:text><xsl:value-of select="$controlfield_008_22"/><xsl:value-of select="$controlfield_008_23"/><xsl:text>|||||||| | </xsl:text></xsl:when>
                    <xsl:when test="$mediatyp = 'merchandise' and (starts-with($bandtyp, 'målarbok') or starts-with($bandtyp, 'korsord') or starts-with($bandtyp, 'pysselbok'))"><xsl:text>||||</xsl:text><xsl:value-of select="$controlfield_008_22"/><xsl:value-of select="$controlfield_008_23"/><xsl:text>    |||| ||</xsl:text></xsl:when>
                    <xsl:when test="($mediatyp = 'övrigt' and ($bandtyp = 'dvd' or starts-with($bandtyp ,'ask') or starts-with($bandtyp, 'kort') or starts-with($bandtyp, 'pussel') or starts-with($bandtyp, 'spel') or starts-with($bandtyp, 'sällskapsspel') or starts-with($bandtyp, 'skyltställ') or starts-with($bandtyp, 'plastkasse'))) or $mediatyp = 'merchandise'"><xsl:text>||| </xsl:text><xsl:value-of select="$controlfield_008_22"/><xsl:value-of select="$controlfield_008_23"/><xsl:text>    |    ||</xsl:text></xsl:when>
                    <xsl:when test="$mediatyp = 'övrigt' and $bandtyp = 'cd'"><xsl:text>||||</xsl:text><xsl:value-of select="$controlfield_008_22"/><xsl:value-of select="$controlfield_008_23"/><xsl:text>|||||||| | </xsl:text></xsl:when>
                    <xsl:when test="$mediatyp = 'bok' or $mediatyp = 'nedladdningsbar bok' or $mediatyp='övrigt' or $mediatyp = 'onlineprodukt'"><xsl:text>||||</xsl:text><xsl:value-of select="$controlfield_008_22"/><xsl:value-of select="$controlfield_008_23"/><xsl:value-of select="$controlfield_008_24-34"/></xsl:when>
                    <xsl:when test="$mediatyp = 'multimedia'"><xsl:text>||||</xsl:text><xsl:value-of select="$controlfield_008_22"/><xsl:value-of select="$controlfield_008_23"/><xsl:text>  u |      </xsl:text></xsl:when>
                    <xsl:when test="$mediatyp = 'karta'"><xsl:text>|||||| |  |  | ||</xsl:text></xsl:when>
                    
                    <!--
                    <xsl:when test="$mediatyp = 'ljudbok' or ($mediatyp = 'övrigt' and (starts-with($bandtyp, 'cd') or $bandtyp = 'ljud-cd')) or ($mediatyp = 'bok' and starts-with($katalogsignumContent, 'X'))">
                        <xsl:text>||||</xsl:text><xsl:value-of select="$controlfield_008_22"/><xsl:value-of select="$controlfield_008_23"/><xsl:text>||||||</xsl:text><xsl:value-of select="$inline_pos30-31"/><xsl:text> | </xsl:text>
                    </xsl:when>
                    <xsl:when test="$mediatyp = 'övrigt' and ($bandtyp = 'dvd' or $bandtyp = 'ask' or $bandtyp = 'kort' or $bandtyp = 'kort/tarot' or $bandtyp = 'pussel' or $bandtyp = 'spel' or $bandtyp = 'sällskapsspel' or $bandtyp = 'video')">
                        <xsl:text>||| </xsl:text><xsl:value-of select="$controlfield_008_22"/><xsl:value-of select="$controlfield_008_23"/><xsl:text>    |    ||</xsl:text>    
                    </xsl:when>
                    <xsl:when test="($mediatyp = 'övrigt' and $bandtyp = 'onlineprodukt') or $mediatyp = 'onlineprodukt'">
                        <xsl:text>    </xsl:text><xsl:value-of select="$controlfield_008_22"/><xsl:value-of select="$controlfield_008_23"/><xsl:text>  u |      </xsl:text> 
                    </xsl:when>
                    <xsl:when test="$mediatyp = 'bok' or $mediatyp = 'nedladdningsbar bok' or $mediatyp = 'övrigt' or ($mediatyp = 'merchandise' and (starts-with($bandtyp, 'målarbok') or starts-with($bandtyp, 'pysselbok') or $bandtyp = 'korsord'))">
                        <xsl:text>||||</xsl:text><xsl:value-of select="$controlfield_008_22"/><xsl:value-of select="$controlfield_008_23"/><xsl:text>    |||| </xsl:text><xsl:value-of select="$controlfield_008_33"/><xsl:text>|</xsl:text>
                    </xsl:when>
                    <xsl:when test="$mediatyp = 'merchandise'">
                        <xsl:text>||| </xsl:text><xsl:value-of select="$controlfield_008_2 f2"/><xsl:value-of select="$controlfield_008_23"/><xsl:text>    |    ||</xsl:text>
                    </xsl:when>
                    <xsl:when test="$mediatyp = 'multimedia'">
                        <xsl:text>    </xsl:text><xsl:value-of select="$controlfield_008_22"/><xsl:value-of select="$controlfield_008_23"/><xsl:text>  u |      </xsl:text>
                    </xsl:when>
                    <xsl:when test="$mediatyp = 'karta'">
                        <xsl:text>|||||</xsl:text><xsl:value-of select="$controlfield_008_23"/><xsl:text> |  |  | ||</xsl:text>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:text>||||</xsl:text><xsl:value-of select="$controlfield_008_22"/><xsl:value-of select="$controlfield_008_23"/><xsl:text>    |||| </xsl:text><xsl:value-of select="$controlfield_008_33"/><xsl:text>|</xsl:text>
                    </xsl:otherwise>-->
                </xsl:choose>
        </xsl:variable>        
        <controlfield tag="008"><xsl:value-of select="$datestamp"/>s<xsl:value-of select="$controlfield_008_07-10"/><xsl:text>    </xsl:text><xsl:value-of select="$controlfield_008_15-16"/><xsl:text> </xsl:text><xsl:value-of select="$controlfield_008_18-34"/><xsl:value-of select="$lang"/><xsl:text> d</xsl:text></controlfield>
    </xsl:template>

    <!-- mediatyp, controlfield 007 -->
    <xsl:template match="mediatyp">
        <xsl:param name="bandtyp"/>
        <xsl:variable name="controlfield_007_01_s">
                <xsl:choose>
                    <xsl:when test="normalize-space(.) = 'Ljudbok' and $bandtyp = 'mp3-spelare med fil'">u</xsl:when>
                    <xsl:when test="starts-with($bandtyp, 'cd') or starts-with($bandtyp, 'dvd') or (normalize-space(.) = 'Ljudbok' and starts-with($bandtyp, 'mp3'))">d</xsl:when> 
                    <xsl:when test="starts-with($bandtyp, 'kassettbok')">s</xsl:when>
                    <xsl:otherwise>u</xsl:otherwise>
                </xsl:choose>
        </xsl:variable>
        <xsl:variable name="controlfield_007_01_c">
            <xsl:choose>
                <xsl:when test="normalize-space(.) = 'Onlineprodukt' or normalize-space(.) = 'Övrigt' and starts-with($bandtyp, 'onlineprodukt') or normalize-space(.) = 'Nedladdningsbar bok'">r</xsl:when> 
                <xsl:when test="normalize-space(.) = 'Multimedia'">o</xsl:when>
                <xsl:when test="normalize-space(.) = 'Ljudbok'and (starts-with($bandtyp, 'minneskort') or $bandtyp = 'mp3-spelare med fil')">k</xsl:when>
            </xsl:choose>
        </xsl:variable>
        <xsl:if test="normalize-space(.) = 'Ljudbok' or (normalize-space(.) = 'Nedladdningsbar bok' and $bandtyp = 'mp3-fil')"><xsl:call-template name="controlfield007"><xsl:with-param name="controlfield_007_value">s<xsl:value-of select="$controlfield_007_01_s"/>||||||||||||</xsl:with-param></xsl:call-template></xsl:if>
        <xsl:if test="normalize-space(.) = 'Onlineprodukt' or normalize-space(.) = 'Nedladdningsbar bok' or normalize-space(.) = 'Multimedia' or (normalize-space(.) = 'Ljudbok'and (starts-with($bandtyp, 'minneskort') or $bandtyp = 'mp3-spelare med fil')) or (normalize-space(.) = 'Övrigt' and starts-with($bandtyp, 'onlineprodukt'))"><xsl:call-template name="controlfield007"><xsl:with-param name="controlfield_007_value">c<xsl:value-of select="$controlfield_007_01_c"/>||||||||||||</xsl:with-param></xsl:call-template></xsl:if>
        <xsl:if test="normalize-space(.) = 'Övrigt' and (starts-with($bandtyp, 'dvd') or starts-with($bandtyp, 'video'))"><xsl:call-template name="controlfield007"><xsl:with-param name="controlfield_007_value">v||||||||</xsl:with-param></xsl:call-template></xsl:if>
        <xsl:if test="normalize-space(.) = 'Övrigt' and (starts-with($bandtyp, 'cd') or $bandtyp = 'ljud-cd')"><xsl:call-template name="controlfield007"><xsl:with-param name="controlfield_007_value">sd||||||||||||</xsl:with-param></xsl:call-template></xsl:if>
        <xsl:if test="normalize-space(.) = 'Karta' and $bandtyp = 'jordglob'"><xsl:call-template name="controlfield007"><xsl:with-param name="controlfield_007_value">d|||||</xsl:with-param></xsl:call-template></xsl:if>
    </xsl:template>
    
    <!-- artikelnummer, 020, 024, 035 -->
    <xsl:template match="artikelnummer">
	   <xsl:param name="mediatyp"/>
	   <xsl:param name="bandtyp"/>
        <xsl:param name="bandtyp_short"/>
        <!--<xsl:param name="katalogsignumContent"/>-->
        <xsl:variable name="sub3" select="number(substring(normalize-space(.), 1, 3))"/>
        <xsl:variable name="tag">
            <xsl:choose>
                <xsl:when test="substring(normalize-space(.), 1, 4) = '9790' and string-length(normalize-space(.)) = 13">024</xsl:when>
                <xsl:when test="string($sub3) != 'NaN' and $sub3 >= 978 and string-length(normalize-space(.)) = 13">020</xsl:when>
                <xsl:otherwise></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <xsl:variable name="ind1">
            <xsl:choose>
                <xsl:when test="$tag = '024'">2</xsl:when>
                <xsl:otherwise><xsl:text> </xsl:text></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <xsl:if test="string-length($tag) = 3">
            <datafield ind1="{$ind1}" ind2=" " tag="{$tag}">
                <subfield code="a"><xsl:value-of select="normalize-space(.)"/><xsl:value-of select="$bandtyp_short"/></subfield>
            </datafield>
        </xsl:if>
        <datafield ind1=" " ind2=" " tag="035">
            <subfield code="a">(BOKR)<xsl:value-of select="normalize-space(.)"/></subfield>
        </datafield>
    </xsl:template>
    
    <!-- artikelnummer, 020 -->
    <xsl:template match="artikelnummer" mode="tag020">
        <xsl:param name="bandtyp_short"/>
        <datafield ind1=" " ind2=" " tag="020">
            <subfield code="a"><xsl:value-of select="normalize-space(.)"/><xsl:value-of select="$bandtyp_short"/></subfield>
        </datafield>
    </xsl:template>
    
    <xsl:template match="artikelnummer" mode="check020">
        <xsl:variable name="sub3" select="number(substring(normalize-space(.), 1, 3))"/>
        <xsl:choose>
            <xsl:when test="substring(normalize-space(.), 1, 4) = '9790' and string-length(normalize-space(.)) = 13">False</xsl:when>
            <xsl:when test="string($sub3) != 'NaN' and $sub3 >= 978 and string-length(normalize-space(.)) = 13">True</xsl:when>
            <xsl:otherwise>False</xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    
    <!--<xsl:template match="artikelnummer" mode="create035holdings">
        <datafield ind1= " " ind2=" " tag="035">
            <subfield code="a">(BOKR)<xsl:value-of select="normalize-space(.)"/></subfield>
        </datafield>    
    </xsl:template>-->
    
    <!-- isbn10, 022 -->
    <xsl:template match="isbn10">
        <datafield ind1=" " ind2=" " tag="020">
            <subfield code="a">
                <xsl:value-of select="normalize-space(.)"/>
            </subfield>
        </datafield>
    </xsl:template>

    <!-- ean, 024 -->
    <xsl:template match="ean">
        <xsl:param name="bandtyp_short"/>
        <datafield ind1="3" ind2=" " tag="024">
            <subfield code="a">
                <xsl:value-of select="normalize-space(.)"/><xsl:value-of select="$bandtyp_short"/>
            </subfield>
        </datafield>
    </xsl:template>

    <!-- artikelnummer, 035 -->
    <!--<xsl:template match="artikelnummer" mode="tag035">
        <datafield ind1=" " ind2=" " tag="035">
            <subfield code="a">
                <xsl:value-of select="normalize-space(.)"/>
            </subfield>
        </datafield>
    </xsl:template>-->
    
    <!-- 040 -->
    <xsl:template name="create_040">
        <datafield ind1=" " ind2=" " tag="040">
            <subfield code="a">BOKR</subfield>   
        </datafield>
    </xsl:template>
    
    <!-- bic, 072 -->
    <!--<xsl:template match="bic">
        <datafield ind1=" " ind2="7" tag="072">
            <subfield code="a">
                <xsl:value-of select="normalize-space(.)"/>
            </subfield>
            <subfield code="2">bicssc</subfield>
        </datafield>
    </xsl:template>-->
    
    <!-- katalogsignum, 084, 082 -->
  <!--  <xsl:template match="katalogsignum">
        <xsl:variable name="tmp" select="substring(normalize-space(.), 1, 1)"/>
        <xsl:variable name="katsignumpos2" select="substring(normalize-space(.), 2, 1)"/>
        <xsl:variable name="katsignumpos1">
            <xsl:choose>
                <xsl:when test="contains('ABCDEFGHIJKLMNOPQRSTUVXYZÅÄÖ', $tmp)">True</xsl:when>
                <xsl:otherwise><xsl:value-of select="$tmp"/></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>        
        <xsl:if test="(contains($katsignumpos1, 'True')) or ($katsignumpos1 = 'u' and contains('ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', $katsignumpos2))">
            <datafield ind1=" " ind2=" " tag="084">
                <subfield code="a">
                    <xsl:value-of select="normalize-space(.)"/>
                </subfield>
                <subfield code="2"><xsl:text>kssb/8</xsl:text></subfield>
            </datafield>
        </xsl:if>-->
        <!--<xsl:if test="contains('0123456789', $tmp)">
            <datafield ind1="0" ind2="4" tag="082">
                <subfield code="a">
                    <xsl:value-of select="normalize-space(.)"/>
                </subfield>
                <subfield code="2">23/swe</subfield>
            </datafield>            
        </xsl:if>-->
    <!--</xsl:template>-->
    
    <!-- medarbetare, 100, 600, 700, 710 -->
    <xsl:template match="medarbetare[@typ = 'forfattare'][1]">
        <xsl:param name="mediatyp"/>
        <xsl:variable name="ind1_tmp">
            <xsl:choose>
                <xsl:when test="contains(normalize-space(.), ', ')">1</xsl:when>
                <xsl:otherwise>0</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <datafield ind1="{$ind1_tmp}" ind2=" " tag="100">
            <subfield code="a"><xsl:value-of select="normalize-space(.)"/></subfield>
            <xsl:if test="$mediatyp != 'bok'">
                <subfield code="4">aut</subfield>
            </xsl:if>
        </datafield>
    </xsl:template>
    
    <xsl:template match="medarbetare[@typ = 'forfattare'][position() > 1]">
        <xsl:variable name="ind1tmp">
            <xsl:choose>
                <xsl:when test="contains(normalize-space(.), ', ')">1</xsl:when>
                <xsl:otherwise>0</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <datafield ind1="{$ind1tmp}" ind2=" " tag="700">
            <subfield code="a">
                <xsl:value-of select="normalize-space(.)"/>
            </subfield>
            <subfield code="4">aut</subfield>
        </datafield>
    </xsl:template>
    
    <xsl:template match="medarbetare[@typ = 'bibliografisk person']">
        <xsl:variable name="i1">
            <xsl:choose>
                <xsl:when test="contains(normalize-space(.), ', ')">1</xsl:when>
                <xsl:otherwise>0</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <datafield ind1="{$i1}" ind2="4" tag="600">
            <subfield code="a">
                <xsl:value-of select="normalize-space(.)"/>
            </subfield> 
        </datafield>
    </xsl:template>
    
    <xsl:template match="medarbetare[@typ != 'forfattare' and @typ != 'institution' and @typ != 'bibliografisk person' and @typ != 'formgivare' and @typ != 'pseudonym']">
        <xsl:param name="mediatyp"/>
        <xsl:variable name="ind1">
            <xsl:choose>
                <xsl:when test="contains(normalize-space(.), ', ')">1</xsl:when>
                <xsl:otherwise>0</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <xsl:if test="$mediatyp != 'bok' or ($mediatyp = 'bok' and @typ = 'redaktor')">
            <datafield ind1="{$ind1}" ind2=" " tag="700">
                <subfield code="a">
                    <xsl:value-of select="normalize-space(.)"/>
                </subfield>
                <xsl:variable name="type">
                    <xsl:call-template name="getType"><xsl:with-param name="type" select="normalize-space(@typ)"></xsl:with-param></xsl:call-template>
                </xsl:variable>
                <subfield code="4"><xsl:value-of select="$type"/></subfield>
            </datafield>
        </xsl:if>
    </xsl:template>
    
    <xsl:template match="medarbetare[@typ = 'institution']">
        <xsl:param name="mediatyp"/>
        <xsl:if test="$mediatyp != 'bok'">
            <datafield ind1="2" ind2=" " tag="710">
                <subfield code="a">
                    <xsl:value-of select="."/>
                </subfield>
            </datafield>
        </xsl:if>
    </xsl:template>
    
    <!-- originaltitel, 240 -->
    <xsl:template match="originaltitel">
        <xsl:variable name="lastpos" select="substring(normalize-space(.), string-length(normalize-space(.)), 1)"/>
        <xsl:choose>
            <xsl:when test="../medarbetare[@typ = 'forfattare']">
                <datafield ind1="1" ind2="0" tag="240">
                    <subfield code="a">
                        <xsl:value-of select="normalize-space(.)"/><xsl:if test="$lastpos != '.'"><xsl:text>.</xsl:text></xsl:if>
                    </subfield>
                    <xsl:if test="../sprak">
                    <subfield code="l">
                        <xsl:value-of select="../sprak"/>
                    </subfield>
                    </xsl:if>
                </datafield>
            </xsl:when>
            <xsl:otherwise>
                <datafield ind1="0" ind2=" " tag="130">
                    <subfield code="a">
                        <xsl:value-of select="normalize-space(.)"/><xsl:if test="$lastpos != '.'"><xsl:text>.</xsl:text></xsl:if>
                    </subfield>
                    <xsl:if test="../sprak">
                    <subfield code="l">
                        <xsl:value-of select="../sprak"/>
                    </subfield>
                    </xsl:if>
                </datafield>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    
    <!-- titel, 245 -->
    <xsl:template match="titel">
        <xsl:param name="mediatyp"/>
        <xsl:param name="bandtyp"/>
        <!--<xsl:param name="katalogsignumContent"/>-->
        <xsl:variable name="AMT">
            <xsl:choose>
                <!--<xsl:when test="$mediatyp = 'bok' and starts-with($katalogsignumContent, 'X')">[Musiktryck]</xsl:when>-->
                <xsl:when test="$mediatyp = 'ljudbok'"> 
                    <xsl:choose>
                        <xsl:when test="starts-with($bandtyp, 'minneskort') or $bandtyp = 'mp3-spelare med fil'">[Elektronisk resurs]</xsl:when>
                        <xsl:otherwise>[Ljudupptagning]</xsl:otherwise>
                    </xsl:choose>
                </xsl:when>
                <xsl:when test="$mediatyp = 'multimedia' or $mediatyp = 'nedladdningsbar bok' or $mediatyp = 'onlineprodukt'">[Elektronisk resurs]</xsl:when>
                <xsl:when test="$mediatyp = 'karta'">[Kartografiskt material]</xsl:when>
                <xsl:when test="$mediatyp = 'övrigt'">
                    <xsl:choose>
                        <xsl:when test="$bandtyp = 'onlineprodukt'">[Elektronisk resurs]</xsl:when>
                        <xsl:when test="$bandtyp = 'dvd' or $bandtyp = 'video' or $bandtyp = 'videokassett'">[Videoupptagning]</xsl:when>
                        <xsl:when test="starts-with($bandtyp, 'cd') or $bandtyp = 'ljud-cd'">[Ljudupptagning]</xsl:when>
                        <xsl:when test="starts-with($bandtyp, 'ask') or starts-with($bandtyp, 'kort') or $bandtyp = 'pussel' or $bandtyp = 'spel' or $bandtyp = 'sällskapsspel' or $bandtyp = 'skyltställ' or $bandtyp = 'plastkasse'">[Föremål]</xsl:when>
                    </xsl:choose>
                </xsl:when>
                <xsl:otherwise></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <xsl:variable name="title_firstword" select="substring-before(normalize-space(.), ' ')"/>
        <xsl:variable name="apos">L'</xsl:variable>
        <xsl:variable name="ind2">
            <xsl:choose>
                <xsl:when test="$title_firstword = 'A' or starts-with(normalize-space(.), $apos)">2</xsl:when>
                <xsl:when test="$title_firstword = 'De' or $title_firstword = 'En' or $title_firstword = 'La' or $title_firstword = 'Le' or $title_firstword = 'Un'">3</xsl:when>
                <xsl:when test="$title_firstword = 'Den' or $title_firstword = 'Det' or $title_firstword = 'Das' or $title_firstword = 'Der' or $title_firstword = 'Die' or $title_firstword = 'Ein' or $title_firstword = 'Ett' or $title_firstword = 'Les' or $title_firstword = 'Une' or $title_firstword = 'The'">4</xsl:when>        
                <xsl:when test="$title_firstword = 'Eine'">5</xsl:when>
                <xsl:otherwise>0</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <datafield ind1="1" ind2="{$ind2}" tag="245">
            <subfield code="a"><xsl:value-of select="normalize-space(.)"/></subfield>
            <xsl:if test="$AMT != ''">
                <subfield code="h"><xsl:value-of select="$AMT"/></subfield>
            </xsl:if>
        </datafield>
    </xsl:template>

    <!-- titel, 245 -->
    <xsl:template match="arbetstitel">
        <datafield ind1="1" ind2=" " tag="245">
            <subfield code="a">
                <xsl:value-of select="normalize-space(.)"/>
            </subfield>
        </datafield>
    </xsl:template>

    <!-- upplagenummer, 250 -->
    <xsl:template match="upplagenummer">
        <datafield ind1=" " ind2=" " tag="250">
            <subfield code="a">
                <xsl:value-of select="normalize-space(.)"/><xsl:text>. uppl.</xsl:text>
            </subfield>
        </datafield>
    </xsl:template>

    <!-- forlag, 260 -->
    <xsl:template match="forlag">
        <xsl:variable name="utgivningsdatum" select="string(../utgivningsdatum[string-length(normalize-space(.)) = 8])"/>
        <datafield ind1=" " ind2=" " tag="260">
            <subfield code="b">
                <xsl:value-of select="normalize-space(.)"/>
                <xsl:if test="$utgivningsdatum != ''"><xsl:text>,</xsl:text></xsl:if>
            </subfield>
            <xsl:if test="$utgivningsdatum != ''">
                <subfield code="c">
                    <xsl:value-of select="substring($utgivningsdatum, 1, 4)"/>
                </subfield>
            </xsl:if>
        </datafield>
    </xsl:template>

    <!-- utgivingsdatum, 263 -->
    <xsl:template match="utgivningsdatum">
        <datafield ind1=" " ind2=" " tag="263">
            <subfield code="a">
                <xsl:value-of select="substring(normalize-space(.), 1, 6)"/>
            </subfield>
        </datafield>
    </xsl:template>

    <!-- generate 300 -->
    <xsl:template name="create_300">
        <xsl:param name="illustrerad"/>
        <xsl:param name="hojd"/>
        <xsl:variable name="omfang">
            <xsl:choose>
                <xsl:when test="normalize-space(omfang) != ''"><xsl:value-of select="normalize-space(omfang)"/><xsl:text> </xsl:text></xsl:when>
                <xsl:otherwise></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>         
        <xsl:variable name="enhet"><xsl:apply-templates select="omfang"/></xsl:variable>
        <xsl:variable name="komponent" select="normalize-space(komponent)"/>
        <datafield ind1=" " ind2=" " tag="300">
            <xsl:if test="$omfang != '' or $enhet != ''">
                <subfield code="a">
                    <xsl:value-of select="$omfang"/><xsl:value-of select="$enhet"/><xsl:choose><xsl:when test="$illustrerad = 'ja'"><xsl:text> :</xsl:text></xsl:when><xsl:when test="$hojd != '' and $hojd != '0'"><xsl:text> ;</xsl:text></xsl:when><xsl:when test="$komponent != ''"><xsl:text> +</xsl:text></xsl:when></xsl:choose>
                </subfield>
            </xsl:if>
            <xsl:if test="$illustrerad = 'ja'">
                <subfield code="b">ill.<xsl:choose><xsl:when test="$hojd != '' and $hojd != '0'"><xsl:text> ;</xsl:text></xsl:when><xsl:when test="$komponent != ''"><xsl:text> +</xsl:text></xsl:when></xsl:choose></subfield>
            </xsl:if>
            <xsl:if test="$hojd != '' and $hojd != '0'">               
                <subfield code="c"><xsl:value-of select="$hojd"/><xsl:text> cm</xsl:text><xsl:if test="$komponent != ''"><xsl:text> +</xsl:text></xsl:if></subfield>
            </xsl:if>
            <xsl:if test="$komponent != ''">
                <subfield code="e"><xsl:value-of select="$komponent"/></subfield>
            </xsl:if>
        </datafield>
    </xsl:template>
    
    <!-- omfang enhet, 300 -->
    <xsl:template match="omfang">
        <xsl:variable name="tmp" select="normalize-space(translate(@enhet, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö'))"/>
        <xsl:choose>
            <xsl:when test="$tmp = 'sidor'">s.</xsl:when>
            <xsl:when test="$tmp = 'sekunder'">sek.</xsl:when>
            <xsl:when test="$tmp = 'cd'">CD</xsl:when>
            <xsl:when test="$tmp = 'dvd'">DVD</xsl:when>
            <xsl:otherwise><xsl:value-of select="$tmp"/></xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    
    <!-- serie, 490 -->
    <xsl:template match="serie">
        <datafield ind1="0" ind2=" " tag="490">
            <subfield code="a"><xsl:value-of select="normalize-space(.)"/></subfield>
        </datafield>
    </xsl:template>
    
    <!-- innehall, 505 -->
    <xsl:template match="innehall">
        <datafield ind1="0" ind2=" " tag="505">
            <subfield code="a"><xsl:value-of select="normalize-space(.)"/></subfield>
        </datafield>
    </xsl:template>
    
    <!-- lasordning, 490 subfield v -->
    <!--<xsl:template match="lasordning">
        <subfield code="v"><xsl:value-of select="normalize-space(.)"/></subfield>
    </xsl:template>-->
    
    <!-- originalforlag, 588 -->
    <xsl:template match="originalforlag">
        <datafield ind1=" " ind2=" " tag="588">
            <subfield code="a"><xsl:text>BOKR: Ursprungligen utgiven av </xsl:text><xsl:value-of select="string(normalize-space(.))"/><xsl:text></xsl:text></subfield>
       </datafield>
    </xsl:template>
    
    <!-- utbildningsniva, 521 -->
    <!--<xsl:template match="utbildningsniva/beskrivning">
        <datafield ind1="0" ind2=" " tag="521">
            <subfield code="a">
                <xsl:value-of select="normalize-space(.)"/>
            </subfield>
            <subfield code="b">
                <xsl:text>Bokrondellen</xsl:text>
            </subfield>
        </datafield>
        
    </xsl:template>-->
    
    <!-- aldersgrupp, 521 -->
    <!--<xsl:template match="aldersgrupp">
        <datafield ind1="1" ind2=" " tag="521">
            <xsl:choose>
                <xsl:when test="normalize-space(.) = 'Unga vuxna'">
                    <subfield code="a"><xsl:value-of select="normalize-space(.)"/></subfield>
                </xsl:when>
                <xsl:otherwise>
                    <subfield code="a"><xsl:value-of select="normalize-space(.)"/><xsl:text> år</xsl:text></subfield>
                </xsl:otherwise>
            </xsl:choose>
            <subfield code="b">Bokrondellen</subfield>
        </datafield>
    </xsl:template>-->
    
    <!-- uppdaterad, 599 -->
    <!--<xsl:template match="uppdaterad">
        <datafield ind1=" " ind2=" " tag="599">
            <subfield code="a">
                <xsl:text>Ändrad i Bokrondellen </xsl:text><xsl:value-of select="substring(normalize-space(.), 1, 8)"/>
            </subfield>
        </datafield>
    </xsl:template>-->
    
    <!-- kommentarfalt, 599 -->
    <xsl:template match="kommentarfalt">
        <datafield ind1=" " ind2=" " tag="599">
            <subfield code="a"><xsl:value-of select="normalize-space(.)"/></subfield>
        </datafield>
    </xsl:template>
    
    <!-- generate 599 -->
    <xsl:template name="create_599">
        <datafield ind1=" " ind2=" " tag="599">
            <subfield code="a">Maskinellt genererad post. Ändra kod för fullständighetsnivå (leader/17), annars kommer manuellt gjorda ändringar att försvinna.</subfield>   
        </datafield>
    </xsl:template>
    
    <!-- amnesord, 655, 653 -->
    <xsl:template name="create_655">
        <xsl:param name="mediatyp"/>
        <xsl:param name="bandtyp"/>
        <xsl:choose>
            <xsl:when test="$mediatyp = 'ljudbok' or ($mediatyp = 'övrigt' and ($bandtyp = 'ljud-cd' or starts-with($bandtyp, 'cd'))) or ($mediatyp = 'nedladdningsbar bok' and starts-with($bandtyp, 'mp3'))">
                <datafield ind1=" " ind2="7" tag="655">
                    <subfield code="a">Ljudböcker</subfield>
                    <subfield code="2">saogf</subfield>
                </datafield>
            </xsl:when>
            <xsl:when test="$mediatyp = 'multimedia'">
                <datafield ind1=" " ind2="4" tag="655">
                    <subfield code="a">Multimedia</subfield>
                </datafield>
            </xsl:when>
            <xsl:when test="$mediatyp = 'nedladdningsbar bok'">
                <datafield ind1=" " ind2="4" tag="655">
                    <xsl:choose>
                        <xsl:when test="$bandtyp = 'e-bok'"><subfield code="a">E-böcker</subfield></xsl:when>
                        <xsl:otherwise><subfield code="a">Nedladdningsbara böcker</subfield></xsl:otherwise>
                    </xsl:choose>
                </datafield>
            </xsl:when>
            <xsl:when test="$mediatyp = 'onlineprodukt' or ($mediatyp = 'övrigt' and $bandtyp = 'onlineprodukt')">
                <datafield ind1=" " ind2="4" tag="655">
                    <subfield code="a">Onlineprodukter</subfield>
                </datafield>
            </xsl:when>
            <xsl:when test="$mediatyp = 'merchandise'">
                <datafield ind1=" " ind2="4" tag="655">
                    <subfield code="a">Merchandise</subfield>
                </datafield>
            </xsl:when>            
        </xsl:choose>
    </xsl:template>
    
    <xsl:template name="create_amnesord">
        <xsl:param name="mediatyp"/>
        <xsl:param name="bandtyp"/>
            <xsl:choose>
               <xsl:when test="amnesord"><xsl:apply-templates select="amnesord"><xsl:with-param name="mediatyp" select="$mediatyp"/><xsl:with-param name="bandtyp" select="$bandtyp"/></xsl:apply-templates></xsl:when>
               <xsl:otherwise>
                   <xsl:call-template name="create_655"><xsl:with-param name="mediatyp" select="$mediatyp"/><xsl:with-param name="bandtyp" select="$bandtyp"/></xsl:call-template>              
               </xsl:otherwise>
           </xsl:choose> 
    </xsl:template>
    
    <xsl:template match="amnesord">
        <xsl:param name="mediatyp"/>
        <xsl:param name="bandtyp"/>
        <xsl:variable name="same_as_655">
            <xsl:choose>
                <xsl:when test="$mediatyp = 'ljudbok' and normalize-space(.) = 'Ljudböcker'">True</xsl:when>
                <xsl:when test="$mediatyp = 'övrigt' and $bandtyp = 'ljud-cd' and normalize-space(.) = 'Ljudböcker'">True</xsl:when>
                <xsl:when test="$mediatyp = 'multimedia' and normalize-space(.) = 'Multimedia'">True</xsl:when>
                <xsl:when test="$mediatyp = 'nedladdningsbar bok' and $bandtyp = 'mp3-fil'and normalize-space(.) = 'Ljudböcker'">True</xsl:when>
                <xsl:when test="$mediatyp = 'nedladdningsbar bok' and $bandtyp = 'e-bok'and normalize-space(.) = 'E-böcker'">True</xsl:when>
                <xsl:when test="$mediatyp = 'nedladdningsbar bok' and normalize-space(.) = 'Nedladdningsbara böcker'">True</xsl:when>
                <xsl:when test="($mediatyp = 'onlineprodukt' or ($mediatyp = 'övrigt' and $bandtyp = 'onlineprodukt')) and normalize-space(.) = 'Onlineprodukter'">True</xsl:when>
                <xsl:when test="$mediatyp = 'merchandise' and normalize-space(.) = 'Merchandise'">True</xsl:when>
                <xsl:otherwise>False</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <xsl:variable name="pos" select="position()"/>
        <xsl:if test="$pos = '1'">
            <xsl:call-template name="create_655"><xsl:with-param name="mediatyp" select="$mediatyp"/><xsl:with-param name="bandtyp" select="$bandtyp"/></xsl:call-template>
        </xsl:if>
        <!--<xsl:if test="$mediatyp != 'bok' and $same_as_655 != 'True'">
            <datafield ind1=" " ind2=" " tag="653">
                <subfield code="a"><xsl:value-of select="normalize-space(.)"/></subfield>
            </datafield>
        </xsl:if> -->
    </xsl:template>
    
    <!-- genre, 655 -->
    <xsl:template match="genre">
        <datafield ind1=" " ind2="4" tag="655">
            <subfield code="a"><xsl:value-of select="normalize-space(.)"/></subfield>
        </datafield>
    </xsl:template>
    
    <xsl:template name="getType">
        <xsl:param name="type"/>
        <xsl:choose>
            <xsl:when test="$type = 'oversattare'">trl</xsl:when>
            <xsl:when test="$type = 'fotograf'">pht</xsl:when>
            <xsl:when test="$type = 'illustrator'">ill</xsl:when>
            <xsl:when test="$type = 'formgivare'">dsr</xsl:when>
            <xsl:when test="$type = 'redaktor'">edt</xsl:when>
            <xsl:when test="$type = 'upplasare'">nrt</xsl:when>
            <xsl:when test="$type = 'artist'">art</xsl:when>
            <xsl:when test="$type = 'kompositor'">cmp</xsl:when>
            <xsl:otherwise>oth</xsl:otherwise>        
        </xsl:choose>
    </xsl:template>
    
    <lang:langcodes>
        <lang name="svenska">swe</lang>
        <lang name="engelska">eng</lang>
        <lang name="tyska">ger</lang>
        <lang name="franska">fra</lang>
        <lang name="danska">dan</lang>
        <lang name="italienska">ita</lang>
        <lang name="finska">fin</lang>
        <lang name="nederlandska">dut</lang>
        <lang name="ryska">rus</lang>
        <lang name="spanska">spa</lang>
        <lang name="polska">pol</lang>
        <lang name="norska">nor</lang>
        <lang name="ungerska">hun</lang>
        <lang name="tjeckiska">cze</lang>
        <lang name="portugisiska">por</lang>
        <lang name="japanska">jpn</lang>
        <lang name="kurdiska">kur</lang>
        <lang name="persiska">per</lang>
        <lang name="arabiska">ara</lang>
        <lang name="turkiska">tur</lang>
        <lang name="slovenska">slv</lang>
        <lang name="estniska">est</lang>
        <lang name="lettiska">lav</lang>
        <lang name="kroatiska">hrv</lang>
        <lang name="latin">lat</lang>
        <lang name="somali">som</lang>
        <lang name="nygrekiska">gre</lang>
        <lang name="nordsamiska">sme</lang>
        <lang name="esperanto">epo</lang>
   </lang:langcodes>
    
</xsl:stylesheet>