<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0" xmlns:java="http://xml.apache.org/xslt/java" xmlns:bic="http://bic" exclude-result-prefixes="java bic">
<xsl:output method="xml" indent="yes" encoding="UTF-8"/>
<!-- BIC->SAB 141127 Kai P -->

<xsl:key name="bk" match="bic:bics/bic:bic" use="@name"/>
<xsl:variable name="bic" select="document('./bic.xml')/bic:bics"/>
    
<!-- transformation date -->
<xsl:variable name="rawdatestamp"><xsl:value-of select="java:format(java:java.text.SimpleDateFormat.new('yyyy-MM-dd'), java:java.util.Date.new())"/></xsl:variable>
<xsl:variable name="datestamp"><xsl:value-of select="substring(translate($rawdatestamp, '-', ''), 3, 6)"/></xsl:variable>
   
<xsl:template match="products">
<collection xmlns="http://www.loc.gov/MARC21/slim">
<xsl:apply-templates select="product"/>
</collection>
</xsl:template>
    
<xsl:template match="product">
<!-- publication year -->
<xsl:variable name="publicationyear"> 
<xsl:choose>
<xsl:when test="normalize-space(publicationDate) != ''"><xsl:value-of select="substring(normalize-space(publicationDate), 1, 4)"/></xsl:when>
<xsl:when test="normalize-space(originalPublicationYear) != ''"><xsl:value-of select="substring(normalize-space(originalPublicationYear), 1, 4)"/></xsl:when>
<xsl:otherwise><xsl:text>    </xsl:text></xsl:otherwise>            
</xsl:choose>
</xsl:variable>
        
<!-- Bibliographic record -->
<record type="Bibliographic">
<xsl:apply-templates select="id"/>
<xsl:apply-templates select="isbn"/>
<xsl:apply-templates select="title"/>
<xsl:apply-templates select="type"><xsl:with-param name="pubyear" select="$publicationyear"/></xsl:apply-templates>
<xsl:apply-templates select="publisher"><xsl:with-param name="pubyear" select="$publicationyear"/></xsl:apply-templates>
<xsl:apply-templates select="authors/author"/>
<xsl:apply-templates select="category"/>
<xsl:apply-templates select="description"/>
<xsl:apply-templates select="authorDescription[normalize-space(.) != '']"/>
<xsl:apply-templates select="pages[number(normalize-space(.)) > 0]"/>
<xsl:apply-templates select="imageUrl"/>
<xsl:apply-templates select="lastModified/metaData"/>
<!--<datafield ind1=" " ind2=" " tag="599">
<subfield code="a">Maskinellt genererad post. Ändra kod för fullständighetsnivå (leader/17), annars kommer manuellt gjorda ändringar att försvinna.</subfield>
</datafield>-->
<datafield ind1=" " ind2="7" tag="072">
<subfield code="a"><xsl:value-of select="category[1]/@bic"/></subfield>
<subfield code="2">bicssc</subfield>
</datafield>
<datafield ind1=" " ind2=" " tag="538">
<subfield code="a">EPUB</subfield>
</datafield>
</record>
        
<!-- Holdings records -->
<xsl:call-template name="create_holdings"><xsl:with-param name="sigel_code">Publ</xsl:with-param><xsl:with-param name="datestamp"><xsl:value-of select="$datestamp"/></xsl:with-param><xsl:with-param name="status"><xsl:value-of select="libris_leader"/></xsl:with-param><xsl:with-param name="product_url"><xsl:text></xsl:text></xsl:with-param><xsl:with-param name="pid"><xsl:value-of select="isbn"/></xsl:with-param></xsl:call-template>
<!--<xsl:apply-templates select="sigel/code"><xsl:with-param name="datestamp"><xsl:value-of select="$datestamp"/></xsl:with-param><xsl:with-param name="product_url"><xsl:value-of select="@url"/></xsl:with-param></xsl:apply-templates>-->
</xsl:template>

<xsl:template name="bic008">
<xsl:variable name="bics" select="../category[1]/@bic"/>
<xsl:choose>
<xsl:when test="substring($bics, 1, 2) = 'YB' or substring($bics, 1, 2) = 'YD' or substring($bics, 1, 2) = 'YF' or substring($bics, 1, 2) = 'YN'">||||jo    |||| 1|</xsl:when>
<xsl:when test="substring($bics, 1, 1) = '5' and substring($bics, 2, 1) != 'R' and substring($bics, 2, 1) != 'S' and substring($bics, 2, 1) != 'T' and substring($bics, 2, 1) != 'U' and substring($bics, 2, 1) != 'V' and substring($bics, 2, 1) != 'W' and substring($bics, 2, 1) != 'X' and substring($bics, 2, 1) != 'Y' and substring ($bics, 2, 1) != 'Z'">||||jo    |||| 0|</xsl:when>
<xsl:otherwise>
<xsl:for-each select="$bic">
<xsl:choose>
<xsl:when test="count(key('bk', $bics)) &gt; 0"><xsl:value-of select="key('bk', $bics)/bic:p008"/></xsl:when>
<xsl:when test="count(key('bk', substring($bics,1,3))) &gt; 0"><xsl:value-of select="key('bk', substring($bics,1,3))/bic:p008"/></xsl:when>
<xsl:when test="count(key('bk', substring($bics,1,2))) &gt; 0"><xsl:value-of select="key('bk', substring($bics,1,2))/bic:p008"/></xsl:when>
<xsl:when test="count(key('bk', substring($bics,1,1))) &gt; 0"><xsl:value-of select="key('bk', substring($bics,1,1))/bic:p008"/></xsl:when>
<xsl:otherwise>|||| o    |||| | </xsl:otherwise>
</xsl:choose>
</xsl:for-each>
</xsl:otherwise>
</xsl:choose>
</xsl:template>

<xsl:template name="bic008a"> <!-- audiobook -->
<xsl:variable name="bics" select="../category[1]/@bic"/>
<xsl:choose>
<xsl:when test="substring($bics, 1, 2) = 'YB' or substring($bics, 1, 2) = 'YD' or substring($bics, 1, 2) = 'YF' or substring($bics, 1, 2) = 'YN'">||||jo||||||f  | </xsl:when>
<xsl:when test="substring($bics, 1, 1) = '5' and substring($bics, 2, 1) != 'R' and substring($bics, 2, 1) != 'S' and substring($bics, 2, 1) != 'T' and substring($bics, 2, 1) != 'U' and substring($bics, 2, 1) != 'V' and substring($bics, 2, 1) != 'W' and substring($bics, 2, 1) != 'X' and substring($bics, 2, 1) != 'Y' and substring ($bics, 2, 1) != 'Z'">||||jo||||||   | </xsl:when>
<xsl:otherwise>
<xsl:for-each select="$bic">
<xsl:choose>
<xsl:when test="count(key('bk', $bics)) &gt; 0"><xsl:value-of select="key('bk', $bics)/bic:p008a"/></xsl:when>
<xsl:when test="count(key('bk', substring($bics,1,3))) &gt; 0"><xsl:value-of select="key('bk', substring($bics,1,3))/bic:p008a"/></xsl:when>
<xsl:when test="count(key('bk', substring($bics,1,2))) &gt; 0"><xsl:value-of select="key('bk', substring($bics,1,2))/bic:p008a"/></xsl:when>
<xsl:when test="count(key('bk', substring($bics,1,1))) &gt; 0"><xsl:value-of select="key('bk', substring($bics,1,1))/bic:p008a"/></xsl:when>
<xsl:otherwise>|||| o|||||||| | </xsl:otherwise>
</xsl:choose>
</xsl:for-each>
</xsl:otherwise>
</xsl:choose>
</xsl:template>

<xsl:template name="bicSAB">
<xsl:variable name="bics" select="string(../../category[substring(@bic, 1, 1) != 'Y' and not(number(substring(@bic,1, 1)))][1]/@bic)"/>
<xsl:variable name="bicsU">
<xsl:if test="substring($bics, 1, 1) = 'Y' or number(substring($bics, 1, 1))">
<xsl:value-of select="'u'"/>
</xsl:if>
</xsl:variable>
<xsl:variable name="lang" select="../../language"/>
<xsl:variable name="bicsR">
<xsl:for-each select="$bic">
<xsl:choose>
<xsl:when test="count(key('bk', $bics)) &gt; 0"><xsl:value-of select="$bics"/></xsl:when>
<xsl:when test="count(key('bk', substring($bics,1,3))) &gt; 0"><xsl:value-of select="substring($bics,1,3)"/></xsl:when>
<xsl:when test="count(key('bk', substring($bics,1,2))) &gt; 0"><xsl:value-of select="substring($bics,1,2)"/></xsl:when>
<xsl:when test="count(key('bk', substring($bics,1,1))) &gt; 0"><xsl:value-of select="substring($bics,1,1)"/></xsl:when>
<xsl:otherwise></xsl:otherwise>
</xsl:choose>
</xsl:for-each>
</xsl:variable>
<xsl:variable name="sab">
<xsl:if test="$bicsR != ''">
<xsl:for-each select="$bic">
<xsl:choose>
<xsl:when test="$lang = 'Svenska'">
<xsl:value-of select="key('bk', $bicsR)/bic:sv"/>
</xsl:when>
<xsl:when test="$lang = 'Engelska'">
<xsl:value-of select="key('bk', $bicsR)/bic:en"/>
</xsl:when>
<xsl:when test="$lang = 'Finska'">
<xsl:value-of select="key('bk', $bicsR)/bic:fi"/>
</xsl:when>
<xsl:when test="$lang = 'Norska'">
<xsl:value-of select="key('bk', $bicsR)/bic:no"/>
</xsl:when>
<xsl:when test="$lang = 'Danska'">
<xsl:value-of select="key('bk', $bicsR)/bic:da"/>
</xsl:when>
<xsl:when test="$lang = 'Tyska'">
<xsl:value-of select="key('bk', $bicsR)/bic:de"/>
</xsl:when>
<xsl:when test="$lang = 'Franska'">
<xsl:value-of select="key('bk', $bicsR)/bic:fr"/>
</xsl:when>
<xsl:when test="$lang = 'Nederländska'">
<xsl:value-of select="key('bk', $bicsR)/bic:nl"/>
</xsl:when>
<xsl:when test="$lang = 'Spanska'">
<xsl:value-of select="key('bk', $bicsR)/bic:es"/>
</xsl:when>
<xsl:when test="$lang = 'Arabiska'">
<xsl:value-of select="key('bk', $bicsR)/bic:ar"/>
</xsl:when>
<xsl:when test="$lang = 'Persiska'">
<xsl:value-of select="key('bk', $bicsR)/bic:fa"/>
</xsl:when>
<xsl:otherwise>
</xsl:otherwise>
</xsl:choose>
</xsl:for-each>
</xsl:if>
</xsl:variable>
<xsl:choose>
<xsl:when test="$sab != ''">
<xsl:value-of select="concat($bicsU, $sab)"/>
</xsl:when>
<xsl:otherwise>
<xsl:for-each select="$bic">
<xsl:if test="key('bk', $bicsR)/bic:generellt != ''">
<xsl:value-of select="concat($bicsU, key('bk', $bicsR)/bic:generellt)"/>
</xsl:if>
</xsl:for-each>
</xsl:otherwise>
</xsl:choose>
</xsl:template>

<!-- type, leader, controlfield 007, 008, datafield 655 -->
<xsl:template match="type[normalize-space(.) = 'E-bok']">
<xsl:param name="pubyear"/>
<leader>*****nam a22*****3a 4500</leader>
<controlfield tag="007"><xsl:text>cr||||||||||||</xsl:text></controlfield>
<xsl:variable name="countrycode">
<xsl:choose> 
<xsl:when test="(string-length(../isbn) = 13 and starts-with(../isbn, '97891')) or (string-length(../isbn) = 10 and starts-with(../isbn, '91'))">sw</xsl:when>
<xsl:otherwise>xx</xsl:otherwise>
</xsl:choose>
</xsl:variable>
<xsl:variable name="lang_code"><xsl:apply-templates select="../language"/></xsl:variable>
<controlfield tag="008"><xsl:value-of select="$datestamp"/>s<xsl:value-of select="$pubyear"/><xsl:text>    </xsl:text><xsl:value-of select="$countrycode"/><xsl:text> </xsl:text><xsl:call-template name="bic008"/><xsl:value-of select="$lang_code"/><xsl:text> d</xsl:text></controlfield>
<datafield tag="655" ind1=" " ind2="4">
<subfield code="a">E-böcker</subfield>
</datafield>        
</xsl:template>
    
<!-- id, controlfield 001, datafield 035 -->
<xsl:template match="id">
<controlfield tag="001"><xsl:value-of select="normalize-space(.)"/></controlfield>
<datafield ind1=" " ind2=" " tag="035">
<subfield code="a">Publit<xsl:value-of select="normalize-space(.)"/></subfield>
</datafield>
</xsl:template>
    
<!-- external_id, datafield 020 -->
<xsl:template match="isbn">
<datafield ind1=" " ind2=" " tag="020">
<subfield code="a"><xsl:value-of select="normalize-space(.)"/></subfield>
</datafield>
</xsl:template>
    
<!-- title, datafield 245 -->
<xsl:template match="title">
<datafield ind1="1" ind2="0" tag="245">
<subfield code="a"><xsl:value-of select="normalize-space(.)"/></subfield>
<subfield code="h">[Elektronisk resurs]<xsl:if test="normalize-space(../subtitle) != ''"><xsl:text> :</xsl:text></xsl:if></subfield>
<xsl:if test="normalize-space(../subtitle) != ''"><subfield code="b"><xsl:value-of select="normalize-space(../subtitle)"/></subfield></xsl:if>
</datafield>
</xsl:template>
    
<!-- publisher, datafield 260 -->
<xsl:template match="publisher">
<xsl:param name="pubyear"/>
<datafield ind1=" " ind2=" " tag="260">
<subfield code="b"><xsl:value-of select="normalize-space(.)"/><xsl:if test="$pubyear != ''">,</xsl:if></subfield>
<xsl:if test="$pubyear != ''">
<subfield code="c"><xsl:value-of select="$pubyear"/></subfield>
</xsl:if>
</datafield>
</xsl:template>
    
<!-- pages, datafield 300 -->
<xsl:template match="pages">
<datafield ind1=" " ind2=" " tag="300">
<subfield code="a"><xsl:value-of select="normalize-space(.)"/> s.</subfield>
</datafield>        
</xsl:template>
   
<!-- description, datafield 520 -->
<xsl:template match="description">
<datafield ind1=" " ind2=" " tag="520">
<subfield code="a"><xsl:value-of select="normalize-space(.)"/><xsl:text> [Publit]</xsl:text></subfield>
</datafield>
</xsl:template>
    
<!-- description, datafield 545 -->
<xsl:template match="authorDescription">
<datafield ind1="0" ind2=" " tag="545">
<subfield code="a"><xsl:value-of select="normalize-space(.)"/><xsl:text> [Publit]</xsl:text></subfield>
</datafield>
</xsl:template>
    
<!-- category, datafield 655 -->
<xsl:template match="category">
<datafield ind1=" " ind2="4" tag="655">
<subfield code="a"><xsl:value-of select="normalize-space(.)"/></subfield>
</datafield>
</xsl:template>
    
<!-- author, datafield 100, 700 -->
<xsl:template match="authors/author">
<xsl:variable name="tag">
<xsl:choose>
<xsl:when test="position() = 1">100</xsl:when>
<xsl:otherwise>700</xsl:otherwise>
</xsl:choose>
</xsl:variable>
<xsl:variable name="lastname" select="normalize-space(./lastname)"/>
<xsl:variable name="firstname" select="normalize-space(./firstname)"/>
<xsl:if test="$firstname != '' or $lastname != ''">
<datafield tag="{$tag}" ind1="1" ind2=" ">
<subfield code="a"><xsl:value-of select="$lastname"/><xsl:if test="$firstname != ''"><xsl:if test="$lastname != ''"><xsl:text>, </xsl:text></xsl:if><xsl:value-of select="$firstname"/></xsl:if></subfield>
</datafield>
</xsl:if>
</xsl:template>
    
<!-- image url, datafield 856 -->
<xsl:template match="imageUrl">
<xsl:variable name="url">
<xsl:if test="substring(normalize-space(.), 1, 5) = 'https'">http<xsl:value-of select="substring(normalize-space(.), 6, string-length(normalize-space(.)))"/></xsl:if>
</xsl:variable>
<xsl:if test="substring(normalize-space(.), string-length(normalize-space(.)), 1) != '/'">
<datafield ind1="4" ind2="2" tag="856">
<subfield code="u"><xsl:value-of select="$url"/></subfield>
<subfield code="z">Omslagsbild</subfield>
<subfield code="x">digipic</subfield>
</datafield>
</xsl:if>
</xsl:template>
    
<!-- metaData change date, datafield 599 -->
<xsl:template match="lastModified/metaData">
<datafield ind1=" " ind2=" " tag="599">
<subfield code="a">Ändrad av Publit <xsl:value-of select="substring(normalize-space(.), 1, 10)"/></subfield>
</datafield>
</xsl:template>
    
<!-- sigel code, holdings record -->
<xsl:template match="sigel/code">
<xsl:param name="datestamp"/>
<xsl:param name="product_url"/>
<xsl:call-template name="create_holdings"><xsl:with-param name="sigel_code"><xsl:value-of select="normalize-space(.)"/></xsl:with-param><xsl:with-param name="datestamp"><xsl:value-of select="$datestamp"/></xsl:with-param><xsl:with-param name="status"><xsl:value-of select="@status"/></xsl:with-param><xsl:with-param name="product_url"><xsl:value-of select="$product_url"/></xsl:with-param><xsl:with-param name="pid"><xsl:value-of select="../../isbn"/></xsl:with-param></xsl:call-template>
</xsl:template>
    
<xsl:template name="create_holdings">
<xsl:param name="sigel_code"/>
<xsl:param name="pid"/>
<xsl:param name="datestamp"/>
<xsl:param name="status"/>
<xsl:param name="product_url"/>
<xsl:if test="$sigel_code != 'Atingo'">
<record type="Holdings">
<xsl:variable name="libris_leader">
<xsl:choose>
<xsl:when test="$status = 'd'">c</xsl:when>
<xsl:when test="$status != ''"><xsl:value-of select="$status"/></xsl:when>
<xsl:otherwise>n</xsl:otherwise>
</xsl:choose>
</xsl:variable>
<leader>*****<xsl:value-of select="$libris_leader"/>x  a22*****1n 4500</leader>
<controlfield tag="008"><xsl:value-of select="$datestamp"/>||0000|||||000||||||000000</controlfield>
<xsl:if test="$status != 'd'">
<datafield tag="024" ind1="7" ind2=" ">
<subfield code="a"><xsl:value-of select="$pid"/></subfield>
<subfield code="2">Distributör: Atingo</subfield>
</datafield>
<xsl:if test="$product_url != ''">
<datafield ind1="4" ind2="2" tag="856">
<subfield code="u">http://www.publit.se<xsl:value-of select="$product_url"/></subfield>
<subfield code="z">Begränsad åtkomst, Atingo</subfield>
<subfield code="x">origin:Atingo</subfield>
</datafield>                
</xsl:if>
</xsl:if>
<datafield ind1=" " ind2=" " tag="852">
<subfield code="b"><xsl:value-of select="$sigel_code"/></subfield>
<subfield code="x">origin:Atingo</subfield>
<xsl:choose>
<xsl:when test="$status = 'd'">
<subfield code="h">Ej tillgänglig</subfield>
<subfield code="x">deleted</subfield>
</xsl:when>
<xsl:otherwise>
<xsl:if test="$sigel_code != 'Publ'">
<subfield code="c">E-bok</subfield>
<subfield code="z">EPUB</subfield>
<xsl:variable name="sab"><xsl:call-template name="bicSAB"/></xsl:variable>
<xsl:if test="$sab != ''">
<subfield code="h"><xsl:value-of select="$sab"/></subfield>
</xsl:if>
</xsl:if>
</xsl:otherwise>
</xsl:choose>
</datafield>
</record>
</xsl:if>
</xsl:template>
    
<!-- language, translate to ISO 639-3B code -->
<xsl:template match="language">
<xsl:choose>
<xsl:when test="normalize-space(.) = 'Svenska'">swe</xsl:when>
<xsl:when test="normalize-space(.) = 'Engelska'">eng</xsl:when>
<xsl:when test="normalize-space(.) = 'Tyska'">ger</xsl:when>
<xsl:when test="normalize-space(.) = 'Franska'">fre</xsl:when>
<xsl:when test="normalize-space(.) = 'Danska'">dan</xsl:when>
<xsl:when test="normalize-space(.) = 'Italienska'">ita</xsl:when>
<xsl:when test="normalize-space(.) = 'Finska'">fin</xsl:when>
<xsl:when test="normalize-space(.) = 'Nederländska'">dut</xsl:when>
<xsl:when test="normalize-space(.) = 'Ryska'">rus</xsl:when>
<xsl:when test="normalize-space(.) = 'Spanska'">spa</xsl:when>
<xsl:when test="normalize-space(.) = 'Polska'">pol</xsl:when>
<xsl:when test="normalize-space(.) = 'Norska'">nor</xsl:when> 
<xsl:when test="normalize-space(.) = 'Ungerska'">hun</xsl:when>
<xsl:when test="normalize-space(.) = 'Tjeckiska'">cze</xsl:when>
<xsl:when test="normalize-space(.) = 'Portugisiska'">por</xsl:when>
<xsl:when test="normalize-space(.) = 'Japanska'">jpn</xsl:when>
<xsl:when test="normalize-space(.) = 'Kurdiska'">kur</xsl:when>
<xsl:when test="normalize-space(.) = 'Persiska'">per</xsl:when>
<xsl:when test="normalize-space(.) = 'Arabiska'">ara</xsl:when>
<xsl:when test="normalize-space(.) = 'Turkiska'">tur</xsl:when>
<xsl:when test="normalize-space(.) = 'Slovenska'">slv</xsl:when>
<xsl:when test="normalize-space(.) = 'Estniska'">est</xsl:when>
<xsl:when test="normalize-space(.) = 'Lettiska'">lav</xsl:when>
<xsl:when test="normalize-space(.) = 'Kroatiska'">hrv</xsl:when>
<xsl:when test="normalize-space(.) = 'Latin'">lat</xsl:when>
<xsl:when test="normalize-space(.) = 'Somaliska'">som</xsl:when>
<xsl:when test="normalize-space(.) = 'Grekiska'">gre</xsl:when>
<xsl:when test="normalize-space(.) = 'Nordsamiska'">sme</xsl:when>
<xsl:when test="normalize-space(.) = 'eo'">epo</xsl:when>
</xsl:choose>
</xsl:template>
    
</xsl:stylesheet>
