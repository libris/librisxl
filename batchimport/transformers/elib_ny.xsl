<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0" xmlns:java="http://xml.apache.org/xslt/java" xmlns:bic="http://bic" exclude-result-prefixes="java bic">
<xsl:output method="xml" indent="yes" encoding="UTF-8"/>
<!-- 140714 Kai P -->

<xsl:key name="bk" match="bic:bics/bic:bic" use="@name"/>
<xsl:variable name="bic" select="document('/appl/import2/etc/bic.xml')/bic:bics"/>

<!-- transformation date -->
<xsl:variable name="rawdatestamp"><xsl:value-of select="java:format(java:java.text.SimpleDateFormat.new('yyyy-MM-dd'), java:java.util.Date.new())"/></xsl:variable>
<xsl:variable name="datestamp"><xsl:value-of select="substring(translate($rawdatestamp, '-', ''), 3, 6)"/></xsl:variable>

<!--<xsl:variable name="lc" select="'abcdefghijklmnopqrstuvwxyz\303\245\303\244\303\266'"/>-->
<!--<xsl:variable name="uc" select="'ABCDEFGHIJKLMNOPQRSTUVWXYZ\303\205\303\204\303\226'"/>-->

<xsl:template match="Products">
<collection xmlns="http://www.loc.gov/MARC21/slim">
<xsl:apply-templates select="Product"/>
</collection>
</xsl:template>

<xsl:template name="bic008">
<xsl:variable name="bics" select="../Categories/Categories-item[Type = 'BIC']"/>
<xsl:variable name="bicsYY" select="$bics/Name[substring(., 1, 2) = 'YB' or substring(., 1, 2) = 'YD' or substring(., 1, 2) = 'YF' or substring(., 1, 2) = 'YN'][1]"/>
<xsl:variable name="bics5" select="$bics/Name[substring(., 1, 1) = '5' and substring(., 2, 1) != 'R' and substring(., 2, 1) != 'S' and substring(., 2, 1) != 'T' and substring(., 2, 1) != 'U' and substring(., 2, 1) != 'V' and substring(., 2, 1) != 'W' and substring(., 2, 1) != 'X' and substring(., 2, 1) != 'Y' and substring(., 2, 1) != 'Z'][1]"/>
<xsl:choose>
<xsl:when test="$bicsYY != ''">||||jo    |||| 1|</xsl:when>
<xsl:when test="$bics5 != ''">||||jo    |||| 0|</xsl:when>
<xsl:otherwise>
<xsl:variable name="bicsR" select="string($bics/Name[substring(., 1, 1) != 'Y' and not(number(substring(., 1, 1)))][1])"/>
<xsl:for-each select="$bic">
<xsl:choose>
<xsl:when test="count(key('bk', $bicsR)) &gt; 0"><xsl:value-of select="key('bk', $bicsR)/bic:p008"/></xsl:when>
<xsl:when test="count(key('bk', substring($bicsR,1,3))) &gt; 0"><xsl:value-of select="key('bk', substring($bicsR,1,3))/bic:p008"/></xsl:when>
<xsl:when test="count(key('bk', substring($bicsR,1,2))) &gt; 0"><xsl:value-of select="key('bk', substring($bicsR,1,2))/bic:p008"/></xsl:when>
<xsl:when test="count(key('bk', substring($bicsR,1,1))) &gt; 0"><xsl:value-of select="key('bk', substring($bicsR,1,1))/bic:p008"/></xsl:when>
<xsl:otherwise>|||| o    |||| | </xsl:otherwise>
</xsl:choose>
</xsl:for-each>
</xsl:otherwise>
</xsl:choose>
</xsl:template>
    
<xsl:template name="bic008a"> <!-- audiobook -->
<xsl:variable name="bics" select="../Categories/Categories-item[Type = 'BIC']"/>
<xsl:variable name="bicsYY" select="$bics/Name[substring(., 1, 2) = 'YB' or substring(., 1, 2) = 'YD' or substring(., 1, 2) = 'YF' or substring(., 1, 2) = 'YN'][1]"/>
<xsl:variable name="bics5" select="$bics/Name[substring(., 1, 1) = '5' and substring(., 2, 1) != 'R' and substring(., 2, 1) != 'S' and substring(., 2, 1) != 'T' and substring(., 2, 1) != 'U' and substring(., 2, 1) != 'V' and substring(., 2, 1) != 'W' and substring(., 2, 1) != 'X' and substring(., 2, 1) != 'Y' and substring(., 2, 1) != 'Z'][1]"/>
<xsl:choose>
<xsl:when test="$bicsYY != ''">||||jo||||||f  | </xsl:when>
<xsl:when test="$bics5 != ''">||||jo||||||   | </xsl:when>
<xsl:otherwise>
<xsl:variable name="bicsR" select="string($bics/Name[substring(., 1, 1) != 'Y' and not(number(substring(., 1, 1)))][1])"/>
<xsl:for-each select="$bic">
<xsl:choose>
<xsl:when test="count(key('bk', $bicsR)) &gt; 0"><xsl:value-of select="key('bk', $bicsR)/bic:p008a"/></xsl:when>
<xsl:when test="count(key('bk', substring($bicsR,1,3))) &gt; 0"><xsl:value-of select="key('bk', substring($bicsR,1,3))/bic:p008a"/></xsl:when>
<xsl:when test="count(key('bk', substring($bicsR,1,2))) &gt; 0"><xsl:value-of select="key('bk', substring($bicsR,1,2))/bic:p008a"/></xsl:when>
<xsl:when test="count(key('bk', substring($bicsR,1,1))) &gt; 0"><xsl:value-of select="key('bk', substring($bicsR,1,1))/bic:p008a"/></xsl:when>
<xsl:otherwise>|||| o|||||||| | </xsl:otherwise>
</xsl:choose>
</xsl:for-each>
</xsl:otherwise>
</xsl:choose>
</xsl:template>

<xsl:template name="bicSAB">
<xsl:variable name="bics" select="../../Categories/Categories-item[Type = 'BIC']"/>
<xsl:variable name="bicsRR" select="string($bics/Name[substring(., 1, 1) != 'Y' and not(number(substring(., 1, 1)))][1])"/>
<xsl:variable name="bicsU">
<xsl:if test="$bics/Name[substring(., 1, 1) = 'Y' or number(substring(., 1, 1))][1] != ''">
<xsl:value-of select="'u'"/>
</xsl:if>
</xsl:variable>
<xsl:variable name="lang" select="../../Language"/>
<xsl:variable name="bicsR">
<xsl:for-each select="$bic">
<xsl:choose>
<xsl:when test="count(key('bk', $bicsRR)) &gt; 0"><xsl:value-of select="$bicsRR"/></xsl:when>
<xsl:when test="count(key('bk', substring($bicsRR,1,3))) &gt; 0"><xsl:value-of select="substring($bicsRR,1,3)"/></xsl:when>
<xsl:when test="count(key('bk', substring($bicsRR,1,2))) &gt; 0"><xsl:value-of select="substring($bicsRR,1,2)"/></xsl:when>
<xsl:when test="count(key('bk', substring($bicsRR,1,1))) &gt; 0"><xsl:value-of select="substring($bicsRR,1,1)"/></xsl:when>
<xsl:otherwise></xsl:otherwise>
</xsl:choose>
</xsl:for-each>
</xsl:variable>
<xsl:variable name="sab">
<xsl:if test="$bicsR != ''">
<xsl:for-each select="$bic">
<xsl:choose>
<xsl:when test="$lang = 'sv'">
<xsl:value-of select="key('bk', $bicsR)/bic:sv"/>
</xsl:when>
<xsl:when test="$lang = 'en'">
<xsl:value-of select="key('bk', $bicsR)/bic:en"/>
</xsl:when>
<xsl:when test="$lang = 'de'">
<xsl:value-of select="key('bk', $bicsR)/bic:de"/>
</xsl:when>
<xsl:when test="$lang = 'fr'">
<xsl:value-of select="key('bk', $bicsR)/bic:fr"/>
</xsl:when>
<xsl:when test="$lang = 'fi'">
<xsl:value-of select="key('bk', $bicsR)/bic:fi"/>
</xsl:when>
<xsl:when test="$lang = 'no'">
<xsl:value-of select="key('bk', $bicsR)/bic:no"/>
</xsl:when>
<xsl:when test="$lang = 'da'">
<xsl:value-of select="key('bk', $bicsR)/bic:da"/>
</xsl:when>
<xsl:when test="$lang = 'nl'">
<xsl:value-of select="key('bk', $bicsR)/bic:nl"/>
</xsl:when>
<xsl:when test="$lang = 'es'">
<xsl:value-of select="key('bk', $bicsR)/bic:es"/>
</xsl:when>
<xsl:when test="$lang = 'ar'">
<xsl:value-of select="key('bk', $bicsR)/bic:ar"/>
</xsl:when>
<xsl:when test="$lang = 'fa'">
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

<xsl:template match="Product">
<!-- Bibliographic record -->
<record type="Bibliographic">
<datafield tag="020" ind1=" " ind2=" ">
<subfield code="a"><xsl:value-of select="./PublicIdentifiers/PublicIdentifiers-item[IdType = 'ISBN']/Id"/></subfield>
</datafield>
<xsl:apply-templates />
</record>
<!-- Holding records -->
<xsl:if test="count(./AvailableFormats/AvailableFormats-item) &gt; 0">
<xsl:call-template name="elib-holding"/>
<xsl:if test="count(./Holdings/Holdings-item) &gt; 0">
<xsl:apply-templates select="./Holdings/Holdings-item" mode="Holdings"/>
</xsl:if>
</xsl:if>
</xsl:template>

<xsl:template match="Holdings-item" mode="Holdings">
<xsl:variable name="status">
<xsl:choose>
<xsl:when test="./Status = 'd'">c</xsl:when>
<xsl:when test="./Status != ''"><xsl:value-of select="./Status"/></xsl:when>
<xsl:otherwise>n</xsl:otherwise>
</xsl:choose>
</xsl:variable>
<record type="Holdings">
<leader>*****<xsl:value-of select="$status"/>x  a22*****1n 4500</leader>
<controlfield tag="008"><xsl:value-of select="$datestamp"/>||0000|||||000||||||000000</controlfield>
<xsl:choose>
<xsl:when test="./Status = 'd'">
<datafield ind1=" " ind2=" " tag="852">
<subfield code="b"><xsl:value-of select="./Sigel"/></subfield>
<subfield code="x">origin:Elib</subfield>
<subfield code="h">Ej tillgänglig</subfield>
<subfield code="x">deleted</subfield>
</datafield>
</xsl:when>
<xsl:otherwise>
<datafield tag="024" ind1="7" ind2=" ">
<subfield code="a"><xsl:value-of select="../../ProductId"/></subfield>
<subfield code="2">Distributör: Elib</subfield>
</datafield>
<xsl:if test="./UrlPrefix != ''">
<xsl:choose>
<xsl:when test="starts-with(./UrlPrefix, 'http')">
<datafield tag="856" ind1="4" ind2="2">
<subfield code="u"><xsl:value-of select="./UrlPrefix"/><xsl:value-of select="../../ProductId"/></subfield>
<subfield code="z">Låna från ditt bibliotek. Lånekort krävs.</subfield>
<subfield code="x">origin:Elib</subfield>
</datafield>
</xsl:when>
<xsl:otherwise>
<datafield tag="856" ind1="4" ind2="2">
<subfield code="u"><xsl:text>http://</xsl:text><xsl:value-of select="./UrlPrefix"/><xsl:text>.elib.se/Books/Details/</xsl:text><xsl:value-of select="../../ProductId"/></subfield>
<subfield code="z">Låna från ditt bibliotek. Lånekort krävs.</subfield>
<subfield code="x">origin:Elib</subfield>
</datafield>
</xsl:otherwise>
</xsl:choose>
</xsl:if>
<xsl:call-template name="holdings_booktype"></xsl:call-template>
</xsl:otherwise>
</xsl:choose>
</record>
</xsl:template>

<xsl:template name="elib-holding">
<record type="Holdings">
<leader>*****nx  a22*****1n 4500</leader>
<controlfield tag="008"><xsl:value-of select="$datestamp"/>||0000|||||000||||||000000</controlfield>
<datafield tag="852" ind1="" ind2="">
<subfield code="b">Elib</subfield>
<subfield code="x">origin:Elib</subfield>
</datafield>
<datafield tag="024" ind1="7" ind2=" ">
<subfield code="a"><xsl:value-of select="./ProductId"/></subfield>
<subfield code="2">Distributör: Elib</subfield>
</datafield>
</record>
</xsl:template>

<xsl:template name="holdings_booktype">
<xsl:variable name="sigel" select="./Sigel"/>
<xsl:variable name="teaser" select="../../Teaser"/>
<xsl:choose>
<xsl:when test="../../BookType = 'E-book'">
<xsl:if test="$teaser != ''">
<datafield tag="856" ind1="4" ind2="2">
<subfield code="u"><xsl:value-of select="$teaser"/></subfield>
<subfield code="z">Provläs</subfield>
<subfield code="x">origin:Elib</subfield>
</datafield>
</xsl:if>
<xsl:variable name="bicS">
<xsl:call-template name="bicSAB"/>
</xsl:variable>
<datafield tag="852" ind1="" ind2="">
<subfield code="b"><xsl:value-of select="$sigel"/></subfield>
<subfield code="c">E-Bok</subfield>
<xsl:if test="$bicS != ''">
<subfield code="h"><xsl:value-of select="$bicS"/></subfield>
</xsl:if>
<subfield code="x">origin:Elib</subfield>
<xsl:for-each select="../../AvailableFormats/AvailableFormats-item">
<xsl:choose>
<xsl:when test="FormatId = 4101">
<subfield code="z">Online epub (<xsl:call-template name="SizeMb"><xsl:with-param name="bytes" select="SizeInBytes"/></xsl:call-template>)</subfield> 
</xsl:when>
<xsl:when test="FormatId = 4103">
<subfield code="z">Online pdf med Adobe-kryptering (<xsl:call-template name="SizeMb"><xsl:with-param name="bytes" select="SizeInBytes"/></xsl:call-template>)</subfield> 
</xsl:when>
<xsl:when test="FormatId = 4104">
<subfield code="z">Offline epub med Adobe-kryptering (<xsl:call-template name="SizeMb"><xsl:with-param name="bytes" select="SizeInBytes"/></xsl:call-template>)</subfield> 
</xsl:when>
<xsl:otherwise>
</xsl:otherwise>
</xsl:choose>
</xsl:for-each>
</datafield>
</xsl:when>
<xsl:when test="../../BookType = 'Audiobook'">
<xsl:if test="$teaser != ''">
<datafield tag="856" ind1="4" ind2="2">
<subfield code="u"><xsl:value-of select="$teaser"/></subfield>
<subfield code="z">Provlyssna</subfield>
<subfield code="x">origin:Elib</subfield>
</datafield>
</xsl:if>
<datafield tag="852" ind1="" ind2="">
<subfield code="b"><xsl:value-of select="$sigel"/></subfield>
<subfield code="c">Ljudbok</subfield>
<subfield code="x">origin:Elib</subfield>
<xsl:for-each select="../../AvailableFormats/AvailableFormats-item">
<xsl:choose>
<xsl:when test="FormatId = 4002">
<subfield code="z">Dator (<xsl:call-template name="SizeMb"><xsl:with-param name="bytes" select="SizeInBytes"/></xsl:call-template>)</subfield> 
</xsl:when>
<xsl:when test="FormatId = 4003">
<subfield code="z">iOS (<xsl:call-template name="SizeMb"><xsl:with-param name="bytes" select="SizeInBytes"/></xsl:call-template>)</subfield> 
</xsl:when>
<xsl:when test="FormatId = 4004">
<subfield code="z">Android (app) (<xsl:call-template name="SizeMb"><xsl:with-param name="bytes" select="SizeInBytes"/></xsl:call-template>)</subfield> 
</xsl:when>
<xsl:when test="FormatId = 4102">
<subfield code="z">Online Flash player (<xsl:call-template name="SizeMb"><xsl:with-param name="bytes" select="SizeInBytes"/></xsl:call-template>)</subfield> 
</xsl:when>
<xsl:otherwise>
</xsl:otherwise>
</xsl:choose>
</xsl:for-each>
</datafield>
</xsl:when>
<xsl:otherwise>
</xsl:otherwise>
</xsl:choose>
</xsl:template>

<xsl:template match="UpdatedDate">
<datafield tag="599" ind1=" " ind2=" ">
<subfield code="a">Ändrad av Elib <xsl:value-of select="substring((.), 1, 10)"/></subfield>
</datafield>
</xsl:template>

<xsl:template match="Title">
<datafield tag="245" ind1="1" ind2="0">
<subfield code="a"><xsl:value-of select="."/></subfield>
<subfield code="h">[Elektronisk resurs]</subfield>
</datafield>
</xsl:template>

<xsl:template match="ProductId">
<controlfield tag="001">
<xsl:value-of select="."/>
</controlfield>
<datafield tag="035" ind1=" " ind2=" ">
<subfield code="a">Elib<xsl:value-of select="."/></subfield>
</datafield>
</xsl:template>

<xsl:template match="Contributors">
<xsl:for-each select="Contributors-item[Role = 'Author']">
<xsl:choose>
<xsl:when test="position() = 1">
<xsl:call-template name="output_role">
<xsl:with-param name="tag" select="'100'"/>
<xsl:with-param name="fn" select="FirstName"/>
<xsl:with-param name="ln" select="LastName"/>
<xsl:with-param name="role" select="Role"/>
</xsl:call-template>
</xsl:when>
<xsl:otherwise>
<xsl:call-template name="output_role">
<xsl:with-param name="tag" select="'700'"/>
<xsl:with-param name="fn" select="FirstName"/>
<xsl:with-param name="ln" select="LastName"/>
<xsl:with-param name="role" select="Role"/>
</xsl:call-template>
</xsl:otherwise>
</xsl:choose>
</xsl:for-each>
<xsl:for-each select="Contributors-item[Role != 'Author']">
<xsl:call-template name="output_role">
<xsl:with-param name="tag" select="'700'"/>
<xsl:with-param name="fn" select="FirstName"/>
<xsl:with-param name="ln" select="LastName"/>
<xsl:with-param name="role" select="Role"/>
</xsl:call-template>
</xsl:for-each>
</xsl:template>

<xsl:template name="output_role">
<xsl:param name="tag"/>
<xsl:param name="fn"/>
<xsl:param name="ln"/>
<xsl:param name="role"/>
<datafield tag="{$tag}" ind1="1" ind2=" ">
<subfield code="a"><xsl:value-of select="$ln"/>, <xsl:value-of select="$fn"/></subfield>
<subfield code="4"><xsl:call-template name="map_role"><xsl:with-param name="role" select="$role"/></xsl:call-template></subfield>
</datafield>
</xsl:template>

<xsl:template match="BookType">
<xsl:choose>
<xsl:when test=". = 'Audiobook'">
<leader>*****nim a22*****3a 4500</leader>
<controlfield tag="007">su||||||||||||</controlfield>
<controlfield tag="007">cr||||||||||||</controlfield>
<controlfield tag="008"><xsl:value-of select="$datestamp"/>s<xsl:choose><xsl:when test="../PublishedDate != ''"><xsl:value-of select="substring(../PublishedDate, 1, 4)"/></xsl:when><xsl:otherwise>nnnn</xsl:otherwise></xsl:choose><xsl:text>    </xsl:text><xsl:call-template name="map_country"/><xsl:text> </xsl:text><xsl:call-template name="bic008a"/><xsl:call-template name="map_language"><xsl:with-param name="lang" select="../Language"/></xsl:call-template><xsl:text> d</xsl:text></controlfield>
<datafield tag="655" ind1=" " ind2="4">
<subfield code="a"><xsl:text>Ljudböcker</xsl:text></subfield>
<subfield code="2"><xsl:text>saogf</xsl:text></subfield>
</datafield>
</xsl:when>
<xsl:otherwise>
<!-- Ebook? -->
<leader>*****nam a22*****3a 4500</leader>
<controlfield tag="007">cr||||||||||||</controlfield>
<controlfield tag="008"><xsl:value-of select="$datestamp"/>s<xsl:choose><xsl:when test="../PublishedDate != ''"><xsl:value-of select="substring(../PublishedDate, 1, 4)"/></xsl:when><xsl:otherwise>nnnn</xsl:otherwise></xsl:choose><xsl:text>    </xsl:text><xsl:call-template name="map_country"/><xsl:text> </xsl:text><xsl:call-template name="bic008"/><xsl:call-template name="map_language"><xsl:with-param name="lang" select="../Language"/></xsl:call-template><xsl:text> d</xsl:text></controlfield>
<datafield tag="655" ind1=" " ind2="4">
<subfield code="a"><xsl:text>E-böcker</xsl:text></subfield>
</datafield>
</xsl:otherwise>
</xsl:choose>
</xsl:template>

<xsl:template match="Publisher">
<datafield tag="260" ind1=" " ind2=" ">
<subfield code="b"><xsl:value-of select="."/></subfield>
<xsl:if test="../PublishedDate != ''">
<subfield code="c"><xsl:value-of select="substring(../PublishedDate, 1, 4)"/></subfield>
</xsl:if>
</datafield>
</xsl:template>

<xsl:template match="Description">
<datafield tag="520" ind1=" " ind2=" ">
<subfield code="a"><xsl:value-of select="."/><xsl:text> [Elib]</xsl:text></subfield>
</datafield>
</xsl:template>

<xsl:template match="CoverImage">
<xsl:if test=". != ''">
<datafield tag="856" ind1="4" ind2="2">
<subfield code="u"><xsl:value-of select="."/></subfield>
<subfield code="z">Omslagsbild</subfield>
<subfield code="x">digipic</subfield>
</datafield>
</xsl:if>
</xsl:template>

<xsl:template match="Categories">
<xsl:for-each select="./Categories-item">
<xsl:choose>
<xsl:when test="Type = 'Elib'">
<datafield tag="655" ind1=" " ind2="4">
<subfield code="a"><xsl:value-of select="Name"/></subfield>
</datafield>
</xsl:when>
<xsl:when test="Type = 'BIC'"> <!-- Also in holdings 852h, 008 -->
<datafield tag="072" ind1=" " ind2="7">
<subfield code="a"><xsl:value-of select="Name"/></subfield> <!-- christer -->
<subfield code="2">bicssc</subfield>
</datafield>
</xsl:when>
<xsl:when test="Type = 'SAB'">
<datafield tag="084" ind1=" " ind2=" ">
<subfield code="a"><xsl:value-of select="Name"/></subfield> <!-- christer -->
<subfield code="2">kssb/8</subfield>
</datafield>
</xsl:when>
<xsl:when test="Type = 'AgeGroup'">
<datafield tag="521" ind1="1" ind2=" ">
<subfield code="a"><xsl:value-of select="Name"/></subfield> <!-- christer -->
</datafield>
</xsl:when>
<xsl:otherwise>
</xsl:otherwise>
</xsl:choose>
</xsl:for-each>
</xsl:template>

<xsl:template match="BookLength">
<xsl:if test="./Value != ''">
<datafield tag="300" ind1=" " ind2=" ">
<subfield code="a"><xsl:value-of select="./Value"/>
<xsl:choose>
<xsl:when test="./Type = 'Seconds'"> sek.</xsl:when>
<xsl:when test="./Type = 'Pages'"> s.</xsl:when>
<xsl:otherwise></xsl:otherwise>
</xsl:choose>
</subfield>
</datafield>
</xsl:if>
</xsl:template>

<!-- country -->
<xsl:template name="map_country">
<!-- <xsl:variable name="isbn13" select="../PublicIdentifiers/PublicIdentifiers-item[IdType = 'ISBN13']/Id"/> -->
<xsl:variable name="isbn" select="../PublicIdentifiers/PublicIdentifiers-item[IdType = 'ISBN']/Id"/>
<xsl:choose>
<xsl:when test="substring($isbn, 1, 5) = '97891' or substring($isbn, 1, 2) = '91'">
<xsl:text>sw</xsl:text>
</xsl:when>
<xsl:otherwise>
<xsl:text>xx</xsl:text>
</xsl:otherwise>
</xsl:choose>
</xsl:template>

<!-- role -->
<xsl:template name="map_role">
<xsl:param name="role"/>
<xsl:choose>
<xsl:when test="$role = 'Author'">aut</xsl:when>
<xsl:when test="$role = 'Co-author'">aut</xsl:when>
<xsl:when test="$role = 'Adapter'">adp</xsl:when>
<xsl:when test="$role = 'Photographer'">pht</xsl:when>
<xsl:when test="$role = 'Illustrator'">ill</xsl:when>
<xsl:when test="$role = 'Sound effects'">sds</xsl:when>
<xsl:when test="$role = 'Editor'">edt</xsl:when>
<xsl:when test="$role = 'Co-editor'">edt</xsl:when>
<xsl:when test="$role = 'Music'">cmp</xsl:when>
<xsl:when test="$role = 'Song'">sng</xsl:when>
<xsl:when test="$role = 'Narrator'">nrt</xsl:when>
<xsl:when test="$role = 'Translator'">trl</xsl:when>
<xsl:otherwise>oth</xsl:otherwise>
</xsl:choose>
</xsl:template>

<!-- language, ISO 639-1 to ISO 639-2B -->
<xsl:template name="map_language">
<xsl:param name="lang"/>
<xsl:choose>
<xsl:when test="$lang = 'sv'">swe</xsl:when>
<xsl:when test="$lang = 'en'">eng</xsl:when>
<xsl:when test="$lang = 'de'">ger</xsl:when>
<xsl:when test="$lang = 'fr'">fre</xsl:when>
<xsl:when test="$lang = 'da'">dan</xsl:when>
<xsl:when test="$lang = 'it'">ita</xsl:when>
<xsl:when test="$lang = 'fi'">fin</xsl:when>
<xsl:when test="$lang = 'nl'">dut</xsl:when>
<xsl:when test="$lang = 'ru'">rus</xsl:when>
<xsl:when test="$lang = 'es'">spa</xsl:when>
<xsl:when test="$lang = 'pl'">pol</xsl:when>
<xsl:when test="$lang = 'no'">nor</xsl:when> 
<xsl:when test="$lang = 'hu'">hun</xsl:when>
<xsl:when test="$lang = 'cs'">cze</xsl:when>
<xsl:when test="$lang = 'pt'">por</xsl:when>
<xsl:when test="$lang = 'ja'">jpn</xsl:when>
<xsl:when test="$lang = 'ku'">kur</xsl:when>
<xsl:when test="$lang = 'fa'">per</xsl:when>
<xsl:when test="$lang = 'ar'">ara</xsl:when>
<xsl:when test="$lang = 'tr'">tur</xsl:when>
<xsl:when test="$lang = 'sl'">slv</xsl:when>
<xsl:when test="$lang = 'et'">est</xsl:when>
<xsl:when test="$lang = 'lv'">lav</xsl:when>
<xsl:when test="$lang = 'hr'">hrv</xsl:when>
<xsl:when test="$lang = 'la'">lat</xsl:when>
<xsl:when test="$lang = 'so'">som</xsl:when>
<xsl:when test="$lang = 'el'">gre</xsl:when>
<xsl:when test="$lang = 'se'">sme</xsl:when>
<xsl:when test="$lang = 'eo'">epo</xsl:when>
<xsl:otherwise></xsl:otherwise>
</xsl:choose>
</xsl:template>

<!-- bytes to Mb -->
<xsl:template name="SizeMb">
<xsl:param name="bytes"/>
<xsl:value-of select="format-number(($bytes div 1000000), '#.##')"/><xsl:text> MB</xsl:text>
</xsl:template>

<!--<xsl:template match="">
</xsl:template>-->

<!-- Identity copy -->
<xsl:template match="@*|node()">
<!--<xsl:copy>
<xsl:apply-templates select="@*|node()"/>
</xsl:copy>-->
</xsl:template>

</xsl:stylesheet>
