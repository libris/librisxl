<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0" xmlns:java="http://xml.apache.org/xslt/java" xmlns:o="http://ns.editeur.org/onix/3.0/reference" xmlns:br="http://br" exclude-result-prefixes="java br o">
<xsl:output method="xml" indent="yes" encoding="UTF-8"/>
<!-- 160420 Kai P -->

<xsl:key name="bk" match="br:brs/br:br" use="@name"/>
<xsl:variable name="br" select="document('/appl/import/transformers/bokr_onix.xml')/br:brs"/>
<!--
<xsl:for-each select="$br">
<xsl:if test="count(key('bk', 'BC-B310')/br:ldr) &gt; 0">
<xsl:value-of select="key('bk', 'BC-B310')/br:ldr"/>
</xsl:if>
</xsl:for-each>
-->

<!-- transformation date -->
<xsl:variable name="rawdatestamp"><xsl:value-of select="java:format(java:java.text.SimpleDateFormat.new('yyyy-MM-dd'), java:java.util.Date.new())"/></xsl:variable>
<xsl:variable name="datestamp"><xsl:value-of select="substring(translate($rawdatestamp, '-', ''), 3, 6)"/></xsl:variable>

<xsl:variable name="lc" select="'abcdefghijklmnopqrstuvwxyz\303\245\303\244\303\266'"/>
<xsl:variable name="uc" select="'ABCDEFGHIJKLMNOPQRSTUVWXYZ\303\205\303\204\303\226'"/>
<!-- <xsl:call-template name="parseyear"><xsl:with-param name="string" select="'acd4444asdf'"/><xsl:with-param name="result" select="''"/></xsl:call-template> -->

<xsl:template match="o:ONIXMessage">
<collection xmlns="http://www.loc.gov/MARC21/slim">
<xsl:apply-templates select="o:Product"/>
</collection>
</xsl:template>

<xsl:template match="o:Product">
<xsl:variable name="ldr">
<xsl:call-template name="brl"><xsl:with-param name="col" select="'ldr'"/></xsl:call-template>
</xsl:variable>
<xsl:if test="$ldr != ''">
<xsl:call-template name="bibliographic"/>
<xsl:call-template name="holding"/>
</xsl:if>
</xsl:template>

<xsl:template name="brl">
<xsl:param name="col"/>
<xsl:variable name="desc" select="o:DescriptiveDetail/o:ProductFormDescription"/>
<xsl:variable name="pf" select="o:DescriptiveDetail/o:ProductForm"/>
<xsl:variable name="pfd">
<xsl:choose>
<xsl:when test="$pf = '00'">
<xsl:choose>
<xsl:when test="starts-with('Licens', $desc)">
<!--<xsl:when test="starts-with('Licens', o:DescriptiveDetail/o:ProductFormDescription )"> -->
<xsl:value-of select="'Licens'"/>
</xsl:when>
<xsl:otherwise>
<xsl:value-of select="$desc"/>
<!--<xsl:value-of select="o:DescriptiveDetail/o:ProductFormDescription"/> -->
</xsl:otherwise>
</xsl:choose>
</xsl:when>
<xsl:otherwise>
<xsl:value-of select="o:DescriptiveDetail/o:ProductFormDetail"/>
</xsl:otherwise>
</xsl:choose>
</xsl:variable>
<xsl:for-each select="$br">
<xsl:choose> <!-- argh, why can't variables be used in xpath... -->
<xsl:when test="$col = 'pf1'">
<xsl:value-of select="key('bk', concat(concat($pf, '-'), $pfd))/br:pf1"/>
</xsl:when>
<xsl:when test="$col = 'pf2'">
<xsl:value-of select="key('bk', concat(concat($pf, '-'), $pfd))/br:pf2"/>
</xsl:when>
<xsl:when test="$col = 'd1'">
<xsl:value-of select="key('bk', concat(concat($pf, '-'), $pfd))/br:d1"/>
</xsl:when>
<xsl:when test="$col = 'd2'">
<xsl:value-of select="key('bk', concat(concat($pf, '-'), $pfd))/br:d2"/>
</xsl:when>
<xsl:when test="$col = 'bu'">
<xsl:value-of select="key('bk', concat(concat($pf, '-'), $pfd))/br:bu"/>
</xsl:when>
<xsl:when test="$col = 'busk'">
<xsl:value-of select="key('bk', concat(concat($pf, '-'), $pfd))/br:busk"/>
</xsl:when>
<xsl:when test="$col = 'sk'">
<xsl:value-of select="key('bk', concat(concat($pf, '-'), $pfd))/br:sk"/>
</xsl:when>
<xsl:when test="$col = 'drm'">
<xsl:value-of select="key('bk', concat(concat($pf, '-'), $pfd))/br:drm"/>
</xsl:when>
<xsl:when test="$col = 'mediatyp'">
<xsl:value-of select="key('bk', concat(concat($pf, '-'), $pfd))/br:mediatyp"/>
</xsl:when>
<xsl:when test="$col = 'ldr'">
<xsl:value-of select="key('bk', concat(concat($pf, '-'), $pfd))/br:ldr"/>
</xsl:when>
<xsl:when test="$col = 'default'">
<xsl:value-of select="key('bk', concat(concat($pf, '-'), $pfd))/br:default"/>
</xsl:when>
<xsl:when test="$col = 'p006'">
<xsl:value-of select="key('bk', concat(concat($pf, '-'), $pfd))/br:p006"/>
</xsl:when>
<xsl:when test="$col = 'p007'">
<xsl:value-of select="key('bk', concat(concat($pf, '-'), $pfd))/br:p007"/>
</xsl:when>
<xsl:when test="$col = 'p245'">
<xsl:value-of select="key('bk', concat(concat($pf, '-'), $pfd))/br:p245"/>
</xsl:when>
<xsl:when test="$col = 'p655'">
<xsl:value-of select="key('bk', concat(concat($pf, '-'), $pfd))/br:p655"/>
</xsl:when>
<xsl:when test="$col = 'p0080017'">
<xsl:value-of select="key('bk', concat(concat($pf, '-'), $pfd))/br:p0080017"/>
</xsl:when>
<xsl:when test="$col = 'p0083539'">
<xsl:value-of select="key('bk', concat(concat($pf, '-'), $pfd))/br:p0083539"/>
</xsl:when>
<xsl:when test="$col = 'p020024'">
<xsl:choose>
<xsl:when test="$pf = 'DA'">
<!--<xsl:value-of select="o:DescriptiveDetail/o:ProductFormDescription"/>-->
<xsl:value-of select="$desc"/>
</xsl:when>
<xsl:otherwise>
<xsl:value-of select="key('bk', concat(concat($pf, '-'), $pfd))/br:p020024"/>
</xsl:otherwise>
</xsl:choose>
</xsl:when>
<!--<xsl:if test="count(key('bk', concat(concat($pf, '-'), $pfd))/br:$col) &gt; 0">
<xsl:value-of select="key('bk', concat(concat($pf, '-'), $pfd))/br:$col"/>
</xsl:if>
-->
<xsl:otherwise>
</xsl:otherwise>
</xsl:choose>
</xsl:for-each>
</xsl:template>

<!-- Bibliographic record -->
<xsl:template name="bibliographic">
<record type="Bibliographic">
<xsl:call-template name="onix_spec"/>
<datafield tag="040" ind1=" " ind2=" ">
<subfield code="a">BOKR</subfield>
</datafield>
<datafield tag="599" ind1=" " ind2=" ">
<subfield code="a">Maskinellt genererad post. Ändra kod för fullständighetsnivå (leader/17), annars kommer manuellt gjorda ändringar att försvinna.</subfield>
</datafield>
<datafield tag="035" ind1=" " ind2=" ">
<subfield code="a">(BOKR)<xsl:value-of select="o:RecordReference"/></subfield>
</datafield>
<xsl:if test="o:DescriptiveDetail/o:Illustrated = '02' or o:DescriptiveDetail/o:Measure[o:MeasureType = '01' and not(o:Measurement = '0' or normalize-space(o:Measurement) = '')] or o:DescriptiveDetail/o:Extent[(o:ExtentType = '00' and o:ExtentUnit = '03' and not(o:ExtentValue = '' or o:ExtentValue = '0' )) or (o:ExtentType = '09' and o:ExtentUnit = '06' and not(o:ExtentValue = '' or o:ExtentValue = '0'))]">
<datafield tag="300" ind1="" ind2="">
<xsl:choose>
<xsl:when test="o:DescriptiveDetail/o:Extent[o:ExtentType = '00' and o:ExtentUnit = '03' and not(o:ExtentValue = '' or o:ExtentValue = '0' )]">
<subfield code="a"><xsl:value-of select="o:DescriptiveDetail/o:Extent[o:ExtentType = '00' and o:ExtentUnit = '03']/o:ExtentValue"/><xsl:text> sidor</xsl:text>
<xsl:choose>
<xsl:when test="o:DescriptiveDetail/o:Illustrated = '02'"><xsl:text> :</xsl:text></xsl:when>
<xsl:when test="o:DescriptiveDetail/o:Measure[o:MeasureType = '01' and not(o:Measurement = '0' or normalize-space(o:Measurement) = '')]"><xsl:text> ;</xsl:text></xsl:when>
</xsl:choose>
</subfield>
</xsl:when>
<xsl:when test="o:DescriptiveDetail/o:Extent[o:ExtentType = '09' and o:ExtentUnit = '06' and not(o:ExtentValue = '' or o:ExtentValue = '0' )]">
<subfield code="a"><xsl:call-template name="secs2hours"><xsl:with-param name="secs"><xsl:value-of select="o:DescriptiveDetail/o:Extent[o:ExtentType = '09' and o:ExtentUnit = '06']/o:ExtentValue"/></xsl:with-param></xsl:call-template><xsl:if test="o:DescriptiveDetail/o:Illustrated = '02' or o:DescriptiveDetail/o:Measure[o:MeasureType = '01' and not(o:Measurement = '0' or normalize-space(o:Measurement) = '')]"><xsl:text> :</xsl:text></xsl:if></subfield>
</xsl:when>
</xsl:choose>
<xsl:if test="o:DescriptiveDetail/o:Illustrated = '02'">
<subfield code="b"><xsl:text>illustrationer</xsl:text><xsl:if test="o:DescriptiveDetail/o:Measure[o:MeasureType = '01' and not(o:Measurement = '0' or normalize-space(o:Measurement) = '')]"><xsl:text> ;</xsl:text></xsl:if></subfield>
</xsl:if>
<xsl:if test="o:DescriptiveDetail/o:Measure[o:MeasureType = '01' and not(o:Measurement = '0' or normalize-space(o:Measurement) = '')]">
<subfield code="c"><xsl:value-of select="format-number((o:DescriptiveDetail/o:Measure[o:MeasureType = '01']/o:Measurement div 10), '#.#')"/><xsl:text> cm</xsl:text></subfield>
</xsl:if>
</datafield>
</xsl:if>
<xsl:variable name="p020024"><xsl:call-template name="brl"><xsl:with-param name="col" select="'p020024'"/></xsl:call-template></xsl:variable>
<xsl:if test="o:ProductIdentifier[o:ProductIDType = '15' and (substring(o:IDValue,1,3) = '979' or substring(o:IDValue,1,3) = '978')]">
<datafield tag="020" ind1=" " ind2=" ">
<subfield code="a"><xsl:value-of select="o:ProductIdentifier[o:ProductIDType = '15']/o:IDValue"/></subfield>
<xsl:if test="$p020024 != ''">
<subfield code="q"><xsl:value-of select="$p020024"/></subfield>
</xsl:if>
</datafield>
</xsl:if>
<xsl:if test="o:ProductIdentifier[o:ProductIDType = '15' and substring(o:IDValue,1,4) = '9790']">
<datafield tag="024" ind1="2" ind2=" ">
<subfield code="a"><xsl:value-of select="o:ProductIdentifier[o:ProductIDType = '15']/o:IDValue"/></subfield>
<xsl:if test="$p020024 != ''">
<subfield code="q"><xsl:value-of select="$p020024"/></subfield>
</xsl:if>
</datafield>
</xsl:if>
<xsl:if test="o:ProductIdentifier[o:ProductIDType = '03' and substring(o:IDValue,1,3) != '978' and substring(o:IDValue,1,3) !='979']">
<datafield tag="024" ind1="3" ind2=" ">
<subfield code="a"><xsl:value-of select="o:ProductIdentifier[o:ProductIDType = '03']/o:IDValue"/></subfield>
<xsl:if test="$p020024 != ''">
<subfield code="q"><xsl:value-of select="$p020024"/></subfield>
</xsl:if>
</datafield>
</xsl:if>
<!-- REDO 245
<xsl:variable name="p245">
<xsl:call-template name="brl"><xsl:with-param name="col" select="'p245'"/></xsl:call-template>
</xsl:variable>
<xsl:choose>
<xsl:when test="o:DescriptiveDetail/o:TitleDetail[o:TitleType = '01' and o:TitleElement[o:TitleElementLevel = '01']]">
<datafield tag="245" ind1="1" ind2="0">
<subfield code="a"><xsl:value-of select="o:DescriptiveDetail/o:TitleDetail[o:TitleType = '01' and o:TitleElement[o:TitleElementLevel = '01']]/o:TitleElement/o:TitleText"/></subfield>
<xsl:if test="$p245 != ''">
<subfield code="h"><xsl:value-of select="$p245"/></subfield>
</xsl:if>
</datafield>
</xsl:when>
<xsl:otherwise>
<datafield tag="245" ind1="1" ind2="0">
<subfield code="a"><xsl:value-of select="o:DescriptiveDetail/o:TitleDetail[o:TitleType = '10' and o:TitleElement[o:TitleElementLevel = '01']]/o:TitleElement/o:TitleText"/></subfield>
<xsl:if test="$p245 != ''">
<subfield code="h"><xsl:value-of select="$p245"/></subfield>
</xsl:if>
</datafield>
</xsl:otherwise>
</xsl:choose>
-->
<xsl:if test="o:DescriptiveDetail/o:TitleDetail[o:TitleType = '03' and o:TitleElement[o:TitleElementLevel = '01']]/o:TitleElement/o:TitleText">
<datafield tag="240" ind1="1" ind2="0">
<subfield code="a"><xsl:value-of select="o:DescriptiveDetail/o:TitleDetail[o:TitleType = '03' and o:TitleElement[o:TitleElementLevel = '01']]/o:TitleElement/o:TitleText"/></subfield>
</datafield>
</xsl:if>
<xsl:if test="o:DescriptiveDetail/o:TitleDetail[o:TitleType = '11' and o:TitleElement[o:TitleElementLevel = '01']]/o:TitleElement/o:TitleText">
<datafield tag="246" ind1="1" ind2="4">
<subfield code="a"><xsl:value-of select="o:DescriptiveDetail/o:TitleDetail[o:TitleType = '11' and o:TitleElement[o:TitleElementLevel = '01']]/o:TitleElement/o:TitleText"/></subfield>
</datafield>
</xsl:if>
<xsl:call-template name="Contributors"/>
<xsl:if test="o:DescriptiveDetail/o:Contributor[o:ContributorRole = 'A01']/o:CorporateName">
<datafield tag="710" ind1="2" ind2=" ">
<subfield code="a"><xsl:value-of select="o:DescriptiveDetail/o:Contributor[o:ContributorRole = 'A01']/o:CorporateName[1]"/></subfield>
</datafield>
</xsl:if>
<xsl:if test="o:DescriptiveDetail[o:EditionNumber != '0' and o:EditionNumber != '1']">
<datafield tag="250" ind1=" " ind2=" ">
<subfield code="a"><xsl:value-of select="o:DescriptiveDetail/o:EditionNumber"/><xsl:text> uppl.</xsl:text></subfield>
</datafield>
</xsl:if>
<xsl:if test="o:CollateralDetail/o:TextContent[o:TextType = '04' and o:ContentAudience = '00']/o:Text">
<datafield tag="505" ind1="0" ind2=" ">
<subfield code="a"><xsl:value-of select="o:CollateralDetail/o:TextContent[o:TextType = '04' and o:ContentAudience = '00']/o:Text"/></subfield>
</datafield>
</xsl:if>
<xsl:if test="o:PublishingDetail/o:Publisher[o:PublishingRole = '01']/o:PublisherName or o:PublishingDetail/o:PublishingDate[o:PublishingDateRole = '01']/o:Date">
<datafield tag="264" ind1=" " ind2="1">
<xsl:if test="o:PublishingDetail/o:Publisher[o:PublishingRole = '01']/o:PublisherName">
<subfield code="b"><xsl:value-of select="o:PublishingDetail/o:Publisher[o:PublishingRole = '01']/o:PublisherName"/><xsl:if test="o:PublishingDetail/o:PublishingDate[o:PublishingDateRole = '01']/o:Date"><xsl:text>,</xsl:text></xsl:if></subfield>
</xsl:if>
<xsl:if test="o:PublishingDetail/o:PublishingDate[o:PublishingDateRole = '01']/o:Date">
<subfield code="c"><xsl:value-of select="substring(o:PublishingDetail/o:PublishingDate[o:PublishingDateRole = '01']/o:Date,1,4)"/></subfield>
</xsl:if>
</datafield>
<xsl:if test="o:PublishingDetail/o:PublishingDate[o:PublishingDateRole = '01']/o:Date">
<datafield tag="263" ind1=" " ind2=" ">
<subfield code="a"><xsl:value-of select="substring(o:PublishingDetail/o:PublishingDate[o:PublishingDateRole = '01']/o:Date,1,6)"/></subfield>
</datafield>
</xsl:if>
<xsl:if test="o:DescriptiveDetail/o:Collection[o:CollectionType = '10']/o:TitleDetail[o:TitleType = '01']/o:TitleElement[o:TitleElementLevel = '02']/o:TitleText[ . != '' ]">
<datafield tag="490" ind1="0" ind2=" ">
<subfield code="a"><xsl:value-of select="o:DescriptiveDetail/o:Collection[o:CollectionType = '10']/o:TitleDetail[o:TitleType = '01']/o:TitleElement[o:TitleElementLevel = '02']/o:TitleText"/></subfield>
</datafield>
</xsl:if>
</xsl:if>
<!--
<xsl:variable name="p655">
<xsl:call-template name="brl"><xsl:with-param name="col" select="'p655'"/></xsl:call-template>
</xsl:variable>
<xsl:if test="$p655 != ''">
<xsl:choose>
<xsl:when test="contains($p655, 'Ljud')">
<datafield tag="655" ind1=" " ind2="7">
<subfield code="a">Ljudböcker</subfield>
<subfield code="2">saogf</subfield>
</datafield>
</xsl:when>
<xsl:otherwise>
<datafield tag="655" ind1=" " ind2="4">
<subfield code="a">E-böcker</subfield>
</datafield>
</xsl:otherwise>
</xsl:choose>
</xsl:if>
-->
</record>
</xsl:template>

<xsl:template name="ind2filering">
<xsl:param name="title"/>
<xsl:variable name="begin">
<xsl:value-of select="translate(normalize-space(translate(substring-before($title,' '), '.,;:[]{}()/+-$%#!','                 ')), $uc, $lc)"/>
</xsl:variable>
<xsl:variable name="count">
<xsl:choose>
<xsl:when test="$begin = 'en' or $begin = 'ett' or $begin = 'den' or $begin = 'det' or $begin = 'de' or $begin = 'the' or $begin ='a' or $begin = 'an'">
<xsl:value-of select="string-length(substring-before($title,' '))+1"/>
</xsl:when>
<xsl:otherwise>
<xsl:value-of select="'0'"/>
</xsl:otherwise>
</xsl:choose>
</xsl:variable>
<xsl:choose>
<xsl:when test="$count &lt; '10'">
<xsl:value-of select="$count"/>
</xsl:when>
<xsl:otherwise> 
<xsl:value-of select="'9'"/>
</xsl:otherwise> 
</xsl:choose>

</xsl:template>

<xsl:template name="t655">
<xsl:variable name="p655">
<xsl:call-template name="brl"><xsl:with-param name="col" select="'p655'"/></xsl:call-template>
</xsl:variable>
<xsl:if test="$p655 != ''">
<xsl:choose>
<xsl:when test="contains(translate($p655,$uc,$lc), 'ljud')">
<datafield tag="655" ind1=" " ind2="7">
<subfield code="a">Ljudböcker</subfield>
<subfield code="2">saogf</subfield>
</datafield>
</xsl:when>
<xsl:otherwise>
<datafield tag="655" ind1=" " ind2="4">
<subfield code="a">E-böcker</subfield>
</datafield>
</xsl:otherwise>
</xsl:choose>
</xsl:if>
</xsl:template>

<xsl:template name="secs2hours">
<xsl:param name="secs"/>
<xsl:variable name="hours"><xsl:value-of select="format-number(floor($secs div 3600), '#')"/></xsl:variable>
<xsl:variable name="mins"><xsl:value-of select="format-number((($secs - (3600*$hours)) div 60), '#')"/></xsl:variable>
<xsl:if test="$hours != 0">
<xsl:value-of select="$hours"/><xsl:text> tim.</xsl:text><xsl:if test="$mins != 0"><xsl:text>, </xsl:text></xsl:if>
</xsl:if>
<xsl:if test="$mins != 0">
<xsl:value-of select="$mins"/><xsl:text> min.</xsl:text>
</xsl:if>
</xsl:template>

<xsl:template name="onix_spec">
<xsl:choose>
<xsl:when test="o:DescriptiveDetail/o:TitleDetail[o:TitleType = '01']/o:TitleElement[o:TitleElementLevel = '01']/o:TitleText[contains(translate(translate(.,$uc,$lc), '(){}[]+-/:;', '          '), ' bok ') and (contains(translate(.,$uc,$lc), 'ljudbok') or contains(translate(translate(.,$uc,$lc), '(){}[]+-/:;', '          '), ' cd'))]">
<leader>*****nom a22*****8i 4500</leader>
<controlfield tag="006">a||||      |00| ||</controlfield>
<controlfield tag="006">i||||  |||||||| | </controlfield>
<controlfield tag="007">ta</controlfield>
<controlfield tag="007">sd||||||||||||</controlfield>
<xsl:variable name="filering">
<xsl:call-template name="ind2filering"><xsl:with-param name="title" select="o:DescriptiveDetail/o:TitleDetail[o:TitleType = '01' and o:TitleElement[o:TitleElementLevel = '01']]/o:TitleElement/o:TitleText"/></xsl:call-template>
</xsl:variable>
<datafield tag="245" ind1="1" ind2="{$filering}">
<!--<subfield code="a"><xsl:value-of select="o:DescriptiveDetail/o:TitleDetail[o:TitleType = '01' and o:TitleElement[o:TitleElementLevel = '01']]/o:TitleElement/o:TitleText"/></subfield>-->
<xsl:call-template name="t245ab"><xsl:with-param name="sfa" select="o:DescriptiveDetail/o:TitleDetail[o:TitleType = '01' and o:TitleElement[o:TitleElementLevel = '01']]/o:TitleElement/o:TitleText"/></xsl:call-template>
</datafield>
<controlfield tag="008">
<xsl:value-of select="$datestamp"/><xsl:text>s</xsl:text><xsl:choose><xsl:when test="o:PublishingDetail/o:PublishingDate[o:PublishingDateRole = '01']/o:Date"><xsl:value-of select="substring(o:PublishingDetail/o:PublishingDate[o:PublishingDateRole = '01']/o:Date,1,4)"/></xsl:when><xsl:otherwise><xsl:text>    </xsl:text></xsl:otherwise></xsl:choose><xsl:text>    </xsl:text><xsl:choose><xsl:when test="o:ProductIdentifier[o:ProductIDType = '15']/o:IDValue[substring(.,1,5) = '97891' or substring(.,1,2) = '91']"><xsl:text>sw</xsl:text></xsl:when><xsl:otherwise><xsl:text>xx</xsl:text></xsl:otherwise></xsl:choose><xsl:text> </xsl:text><xsl:call-template name="t008st"/><xsl:value-of select="o:DescriptiveDetail/o:Language[o:LanguageRole = '01']/o:LanguageCode"/><xsl:text> d</xsl:text>
</controlfield>
<!-- 336 -->
<xsl:call-template name="t336all"/>
</xsl:when>
<xsl:when test="o:DescriptiveDetail/o:TitleDetail[o:TitleType = '10']/o:TitleElement[o:TitleElementLevel = '01']/o:TitleText[contains(translate(translate(.,$uc,$lc), '(){}[]+-/:;', '          '), ' bok ') and (contains(translate(.,$uc,$lc), 'ljudbok') or contains(translate(translate(.,$uc,$lc), '(){}[]+-/:;', '          '), ' cd'))]">
<leader>*****nom a22*****8i 4500</leader>
<controlfield tag="006">a||||      |00| ||</controlfield>
<controlfield tag="006">i||||  |||||||| | </controlfield>
<controlfield tag="007">ta</controlfield>
<controlfield tag="007">sd||||||||||||</controlfield>
<xsl:variable name="filering">
<xsl:call-template name="ind2filering"><xsl:with-param name="title" select="o:DescriptiveDetail/o:TitleDetail[o:TitleType = '10' and o:TitleElement[o:TitleElementLevel = '01']]/o:TitleElement/o:TitleText"/></xsl:call-template>
</xsl:variable>
<datafield tag="245" ind1="1" ind2="{$filering}">
<!--<subfield code="a"><xsl:value-of select="o:DescriptiveDetail/o:TitleDetail[o:TitleType = '10' and o:TitleElement[o:TitleElementLevel = '01']]/o:TitleElement/o:TitleText"/></subfield>-->
<xsl:call-template name="t245ab"><xsl:with-param name="sfa" select="o:DescriptiveDetail/o:TitleDetail[o:TitleType = '10' and o:TitleElement[o:TitleElementLevel = '01']]/o:TitleElement/o:TitleText"/></xsl:call-template>
</datafield>
<xsl:call-template name="t655"/>
<controlfield tag="008">
<xsl:value-of select="$datestamp"/><xsl:text>s</xsl:text><xsl:choose><xsl:when test="o:PublishingDetail/o:PublishingDate[o:PublishingDateRole = '01']/o:Date"><xsl:value-of select="substring(o:PublishingDetail/o:PublishingDate[o:PublishingDateRole = '01']/o:Date,1,4)"/></xsl:when><xsl:otherwise><xsl:text>    </xsl:text></xsl:otherwise></xsl:choose><xsl:text>    </xsl:text><xsl:choose><xsl:when test="o:ProductIdentifier[o:ProductIDType = '15']/o:IDValue[substring(.,1,5) = '97891' or substring(.,1,2) = '91']"><xsl:text>sw</xsl:text></xsl:when><xsl:otherwise><xsl:text>xx</xsl:text></xsl:otherwise></xsl:choose><xsl:text> </xsl:text><xsl:call-template name="t008st"/><xsl:value-of select="o:DescriptiveDetail/o:Language[o:LanguageRole = '01']/o:LanguageCode"/><xsl:text> d</xsl:text>
</controlfield>
<!-- 336 -->
<xsl:call-template name="t336all"/>
</xsl:when>
<xsl:otherwise>
<leader><xsl:call-template name="brl"><xsl:with-param name="col" select="'ldr'"/></xsl:call-template></leader>
<xsl:variable name="p007"><xsl:call-template name="brl"><xsl:with-param name="col" select="'p007'"/></xsl:call-template></xsl:variable>
<xsl:if test="$p007 != ''">
<xsl:choose>
<xsl:when test="contains($p007, ';')"> <!-- table has at most 2 values in cell -->
<controlfield tag="007"><xsl:value-of select="substring-before($p007, ';')"/></controlfield>
<controlfield tag="007"><xsl:value-of select="substring-after($p007, ';')"/></controlfield>
</xsl:when>
<xsl:otherwise>
<controlfield tag="007"><xsl:value-of select="$p007"/></controlfield>
</xsl:otherwise>
</xsl:choose>
</xsl:if>
<!--<xsl:variable name="p245">
<xsl:call-template name="brl"><xsl:with-param name="col" select="'p245'"/></xsl:call-template>
</xsl:variable>-->
<xsl:variable name="filering">
<xsl:choose>
<xsl:when test="o:DescriptiveDetail/o:TitleDetail[o:TitleType = '01' and o:TitleElement[o:TitleElementLevel = '01']]/o:TitleElement/o:TitleText">
<xsl:call-template name="ind2filering"><xsl:with-param name="title" select="o:DescriptiveDetail/o:TitleDetail[o:TitleType = '01' and o:TitleElement[o:TitleElementLevel = '01']]/o:TitleElement/o:TitleText"/></xsl:call-template>
</xsl:when>
<xsl:when test="o:DescriptiveDetail/o:TitleDetail[o:TitleType = '10' and o:TitleElement[o:TitleElementLevel = '01']]/o:TitleElement/o:TitleText">
<xsl:call-template name="ind2filering"><xsl:with-param name="title" select="o:DescriptiveDetail/o:TitleDetail[o:TitleType = '10' and o:TitleElement[o:TitleElementLevel = '01']]/o:TitleElement/o:TitleText"/></xsl:call-template>
</xsl:when>
</xsl:choose>
</xsl:variable>
<datafield tag="245" ind1="1" ind2="{$filering}">
<!--<subfield code="a">-->
<xsl:choose>
<xsl:when test="o:DescriptiveDetail/o:TitleDetail[o:TitleType = '01' and o:TitleElement[o:TitleElementLevel = '01']]/o:TitleElement/o:TitleText">
<!--<xsl:value-of select="o:DescriptiveDetail/o:TitleDetail[o:TitleType = '01' and o:TitleElement[o:TitleElementLevel = '01']]/o:TitleElement/o:TitleText"/>-->
<xsl:call-template name="t245ab"><xsl:with-param name="sfa" select="o:DescriptiveDetail/o:TitleDetail[o:TitleType = '01' and o:TitleElement[o:TitleElementLevel = '01']]/o:TitleElement/o:TitleText"/></xsl:call-template>
</xsl:when>
<xsl:when test="o:DescriptiveDetail/o:TitleDetail[o:TitleType = '10' and o:TitleElement[o:TitleElementLevel = '01']]/o:TitleElement/o:TitleText">
<!--<xsl:value-of select="o:DescriptiveDetail/o:TitleDetail[o:TitleType = '10' and o:TitleElement[o:TitleElementLevel = '01']]/o:TitleElement/o:TitleText"/>-->
<xsl:call-template name="t245ab"><xsl:with-param name="sfa" select="o:DescriptiveDetail/o:TitleDetail[o:TitleType = '10' and o:TitleElement[o:TitleElementLevel = '01']]/o:TitleElement/o:TitleText"/></xsl:call-template>
</xsl:when>
</xsl:choose>
<!--</subfield>-->
<!--<xsl:if test="$p245 != ''">
<subfield code="h"><xsl:value-of select="$p245"/></subfield>
</xsl:if>-->
</datafield>
<xsl:call-template name="t655"/>
<controlfield tag="008">
<xsl:value-of select="$datestamp"/><xsl:text>s</xsl:text><xsl:choose><xsl:when test="o:PublishingDetail/o:PublishingDate[o:PublishingDateRole = '01']/o:Date"><xsl:value-of select="substring(o:PublishingDetail/o:PublishingDate[o:PublishingDateRole = '01']/o:Date,1,4)"/></xsl:when><xsl:otherwise><xsl:text>    </xsl:text></xsl:otherwise></xsl:choose><xsl:text>    </xsl:text><xsl:choose><xsl:when test="o:ProductIdentifier[o:ProductIDType = '15']/o:IDValue[substring(.,1,5) = '97891' or substring(.,1,2) = '91']"><xsl:text>sw</xsl:text></xsl:when><xsl:otherwise><xsl:text>xx</xsl:text></xsl:otherwise></xsl:choose><xsl:text> </xsl:text><xsl:call-template name="brl"><xsl:with-param name="col"><xsl:call-template name="subject_type"/></xsl:with-param></xsl:call-template><xsl:value-of select="o:DescriptiveDetail/o:Language[o:LanguageRole = '01']/o:LanguageCode"/><xsl:text> d</xsl:text>
</controlfield>
<!-- 336 -->
<xsl:call-template name="resolve336338"/>
</xsl:otherwise>
</xsl:choose>
</xsl:template>

<xsl:template name="t008st">
<xsl:variable name="st">
<xsl:call-template name="subject_type"/>
</xsl:variable>
<xsl:choose>
<xsl:when test="$st = 'bu'">
<xsl:value-of select="'||| j     |    b|'"/>
</xsl:when>
<xsl:when test="$st = 'sk'">
<xsl:value-of select="'|||       |    b|'"/>
</xsl:when>
<xsl:when test="$st = 'busk'">
<xsl:value-of select="'||| j     |    b|'"/>
</xsl:when>
<xsl:when test="$st = 'default'">
<xsl:value-of select="'|||       |    b|'"/>
</xsl:when>
<xsl:otherwise>
</xsl:otherwise>
</xsl:choose>
</xsl:template>

<xsl:template name="t245ab">
<xsl:param name="sfa"/>
<xsl:choose>
<xsl:when test="contains($sfa, ': ')">
<subfield code="a"><xsl:value-of select="concat(substring-before($sfa,':'), ':')"/></subfield>
<subfield code="b"><xsl:value-of select="substring-after($sfa, ': ')"/></subfield>
</xsl:when>
<xsl:otherwise>
<subfield code="a"><xsl:value-of select="$sfa"/></subfield>
</xsl:otherwise>
</xsl:choose>
</xsl:template>

<!-- Holding records -->
<xsl:template name="holding">
<record type="Holdings">
<leader>*****nx  a22*****1n 4500</leader>
<controlfield tag="008"><xsl:value-of select="$datestamp"/>||0000|||||000||||||000000</controlfield>
<datafield ind1=" " ind2=" " tag="852">
<subfield code="b">BOKR</subfield>
</datafield>
<!--<xsl:apply-templates select="artikelnummer" mode="create035holdings"/>-->
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

<!--
<subfield code="z">Online Flash player (<xsl:call-template name="SizeMb"><xsl:with-param name="bytes" select="SizeInBytes"/></xsl:call-template>)</subfield> 
-->

<xsl:template name="ProductId">
<controlfield tag="001">
<xsl:value-of select="."/>
</controlfield>
<datafield tag="035" ind1=" " ind2=" ">
<subfield code="a">Elib<xsl:value-of select="."/></subfield>
</datafield>
</xsl:template>

<xsl:template name="subject_type"> <!-- column for brl -->
<xsl:choose>
<xsl:when test="o:DescriptiveDetail/o:Subject[o:SubjectSchemeIdentifier = '93']/o:SubjectCode[substring(.,1,3) = 'DSY']">
<xsl:value-of select="'bu'"/>
</xsl:when>
<xsl:when test="o:DescriptiveDetail/o:Subject[o:SubjectSchemeIdentifier = '93']/o:SubjectCode[substring(.,1,3) = 'YDP' or substring(.,1,2) = 'YF' or substring(.,1,2) = 'YZ']">
<xsl:value-of select="'busk'"/>
</xsl:when>
<xsl:when test="o:DescriptiveDetail/o:Subject[o:SubjectSchemeIdentifier = '93']/o:SubjectCode[substring(.,1,1) = 'Y']">
<xsl:value-of select="'bu'"/>
</xsl:when>
<xsl:when test="o:DescriptiveDetail/o:Subject[o:SubjectSchemeIdentifier = '93']/o:SubjectCode[substring(.,1,2) = 'DC' or substring(.,1,2) = 'DD' or substring(.,1,1) = 'F' or substring(.,1,1) = 'X' or substring(.,1,1) = 'Y']">
<xsl:value-of select="'sk'"/>
</xsl:when>
<xsl:otherwise>
<xsl:value-of select="'default'"/>
</xsl:otherwise>
</xsl:choose>
</xsl:template>

<xsl:template name="Contributors">
<xsl:for-each select="o:DescriptiveDetail/o:Contributor[o:ContributorRole = 'A01']">
<xsl:choose>
<xsl:when test="position() = 1">
<xsl:call-template name="output_role">
<xsl:with-param name="tag" select="'100'"/>
<xsl:with-param name="fn" select="o:PersonNameInverted"/>
<!--<xsl:with-param name="fn" select="FirstName"/>
<xsl:with-param name="ln" select="LastName"/>-->
<xsl:with-param name="role" select="o:ContributorRole"/>
<xsl:with-param name="ps" select="o:NameType[. = '01']"/>
</xsl:call-template>
</xsl:when>
<xsl:otherwise>
<xsl:call-template name="output_role">
<xsl:with-param name="tag" select="'700'"/>
<xsl:with-param name="fn" select="o:PersonNameInverted"/>
<!--<xsl:with-param name="fn" select="FirstName"/>
<xsl:with-param name="ln" select="LastName"/>-->
<xsl:with-param name="role" select="o:ContributorRole"/>
<xsl:with-param name="ps" select="o:NameType[. = '01']"/>
</xsl:call-template>
</xsl:otherwise>
</xsl:choose>
</xsl:for-each>
<xsl:for-each select="o:DescriptiveDetail/o:Contributor[o:ContributorRole != 'A01' and o:ContributorRole != 'A11' and o:ContributorRole != 'A36']">
<xsl:call-template name="output_role">
<xsl:with-param name="tag" select="'700'"/>
<xsl:with-param name="fn" select="o:PersonNameInverted"/>
<!--<xsl:with-param name="fn" select="FirstName"/>
<xsl:with-param name="ln" select="LastName"/>-->
<xsl:with-param name="role" select="o:ContributorRole"/>
<xsl:with-param name="ps" select="''"/>
</xsl:call-template>
</xsl:for-each>
</xsl:template>

<xsl:template name="contributor_role">
<xsl:param name="role"/>
<xsl:choose>
<xsl:when test="$role = 'A01'">aut</xsl:when>
<xsl:when test="$role = 'A06'">cmp</xsl:when>
<xsl:when test="$role = 'A07'">art</xsl:when>
<xsl:when test="$role = 'A11'">formgivare</xsl:when>
<xsl:when test="$role = 'A12'">ill</xsl:when>
<xsl:when test="$role = 'A13'">pht</xsl:when>
<xsl:when test="$role = 'A19'">aft</xsl:when>
<xsl:when test="$role = 'A23'">aui</xsl:when>
<xsl:when test="$role = 'A36'">omslag</xsl:when>
<xsl:when test="$role = 'A99'">oth</xsl:when>
<xsl:when test="$role = 'B06'">trl</xsl:when>
<xsl:when test="$role = 'B11'">edt</xsl:when>
<xsl:when test="$role = 'B13'">edt</xsl:when>
<xsl:when test="$role = 'B21'">edt</xsl:when>
<xsl:when test="$role = 'E07'">nrt</xsl:when>
</xsl:choose>
</xsl:template>

<xsl:template name="output_role">
<xsl:param name="tag"/>
<xsl:param name="fn"/>
<xsl:param name="ps"/>
<!--<xsl:param name="ln"/>-->
<xsl:param name="role"/>
<datafield tag="{$tag}" ind1="1" ind2=" ">
<subfield code="a"><xsl:value-of select="$fn"/><xsl:if test="$ps != ''"><xsl:text>,</xsl:text></xsl:if></subfield>
<xsl:if test="$ps != ''">
<subfield code="c">(pseud.)</subfield>
</xsl:if>
<!--<subfield code="a"><xsl:value-of select="$ln"/>, <xsl:value-of select="$fn"/></subfield>-->
<subfield code="4"><xsl:call-template name="contributor_role"><xsl:with-param name="role" select="$role"/></xsl:call-template></subfield>
</datafield>
</xsl:template>

<!--
<xsl:template name="BookLength">
<xsl:if test="./Value != ''">
<datafield tag="300" ind1=" " ind2=" ">
<subfield code="a"><xsl:value-of select="./Value"/>
<xsl:choose>
<xsl:when test="./Type = 'Seconds'"> sek.</xsl:when>
<xsl:when test="./Type = 'Pages'"> sidor</xsl:when>
<xsl:otherwise></xsl:otherwise>
</xsl:choose>
</subfield>
</datafield>
</xsl:if>
</xsl:template>
-->

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
<xsl:otherwise>   </xsl:otherwise>
</xsl:choose>
</xsl:template>

<xsl:template name="t336all">
<datafield tag="336" ind1=" " ind2=" ">
<subfield code="3">Textbok</subfield>
<subfield code="b">txt</subfield>
<subfield code="2">rdacontent</subfield>
</datafield>
<datafield tag="336" ind1=" " ind2=" ">
<subfield code="3">Ljudbok</subfield>
<subfield code="b">spw</subfield>
<subfield code="2">rdacontent</subfield>
</datafield>
<datafield tag="337" ind1=" " ind2=" ">
<subfield code="3">Textbok</subfield>
<subfield code="b">n</subfield>
<subfield code="2">rdamedia</subfield>
</datafield>
<datafield tag="337" ind1=" " ind2=" ">
<subfield code="3">Ljudbok</subfield>
<subfield code="b">s</subfield>
<subfield code="2">rdamedia</subfield>
</datafield>
<datafield tag="338" ind1=" " ind2=" ">
<subfield code="3">Textbok</subfield>
<subfield code="b">nc</subfield>
<subfield code="2">rdacarrier</subfield>
</datafield>
<datafield tag="338" ind1=" " ind2=" ">
<subfield code="3">Ljudbok</subfield>
<subfield code="b">sd</subfield>
<subfield code="2">rdacarrier</subfield>
</datafield>
</xsl:template>

<xsl:template name="resolve336338">
<xsl:variable name="pf" select="o:DescriptiveDetail/o:ProductForm"/>
<xsl:variable name="pd" select="o:DescriptiveDetail/o:ProductFormDetail"/>
<xsl:choose>
<xsl:when test="$pf = 'BB' and (not(o:DescriptiveDetail/o:ProductFormDetail) or $pd='' or $pd='B401' or $pd='B409' or $pd='B115' or $pd='B308')"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'Bok'"/></xsl:call-template></xsl:when>
<xsl:when test="$pf = 'BC' and ($pd='B504' or $pd='B116' or $pd='B310' or $pd='B113' or $pd='B114' or $pd='B118')"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'Bok'"/></xsl:call-template></xsl:when>
<xsl:when test="$pf = 'BE' and (not(o:DescriptiveDetail/o:ProductFormDetail) or $pd='')"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'Bok'"/></xsl:call-template></xsl:when>
<xsl:when test="$pf = 'BH' and (not(o:DescriptiveDetail/o:ProductFormDetail) or $pd='')"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'Bok'"/></xsl:call-template></xsl:when>
<xsl:when test="$pf = 'BZ' and (not(o:DescriptiveDetail/o:ProductFormDetail) or $pd='')"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'Bok'"/></xsl:call-template></xsl:when>
<xsl:when test="$pf = 'AC' and ($pd='A101' or $pd='A103')"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'Ljudbok CD/DVD'"/></xsl:call-template></xsl:when>
<xsl:when test="$pf = 'AI'"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'Ljudbok CD/DVD'"/></xsl:call-template></xsl:when>
<xsl:when test="$pf = 'AB'"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'Ljudbok kassett'"/></xsl:call-template></xsl:when>
<xsl:when test="$pf = 'AK'"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'Ljudbok fil/minneskort'"/></xsl:call-template></xsl:when>
<xsl:when test="$pf = 'AL'"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'Ljudbok fil/minneskort'"/></xsl:call-template></xsl:when>
<xsl:when test="$pf = 'DM'"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'Ljudbok fil/minneskort'"/></xsl:call-template></xsl:when>
<xsl:when test="$pf = 'DB' and (not(o:DescriptiveDetail/o:ProductFormDetail) or $pd='' or $pd='D202' or $pd='D203')"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'Multimedia'"/></xsl:call-template></xsl:when>
<xsl:when test="$pf = 'DI' and (not(o:DescriptiveDetail/o:ProductFormDetail) or $pd='' or $pd='D202' or $pd='D203')"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'Multimedia'"/></xsl:call-template></xsl:when>
<xsl:when test="$pf = 'AJ' and ($pd='A103')"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'E-ljudbok'"/></xsl:call-template></xsl:when>
<xsl:when test="$pf = 'EA' and (not(o:DescriptiveDetail/o:ProductFormDetail) or $pd='' or $pd='E101' or $pd='E107')"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'E-bok'"/></xsl:call-template></xsl:when>
<xsl:when test="$pf = 'CB'"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'Karta'"/></xsl:call-template></xsl:when>
<xsl:when test="$pf = 'CC'"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'Karta'"/></xsl:call-template></xsl:when>
<xsl:when test="$pf = 'CD'"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'Karta'"/></xsl:call-template></xsl:when>
<xsl:when test="$pf = 'CE'"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'Jordglob'"/></xsl:call-template></xsl:when>
<xsl:when test="$pf = 'CZ'"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'Annan karttyp'"/></xsl:call-template></xsl:when>
<xsl:when test="$pf = 'DA'"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'E-bok'"/></xsl:call-template></xsl:when>
<xsl:when test="$pf = '00'">
<xsl:variable name="pfd" select="o:DescriptiveDetail/o:ProductFormDescription"/>
<xsl:choose>
<xsl:when test="$pfd = 'CD'"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'Ljudbok CD/DVD'"/></xsl:call-template></xsl:when>
<xsl:when test="$pfd = 'CD-skiva'"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'Ljudbok CD/DVD'"/></xsl:call-template></xsl:when>
<xsl:when test="contains(translate($pfd,$uc,$lc), 'daisy')"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'E-bok'"/></xsl:call-template></xsl:when>
<xsl:when test="$pfd = 'Häftad'"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'Bok'"/></xsl:call-template></xsl:when>
<xsl:when test="$pfd = 'Kodkort'"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'E-bok'"/></xsl:call-template></xsl:when>
<xsl:when test="contains($pfd,'Licens')"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'E-bok'"/></xsl:call-template></xsl:when>
<xsl:when test="$pfd = 'DVD'"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'Video'"/></xsl:call-template></xsl:when>
<xsl:when test="$pfd = 'Online'"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'E-bok'"/></xsl:call-template></xsl:when>
<xsl:when test="$pfd = 'Onlinebok'"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'E-bok'"/></xsl:call-template></xsl:when>
<xsl:when test="$pfd = 'Onlineprodukt'"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'E-bok'"/></xsl:call-template></xsl:when>
<xsl:when test="$pfd = 'PDF'"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'E-bok'"/></xsl:call-template></xsl:when>
<xsl:when test="$pfd = 'Specialbindning'"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'Bok'"/></xsl:call-template></xsl:when>
<xsl:when test="$pfd = 'Webbapplikation'"><xsl:call-template name="t336338"><xsl:with-param name="content" select="'E-bok'"/></xsl:call-template></xsl:when>
</xsl:choose>
</xsl:when>
</xsl:choose>
</xsl:template>

<xsl:template name="t336338">
<xsl:param name="content"/>
<xsl:choose>
<xsl:when test="$content = 'Bok'">
<datafield tag="336" ind1=" " ind2=" ">
<subfield code="b">txt</subfield>
<subfield code="2">rdacontent</subfield>
</datafield>
<datafield tag="337" ind1=" " ind2=" ">
<subfield code="b">n</subfield>
<subfield code="2">rdamedia</subfield>
</datafield>
<datafield tag="338" ind1=" " ind2=" ">
<subfield code="b">nc</subfield>
<subfield code="2">rdacarrier</subfield>
</datafield>
</xsl:when>
<xsl:when test="$content = 'Ljudbok CD/DVD'">
<datafield tag="336" ind1=" " ind2=" ">
<subfield code="b">spw</subfield>
<subfield code="2">rdacontent</subfield>
</datafield>
<datafield tag="337" ind1=" " ind2=" ">
<subfield code="b">s</subfield>
<subfield code="2">rdamedia</subfield>
</datafield>
<datafield tag="338" ind1=" " ind2=" ">
<subfield code="b">sd</subfield>
<subfield code="2">rdacarrier</subfield>
</datafield>
</xsl:when>
<xsl:when test="$content = 'Ljudbok kassett'">
<datafield tag="336" ind1=" " ind2=" ">
<subfield code="b">spw</subfield>
<subfield code="2">rdacontent</subfield>
</datafield>
<datafield tag="337" ind1=" " ind2=" ">
<subfield code="b">s</subfield>
<subfield code="2">rdamedia</subfield>
</datafield>
<datafield tag="338" ind1=" " ind2=" ">
<subfield code="b">ss</subfield>
<subfield code="2">rdacarrier</subfield>
</datafield>
</xsl:when>
<xsl:when test="$content = 'Ljudbok fil/minneskort'">
<datafield tag="336" ind1=" " ind2=" ">
<subfield code="b">spw</subfield>
<subfield code="2">rdacontent</subfield>
</datafield>
<datafield tag="337" ind1=" " ind2=" ">
<subfield code="b">c</subfield>
<subfield code="2">rdamedia</subfield>
</datafield>
<datafield tag="338" ind1=" " ind2=" ">
<subfield code="b">cz</subfield>
<subfield code="2">rdacarrier</subfield>
</datafield>
</xsl:when>
<xsl:when test="$content = 'E-ljudbok'">
<datafield tag="336" ind1=" " ind2=" ">
<subfield code="b">spw</subfield>
<subfield code="2">rdacontent</subfield>
</datafield>
<datafield tag="337" ind1=" " ind2=" ">
<subfield code="b">c</subfield>
<subfield code="2">rdamedia</subfield>
</datafield>
<datafield tag="338" ind1=" " ind2=" ">
<subfield code="b">cr</subfield>
<subfield code="2">rdacarrier</subfield>
</datafield>
</xsl:when>
<xsl:when test="$content = 'Multimedia'">
<datafield tag="336" ind1=" " ind2=" ">
<subfield code="b">cop</subfield>
<subfield code="2">rdacontent</subfield>
</datafield>
<datafield tag="337" ind1=" " ind2=" ">
<subfield code="b">c</subfield>
<subfield code="2">rdamedia</subfield>
</datafield>
<datafield tag="338" ind1=" " ind2=" ">
<subfield code="b">cd</subfield>
<subfield code="2">rdacarrier</subfield>
</datafield>
</xsl:when>
<xsl:when test="$content = 'Video'">
<datafield tag="336" ind1=" " ind2=" ">
<subfield code="b">tdi</subfield>
<subfield code="2">rdacontent</subfield>
</datafield>
<datafield tag="337" ind1=" " ind2=" ">
<subfield code="b">v</subfield>
<subfield code="2">rdamedia</subfield>
</datafield>
<datafield tag="338" ind1=" " ind2=" ">
<subfield code="b">vd</subfield>
<subfield code="2">rdacarrier</subfield>
</datafield>
</xsl:when>
<xsl:when test="$content = 'Karta'">
<datafield tag="336" ind1=" " ind2=" ">
<subfield code="b">cri</subfield>
<subfield code="2">rdacontent</subfield>
</datafield>
<datafield tag="337" ind1=" " ind2=" ">
<subfield code="b">n</subfield>
<subfield code="2">rdamedia</subfield>
</datafield>
<datafield tag="338" ind1=" " ind2=" ">
<subfield code="b">nb</subfield>
<subfield code="2">rdacarrier</subfield>
</datafield>
</xsl:when>
<xsl:when test="$content = 'Jordglob'">
<datafield tag="336" ind1=" " ind2=" ">
<subfield code="b">crf</subfield>
<subfield code="2">rdacontent</subfield>
</datafield>
<datafield tag="337" ind1=" " ind2=" ">
<subfield code="b">n</subfield>
<subfield code="2">rdamedia</subfield>
</datafield>
<datafield tag="338" ind1=" " ind2=" ">
<subfield code="b">nz</subfield>
<subfield code="2">rdacarrier</subfield>
</datafield>
</xsl:when>
<xsl:when test="$content = 'Annan karttyp'">
<datafield tag="336" ind1=" " ind2=" ">
<subfield code="b">cri</subfield>
<subfield code="2">rdacontent</subfield>
</datafield>
<datafield tag="337" ind1=" " ind2=" ">
<subfield code="b">n</subfield>
<subfield code="2">rdamedia</subfield>
</datafield>
<datafield tag="338" ind1=" " ind2=" ">
<subfield code="b">nz</subfield>
<subfield code="2">rdacarrier</subfield>
</datafield>
</xsl:when>
</xsl:choose>
</xsl:template>

<xsl:template name="parseyear">
<xsl:param name="string"/>
<xsl:param name="result"/>
<xsl:choose>
<xsl:when test="$string = ''">
<xsl:value-of select="$result"/>
</xsl:when>
<xsl:when test="contains('0123456789', substring($string,1,1))">
<xsl:call-template name="parseyear"><xsl:with-param name="result" select="concat($result, substring($string,1,1))"/><xsl:with-param name="string" select="substring($string,2)"/></xsl:call-template>
</xsl:when>
<xsl:when test="$result = ''">
<xsl:call-template name="parseyear"><xsl:with-param name="result" select="''"/><xsl:with-param name="string" select="substring($string,2)"/></xsl:call-template>
</xsl:when>
<xsl:otherwise>
<xsl:call-template name="parseyear"><xsl:with-param name="result" select="$result"/><xsl:with-param name="string" select="''"/></xsl:call-template>
</xsl:otherwise>
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
