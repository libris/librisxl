<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:marc="http://www.loc.gov/MARC21/slim" xmlns:java="http://xml.apache.org/xslt/java" xmlns:mtm="http://mtm.se/Schemas/MarcSlimExtension" version="1.0" exclude-result-prefixes="mtm marc java">

<xsl:output method="xml" omit-xml-declaration="no" indent="no"/>
<xsl:strip-space elements="*"/>

<xsl:template match="/marc:collection">
<collection xmlns="http://www.loc.gov/MARC21/slim">
<xsl:apply-templates select="marc:record"/>
</collection>
</xsl:template>

<xsl:template match="marc:record">
<xsl:if test="substring(./marc:leader, 18, 1) = '7' and ./marc:datafield[@tag ='887' and @ind1='0']/marc:subfield[@code='b' and (substring(., 2, 2) = 'GL' or substring(., 2, 2) = 'IC' or substring(., 2, 2) = 'LC')]">
<record type="Bibliographic">
<xsl:apply-templates select="marc:leader"/>
<xsl:apply-templates select="marc:controlfield"/>
<controlfield tag="001"><xsl:value-of select="./marc:datafield[@tag='029' and @ind1='3']/marc:subfield[@code='a']"/></controlfield>
<xsl:call-template name="t006"/>
<xsl:call-template name="t249"/>
<xsl:apply-templates select="marc:datafield"/>
<datafield ind1=" " ind2=" " tag="040">
<subfield code='a'>Mtm</subfield>
</datafield>
<datafield ind1=" " ind2=" " tag="042">
<subfield code='9'>Mtm</subfield>
</datafield>
<datafield ind1=" " ind2=" " tag="599">
<subfield code='a'>MTM MarcRecordId: <xsl:value-of select="@mtm:id"/></subfield>
</datafield>
<datafield ind1="1" ind2=" " tag="506">
<subfield code='a'>Tillgänglig för personer med läsnedsättning enligt § 17 Upphovsrättslagen</subfield>
<subfield code='u'>http://www.mtm.se/om-oss/uppdrag-och-lagar/upphovsrattslagen/</subfield>
</datafield>
</record>
<xsl:call-template name="holdings"/>
</xsl:if>

<xsl:if test="./@db = 'LIBRIS'">
<xsl:copy-of select="."/>
<xsl:call-template name="delete_holdings"/>
</xsl:if>

</xsl:template>

<xsl:template match="marc:leader">
<xsl:variable name="l0004" select="'     '"/>
<xsl:variable name="l05" select="substring(., 6, 1)"/>
<xsl:variable name="l06"><xsl:choose><xsl:when test="substring(., 7, 1) = 'm' and ../marc:datafield[@tag='500']/marc:subfield[@code='a' and contains(., 'E-textbok')]">a</xsl:when><xsl:otherwise><xsl:value-of select="substring(., 7, 1)"/></xsl:otherwise></xsl:choose></xsl:variable>
<xsl:variable name="l07"><xsl:choose><xsl:when test="substring(., 8, 1) = 'a'">m</xsl:when><xsl:when test="substring(., 8, 1) = 'm' and ../marc:datafield[@tag='500']/marc:subfield[@code='a' and contains(., 'E-textbok')]">m</xsl:when><xsl:otherwise><xsl:value-of select="substring(., 8, 1)"/></xsl:otherwise></xsl:choose></xsl:variable>
<xsl:variable name="l0811" select="substring(., 9, 4)"/>
<xsl:variable name="l1216" select="substring(., 13, 5)"/>
<xsl:variable name="l17" select="substring(., 18, 1)"/>
<xsl:variable name="l18" select="substring(., 19, 1)"/>
<leader><xsl:value-of select="$l0004"/><xsl:value-of select="$l05"/><xsl:value-of select="$l06"/><xsl:value-of select="$l07"/><xsl:value-of select="$l0811"/><xsl:value-of select="$l1216"/><xsl:value-of select="$l17"/><xsl:value-of select="$l18"/> 4500</leader>
</xsl:template>

<xsl:template name="t006">
<xsl:if test="substring(./marc:leader, 7, 1) = 'm' and ./marc:datafield[@tag='500']/marc:subfield[@code='a' and contains(., 'Digital talbok')]">
<controlfield tag="006">a|||||||||||||||||</controlfield>
<controlfield tag="006">i|||||||||||||||||</controlfield>
</xsl:if>
</xsl:template>

<xsl:template match="marc:controlfield[@tag='007']">
<xsl:variable name="c007">
<xsl:choose>
<xsl:when test="substring(., 1,2) = 'co'">
<xsl:value-of select="'co||||||||||||'"/>
</xsl:when>
<xsl:when test="substring(., 1,1) = 'c'">
<xsl:value-of select="'c|||||||||||||'"/>
</xsl:when>
<xsl:when test="substring(., 1,2) = 'fb'">
<xsl:value-of select="'fb||||||||'"/>
</xsl:when>
<xsl:when test="substring(., 1,1) = 'f'">
<xsl:value-of select="'f|||||||||'"/>
</xsl:when>
<xsl:when test="substring(., 1,2) = 'sd'">
<xsl:value-of select="'sd||||||||||||'"/>
</xsl:when>
<xsl:when test="substring(., 1,1) = 's'">
<xsl:value-of select="'s|||||||||||||'"/>
</xsl:when>
<xsl:when test="substring(., 1,2) = 'vd'">
<xsl:value-of select="'vd|||||||'"/>
</xsl:when>
<xsl:when test="substring(., 1,1) = 'v'">
<xsl:value-of select="'v||||||||'"/>
</xsl:when>
<xsl:otherwise>
<xsl:value-of select="."/>
</xsl:otherwise>
</xsl:choose>
</xsl:variable>
<controlfield tag="007"><xsl:value-of select="$c007"/></controlfield>        
</xsl:template>

<xsl:template match="marc:controlfield[@tag='008']">
<xsl:variable name="c0017" select="substring(., 1, 18)"/>
<xsl:variable name="c1834">
<xsl:choose>
<xsl:when test="substring(../marc:leader, 7, 1) = 'a'">
<xsl:value-of select="substring(., 19, 4)"/>
<xsl:choose><xsl:when test="contains('abcd', substring(., 23, 1))">j</xsl:when><xsl:otherwise><xsl:value-of select="substring(., 23, 1)"/></xsl:otherwise></xsl:choose>
<xsl:value-of select="'q'"/>
<xsl:value-of select="substring(., 25, 4)"/>
<xsl:value-of select="'|'"/>
<xsl:value-of select="substring(., 30, 6)"/>
</xsl:when>
<xsl:when test="substring(../marc:leader, 7, 1) = 'g'">
<xsl:value-of select="'||| '"/>
<xsl:choose><xsl:when test="contains('abcd', substring(., 23, 1))">j</xsl:when><xsl:otherwise><xsl:value-of select="substring(., 23, 1)"/></xsl:otherwise></xsl:choose>
<xsl:value-of select="'     |    v|'"/>
</xsl:when>
<xsl:when test="substring(../marc:leader, 7, 1) = 'i'">
<xsl:value-of select="'nnnn'"/>
<xsl:choose><xsl:when test="contains('abcd', substring(., 23, 1))">j</xsl:when><xsl:otherwise><xsl:value-of select="substring(., 23, 1)"/></xsl:otherwise></xsl:choose>
<xsl:value-of select="substring(., 24, 1)"/>
<xsl:value-of select="'||||||'"/>
<xsl:choose><xsl:when test="substring(., 34, 1) = '0'"><xsl:value-of select="' '"/></xsl:when><xsl:when test="substring(., 34, 1) = '1'">f</xsl:when><xsl:otherwise><xsl:value-of select="substring(., 34, 1)"/></xsl:otherwise></xsl:choose>
<xsl:value-of select="'  n '"/>
</xsl:when>
<xsl:when test="substring(../marc:leader, 7, 1) = 'm' and ../marc:datafield[@tag='500']/marc:subfield[@code='a' and contains(., 'E-textbok')]">
<xsl:value-of select="substring(., 19, 4)"/>
<xsl:choose><xsl:when test="contains('abcd', substring(., 23, 1))">j</xsl:when><xsl:otherwise><xsl:value-of select="substring(., 23, 1)"/></xsl:otherwise></xsl:choose>
<xsl:value-of select="'q'"/>
<xsl:value-of select="substring(., 25, 4)"/>
<xsl:value-of select="'|'"/>
<xsl:value-of select="substring(., 30, 6)"/>
</xsl:when>
<xsl:when test="substring(../marc:leader, 7, 1) = 'm' and ../marc:datafield[@tag='500']/marc:subfield[@code='a' and contains(., 'Digital talbok')]">
<xsl:value-of select="'    '"/>
<xsl:choose><xsl:when test="contains('abcd', substring(., 23, 1))">j</xsl:when><xsl:otherwise><xsl:value-of select="substring(., 23, 1)"/></xsl:otherwise></xsl:choose>
<xsl:value-of select="'q  m |      '"/>
</xsl:when>
<xsl:otherwise>
<xsl:value-of select="'                 '"/>
</xsl:otherwise>
</xsl:choose>
</xsl:variable>
<xsl:variable name="c3537">
<xsl:choose><xsl:when test="substring(., 36, 3)= 'tkn'">sgn</xsl:when><xsl:otherwise><xsl:value-of select="substring(., 36, 3)"/></xsl:otherwise></xsl:choose>
</xsl:variable>
<controlfield tag="008"><xsl:value-of select="$c0017"/><xsl:value-of select="$c1834"/><xsl:value-of select="$c3537"/><xsl:value-of select="' c'"/></controlfield>        
</xsl:template>

<!-- delete templates -->
<xsl:template match="marc:controlfield[@tag='001']">
</xsl:template>
<xsl:template match="marc:datafield[@tag='020']">
</xsl:template>
<xsl:template match="marc:datafield[@tag='029']">
</xsl:template>
<xsl:template match="marc:datafield[@tag='249']">
</xsl:template>
<xsl:template match="marc:datafield[@tag='350']">
</xsl:template>
<xsl:template match="marc:datafield[@tag='852']">
</xsl:template>
<xsl:template match="marc:datafield[@tag='856']">
</xsl:template>
<xsl:template match="marc:datafield[@tag &lt; '600' and @tag &gt; '589']">
</xsl:template>
<xsl:template match="marc:datafield[@tag &lt; '976' and @tag &gt; '899']">
</xsl:template>
<xsl:template match="marc:datafield[@tag &lt; '1000' and @tag &gt; '976']">
</xsl:template>
<!-- delete templates end -->

<xsl:template match="marc:datafield[@tag='029' and @ind1='3']">
<datafield ind1="7" ind2=" " tag="024">
<subfield code='a'><xsl:value-of select="./marc:subfield[@code='a']"/></subfield>
<subfield code='2'>MTM medienummer</subfield>
</datafield>
<datafield ind1=" " ind2=" " tag="035">
<subfield code='a'>(MTM)<xsl:value-of select="./marc:subfield[@code='a']"/></subfield>
</datafield>
</xsl:template>

<xsl:template match="marc:datafield[@tag='041']">
<datafield ind1="{@ind1}" ind2="{@ind2}" tag="041">
<xsl:for-each select="./marc:subfield">
<subfield code="{./@code}"><xsl:choose><xsl:when test=".='tkn'">sgn</xsl:when><xsl:otherwise><xsl:value-of select="."/></xsl:otherwise></xsl:choose></subfield>
</xsl:for-each>
</datafield>
</xsl:template>

<xsl:template match="marc:datafield[@tag='084']">
<datafield ind1="{@ind1}" ind2="{@ind2}" tag="084">
<subfield code='a'><xsl:value-of select="./marc:subfield[@code='a']"/></subfield>
<subfield code='2'><xsl:value-of select="'kssb/8'"/></subfield>
</datafield>
</xsl:template>

<xsl:template match="marc:datafield[@tag='100']">
<xsl:variable name="i1"><xsl:choose><xsl:when test="contains(./marc:subfield[@code='a'], ', ')">1</xsl:when><xsl:otherwise>0</xsl:otherwise></xsl:choose></xsl:variable>
<datafield ind1="{$i1}" ind2=" " tag="100">
<subfield code='a'><xsl:value-of select="./marc:subfield[@code='a']"/></subfield>
<xsl:if test="./marc:subfield[@code='b']">
<subfield code='b'><xsl:value-of select="./marc:subfield[@code='b']"/><xsl:if test="./marc:subfield[@code='c']">,</xsl:if></subfield>
</xsl:if>
<xsl:if test="./marc:subfield[@code='c']">
<subfield code='c'><xsl:value-of select="./marc:subfield[@code='c']"/><xsl:if test="./marc:subfield[@code='d']">,</xsl:if></subfield>
</xsl:if>
<xsl:if test="./marc:subfield[@code='d']">
<subfield code='d'><xsl:value-of select="./marc:subfield[@code='d']"/></subfield>
</xsl:if>
</datafield>
</xsl:template>

<xsl:template match="marc:datafield[@tag='110']">
<datafield ind1="{@ind1}" ind2="{@ind2}" tag="110">
<xsl:for-each select="./marc:subfield[@code != 'w']">
<subfield code='{@code}'><xsl:value-of select="."/></subfield>
</xsl:for-each>
</datafield>
</xsl:template>

<xsl:template match="marc:datafield[@tag='111']">
<datafield ind1="{@ind1}" ind2="{@ind2}" tag="111">
<xsl:for-each select="./marc:subfield[@code != 'w']">
<subfield code='{@code}'><xsl:value-of select="."/></subfield>
</xsl:for-each>
</datafield>
</xsl:template>

<xsl:template match="marc:datafield[@tag='130']">
<datafield ind1="{@ind1}" ind2="{@ind2}" tag="130">
<subfield code='a'><xsl:value-of select="./marc:subfield[@code='a']"/><xsl:if test="./marc:subfield[@code='f']">.</xsl:if></subfield>
<xsl:if test="./marc:subfield[@code='f']">
<subfield code='f'><xsl:value-of select="./marc:subfield[@code='f']"/><xsl:if test="./marc:subfield[@code='l']">.</xsl:if></subfield>
</xsl:if>
<xsl:if test="./marc:subfield[@code='l']">
<subfield code='l'><xsl:value-of select="./marc:subfield[@code='l']"/><xsl:if test="./marc:subfield[@code='p']">.</xsl:if></subfield>
</xsl:if>
<xsl:if test="./marc:subfield[@code='p']">
<subfield code='p'><xsl:value-of select="./marc:subfield[@code='p']"/></subfield>
</xsl:if>
</datafield>
</xsl:template>

<xsl:template match="marc:datafield[@tag='240']">
<datafield ind1="1" ind2="0" tag="240">
<xsl:if test="./marc:subfield[@code='a']">
<subfield code='a'><xsl:value-of select="./marc:subfield[@code='a']"/><xsl:if test="./marc:subfield[@code='f']">.</xsl:if></subfield>
</xsl:if>
<xsl:if test="./marc:subfield[@code='f']">
<subfield code='f'><xsl:value-of select="./marc:subfield[@code='f']"/><xsl:if test="./marc:subfield[@code='l']">.</xsl:if></subfield>
</xsl:if>
<xsl:if test="./marc:subfield[@code='l']">
<subfield code='l'><xsl:value-of select="./marc:subfield[@code='l']"/><xsl:if test="./marc:subfield[@code='p']">.</xsl:if></subfield>
</xsl:if>
<xsl:if test="./marc:subfield[@code='p']">
<subfield code='p'><xsl:value-of select="./marc:subfield[@code='p']"/></subfield>
</xsl:if>
<xsl:for-each select="./marc:subfield[not(contains('aflpw', @code))]">
<subfield code='{@code}'><xsl:value-of select="."/></subfield>
</xsl:for-each>
</datafield>
</xsl:template>

<xsl:template match="marc:datafield[@tag='245']">
<datafield ind1="1" ind2="{@ind2}" tag="245">
<subfield code='a'><xsl:value-of select="./marc:subfield[@code='a']"/></subfield>
<xsl:if test="./marc:subfield[@code='h']">
<subfield code='h'><xsl:choose><xsl:when test="contains('[]', ./marc:subfield[@code='h'])"><xsl:value-of select="./marc:subfield[@code='h']"/></xsl:when><xsl:otherwise>[<xsl:value-of select="./marc:subfield[@code='h']"/>]</xsl:otherwise></xsl:choose><xsl:if test="./marc:subfield[@code='b' or @code='c']"> :</xsl:if></subfield>
</xsl:if>
<xsl:if test="./marc:subfield[@code='b']">
<subfield code='b'><xsl:value-of select="./marc:subfield[@code='b']"/><xsl:if test="./marc:subfield[@code='c']"> /</xsl:if></subfield>
</xsl:if>
<xsl:if test="./marc:subfield[@code='c']">
<subfield code='c'><xsl:value-of select="./marc:subfield[@code='c']"/></subfield>
</xsl:if>
</datafield>
</xsl:template>

<xsl:template match="marc:datafield[@tag='246']">
<datafield ind1="1" ind2=" " tag="246">
<subfield code='a'><xsl:value-of select="./marc:subfield[@code='a']"/></subfield>
</datafield>
</xsl:template>

<xsl:template name="t249">
<xsl:for-each select="marc:datafield[@tag='249']">
<xsl:choose>
<xsl:when test="position() = '1' and not(../marc:datafield[@tag='240'] or ../marc:datafield[@tag='130'])">
<xsl:variable name="tag249">
<xsl:choose>
<xsl:when test="../marc:datafield[@tag &gt; '99' and @tag &lt; '200']">
<xsl:value-of select="'240'"/>
</xsl:when>
<xsl:otherwise>
<xsl:value-of select="'130'"/>
</xsl:otherwise>
</xsl:choose>
</xsl:variable>
<xsl:variable name="i1">
<xsl:choose><xsl:when test="$tag249 = '240'">1</xsl:when><xsl:otherwise>0</xsl:otherwise></xsl:choose>
</xsl:variable>
<xsl:variable name="i2">
<xsl:choose><xsl:when test="$tag249 = '240'">0</xsl:when><xsl:otherwise> </xsl:otherwise></xsl:choose>
</xsl:variable>
<datafield ind1="{$i1}" ind2="{$i2}" tag="{$tag249}">
<subfield code='a'><xsl:value-of select="./marc:subfield[@code='a']"/></subfield>
</datafield>
</xsl:when>
<xsl:otherwise>
<datafield ind1=" " ind2="0" tag="249">
<subfield code='a'><xsl:value-of select="./marc:subfield[@code='a']"/></subfield>
</datafield>
</xsl:otherwise>
</xsl:choose>
</xsl:for-each>
</xsl:template>

<!-- The following should be copied by identity (unchanged) -->
<!-- 046 (generated in import) -->
<!-- 250 -->
<!-- 256 -->
<!-- 520 -->
<!-- 538 -->
<!-- 546 -->
<!-- 653 -->

<xsl:template match="marc:datafield[@tag='260']">
<datafield ind1="{@ind1}" ind2="{@ind2}" tag="260">
<subfield code='a'><xsl:value-of select="./marc:subfield[@code='a']"/><xsl:if test="./marc:subfield[@code='b']"> :</xsl:if></subfield>
<xsl:if test="./marc:subfield[@code='b']">
<subfield code='b'><xsl:value-of select="./marc:subfield[@code='b']"/><xsl:if test="./marc:subfield[@code='c']">,</xsl:if></subfield>
</xsl:if>
<xsl:if test="./marc:subfield[@code='c']">
<subfield code='c'><xsl:value-of select="./marc:subfield[@code='c']"/></subfield>
</xsl:if>
</datafield>
</xsl:template>

<xsl:template match="marc:datafield[@tag='295']">
<datafield ind1="1" ind2="{@ind2}" tag="245">
<xsl:if test="./marc:subfield[@code='a']">
<subfield code='a'><xsl:value-of select="./marc:subfield[@code='a']"/></subfield>
</xsl:if>
<xsl:if test="./marc:subfield[@code='h']">
<subfield code='h'><xsl:choose><xsl:when test="contains('[]', ./marc:subfield[@code='h'])"><xsl:value-of select="./marc:subfield[@code='h']"/></xsl:when><xsl:otherwise>[<xsl:value-of select="./marc:subfield[@code='h']"/>]</xsl:otherwise></xsl:choose><xsl:if test="./marc:subfield[@code='b' or @code='c']"> :</xsl:if></subfield>
</xsl:if>
<xsl:if test="./marc:subfield[@code='b']">
<subfield code='b'><xsl:value-of select="./marc:subfield[@code='b']"/><xsl:if test="./marc:subfield[@code='c']"> /</xsl:if></subfield>
</xsl:if>
<xsl:if test="./marc:subfield[@code='c']">
<subfield code='c'><xsl:value-of select="./marc:subfield[@code='c']"/><xsl:if test="./marc:subfield[@code='n']">.</xsl:if></subfield>
</xsl:if>
<xsl:if test="./marc:subfield[@code='n']">
<subfield code='n'><xsl:value-of select="./marc:subfield[@code='n']"/><xsl:if test="./marc:subfield[@code='p']"> ,</xsl:if></subfield>
</xsl:if>
<xsl:if test="./marc:subfield[@code='p']">
<subfield code='p'><xsl:value-of select="./marc:subfield[@code='p']"/></subfield>
</xsl:if>
</datafield>
</xsl:template>

<xsl:template match="marc:datafield[@tag='300']">
<datafield ind1="{@ind1}" ind2="{@ind2}" tag="300">
<subfield code='a'><xsl:value-of select="./marc:subfield[@code='a']"/><xsl:if test="./marc:subfield[@code='b']"> :</xsl:if></subfield>
<xsl:if test="./marc:subfield[@code='b']">
<subfield code='b'><xsl:value-of select="./marc:subfield[@code='b']"/><xsl:if test="./marc:subfield[@code='c']"> ;</xsl:if></subfield>
</xsl:if>
<xsl:if test="./marc:subfield[@code='c']">
<subfield code='c'><xsl:value-of select="./marc:subfield[@code='c']"/></subfield>
</xsl:if>
</datafield>
</xsl:template>

<xsl:template match="marc:datafield[@tag='440']">
<datafield ind1="0" ind2=" " tag="490">
<subfield code='a'><xsl:value-of select="./marc:subfield[@code='a']"/></subfield>
</datafield>
</xsl:template>

<xsl:template match="marc:datafield[@tag='500']">
<datafield ind1="{@ind1}" ind2="{@ind2}" tag="500">
<xsl:apply-templates select="./marc:subfield"/>
</datafield>
<xsl:choose>
<xsl:when test="contains(./marc:subfield[@code='a'], 'Inläst ur')">
<datafield ind1="0" ind2="8" tag="776">
<subfield code='i'><xsl:value-of select="'Inläst ur'"/></subfield>
<subfield code='t'><xsl:value-of select="../marc:datafield[@tag='245']/marc:subfield[@code='a'][1]"/></subfield>
<xsl:if test="../marc:datafield[@tag='020']/marc:subfield[@code='a']">
<subfield code='z'><xsl:value-of select="translate(../marc:datafield[@tag='020']/marc:subfield[@code='a'][1], '-', '')"/></subfield>
</xsl:if>
</datafield>
</xsl:when>
<xsl:when test="contains(./marc:subfield[@code='a'], 'Kopierad från')">
<datafield ind1="0" ind2="8" tag="776">
<subfield code='i'><xsl:value-of select="'Kopierad från'"/></subfield>
<subfield code='t'><xsl:value-of select="../marc:datafield[@tag='245']/marc:subfield[@code='a'][1]"/></subfield>
<xsl:if test="../marc:datafield[@tag='020']/marc:subfield[@code='a']">
<subfield code='z'><xsl:value-of select="translate(../marc:datafield[@tag='020']/marc:subfield[@code='a'][1], '-', '')"/></subfield>
</xsl:if>
</datafield>
</xsl:when>
<xsl:when test="contains(./marc:subfield[@code='a'], 'Elektronisk version av')">
<datafield ind1="0" ind2="8" tag="776">
<subfield code='i'><xsl:value-of select="'Elektronisk version av'"/></subfield>
<subfield code='t'><xsl:value-of select="../marc:datafield[@tag='245']/marc:subfield[@code='a'][1]"/></subfield>
<xsl:if test="../marc:datafield[@tag='020']/marc:subfield[@code='a']">
<subfield code='z'><xsl:value-of select="translate(../marc:datafield[@tag='020']/marc:subfield[@code='a'][1],'-','')"/></subfield>
</xsl:if>
</datafield>
</xsl:when>
</xsl:choose>
</xsl:template>

<xsl:template match="marc:datafield[@tag='505']">
<datafield ind1="8" ind2=" " tag="505">
<subfield code='a'><xsl:value-of select="./marc:subfield[@code='a']"/></subfield>
</datafield>
</xsl:template>

<xsl:template match="marc:datafield[@tag='508']">
<datafield ind1="0" ind2=" " tag="511">
<subfield code='a'><xsl:value-of select="./marc:subfield[@code='a']"/></subfield>
</datafield>
</xsl:template>


<xsl:template match="marc:datafield[@tag='600']">
<xsl:variable name="i1">
<xsl:choose>
<xsl:when test="contains(./marc:subfield[@code='a'], ', ')">
<xsl:value-of select="1"/>
</xsl:when>
<xsl:otherwise>
<xsl:value-of select="0"/>
</xsl:otherwise>
</xsl:choose>
</xsl:variable>
<datafield ind1="{$i1}" ind2="4" tag="600">
<xsl:apply-templates select="./marc:subfield"/>
</datafield>
</xsl:template>

<xsl:template match="marc:datafield[@tag='610']">
<datafield ind1="2" ind2="4" tag="610">
<xsl:apply-templates select="./marc:subfield[@code != 'w']"/>
</datafield>
</xsl:template>

<xsl:template match="marc:datafield[@tag='611']">
<datafield ind1="2" ind2="4" tag="611">
<xsl:apply-templates select="./marc:subfield[@code != 'w']"/>
</datafield>
</xsl:template>

<xsl:template match="marc:datafield[@tag='630']">
<datafield ind1="0" ind2="4" tag="630">
<xsl:if test="./marc:subfield[@code='a']">
<subfield code='a'><xsl:value-of select="./marc:subfield[@code='a']"/><xsl:if test="./marc:subfield[@code='l']">.</xsl:if></subfield>
</xsl:if>
<xsl:if test="./marc:subfield[@code='l']">
<subfield code='l'><xsl:value-of select="./marc:subfield[@code='l']"/><xsl:if test="./marc:subfield[@code='p']">.</xsl:if></subfield>
</xsl:if>
<xsl:if test="./marc:subfield[@code='p']">
<subfield code='p'><xsl:value-of select="./marc:subfield[@code='p']"/></subfield>
</xsl:if>
</datafield>
</xsl:template>

<xsl:template match="marc:datafield[@tag='650']">
<datafield ind1=" " ind2="7" tag="650">
<xsl:apply-templates select="./marc:subfield"/>
</datafield>
</xsl:template>

<xsl:template match="marc:datafield[@tag='651']">
<datafield ind1=" " ind2="7" tag="651">
<xsl:apply-templates select="./marc:subfield"/>
</datafield>
</xsl:template>

<xsl:template match="marc:datafield[@tag='655']">
<datafield ind1=" " ind2="7" tag="655">
<xsl:apply-templates select="./marc:subfield"/>
</datafield>
</xsl:template>

<xsl:template match="marc:datafield[@tag='700']">
<xsl:variable name="i1">
<xsl:choose>
<xsl:when test="contains(./marc:subfield[@code='a'], ', ')">
<xsl:value-of select="'1'"/>
</xsl:when>
<xsl:otherwise>
<xsl:value-of select="'0'"/>
</xsl:otherwise>
</xsl:choose>
</xsl:variable>
<datafield ind1="{$i1}" ind2=" " tag="700">
<subfield code='a'><xsl:value-of select="./marc:subfield[@code='a']"/><xsl:if test="./marc:subfield[@code='d']">,</xsl:if></subfield>
<xsl:if test="./marc:subfield[@code='d']">
<subfield code='d'><xsl:value-of select="./marc:subfield[@code='d']"/><xsl:if test="./marc:subfield[@code='t']">.</xsl:if></subfield>
</xsl:if>
<xsl:if test="./marc:subfield[@code='t']">
<subfield code='t'><xsl:value-of select="./marc:subfield[@code='t']"/></subfield>
</xsl:if>
<xsl:choose>
<xsl:when test="@ind1 = '9'">
<subfield code='4'>nrt</subfield>
</xsl:when>
<xsl:otherwise>
<xsl:if test="./marc:subfield[@code='4']">
<subfield code='4'><xsl:value-of select="./marc:subfield[@code='4']"/></subfield>
</xsl:if>
</xsl:otherwise>
</xsl:choose>
</datafield>
</xsl:template>

<xsl:template match="marc:datafield[@tag='710']">
<datafield ind1="2" ind2=" " tag="710">
<xsl:apply-templates select="./marc:subfield[@code != 'w']"/>
</datafield>
</xsl:template>

<xsl:template match="marc:datafield[@tag='711']">
<datafield ind1="2" ind2=" " tag="711">
<xsl:apply-templates select="./marc:subfield[@code != 'w']"/>
</datafield>
</xsl:template>

<xsl:template match="marc:datafield[@tag='730']">
<datafield ind1="{@ind1}" ind2="{@ind2}" tag="730">
<xsl:if test="./marc:subfield[@code='a']">
<subfield code='a'><xsl:value-of select="./marc:subfield[@code='a']"/><xsl:if test="./marc:subfield[@code='l']">.</xsl:if></subfield>
</xsl:if>
<xsl:if test="./marc:subfield[@code='l']">
<subfield code='l'><xsl:value-of select="./marc:subfield[@code='l']"/><xsl:if test="./marc:subfield[@code='p']">.</xsl:if></subfield>
</xsl:if>
<xsl:if test="./marc:subfield[@code='p']">
<subfield code='p'><xsl:value-of select="./marc:subfield[@code='p']"/></subfield>
</xsl:if>
</datafield>
</xsl:template>

<xsl:template match="marc:datafield[@tag='740']">
<datafield ind1="{@ind1}" ind2="{@ind2}" tag="740">
<subfield code='a'><xsl:value-of select="./marc:subfield[@code='a']"/></subfield>
</datafield>
</xsl:template>

<xsl:template match="marc:datafield[@tag='856' and ./marc:subfield[@code='s'] and (count(./marc:subfield) = '1')]">
<datafield ind1="{@ind1}" ind2="{@ind2}" tag="856">
<subfield code='s'><xsl:value-of select="./marc:subfield[@code='s']"/></subfield>
</datafield>
</xsl:template>

<!--
<xsl:template match="marc:datafield[@tag='856']">
<datafield ind1="4" ind2="2" tag="856">
<subfield code='u'>http://www.legimus.se/work/redirect?MedieNr=<xsl:value-of select="./marc:datafield[@tag='029' and @ind1='3']/marc:subfield[@code='a']"/></subfield>
<subfield code='z'>Ladda ner. Tillgänglig för personer med läsnedsättning enligt § 17 Upphovsrättslagen</subfield>
</datafield>
</xsl:template>
-->

<xsl:template match="marc:datafield[@tag='887']">
<xsl:choose>
<xsl:when test="@ind1 = '0'">
<datafield ind1="0" ind2=" " tag="886">
<xsl:apply-templates select="./marc:subfield"/>
</datafield>
<xsl:choose>
<xsl:when test="substring(./marc:subfield[@code='b'], 2, 2) = 'GL'">
<datafield ind1=" " ind2="4" tag="655">
<subfield code='a'>Litteratur på teckenspråk</subfield>
</datafield>
</xsl:when>
<xsl:when test="substring(./marc:subfield[@code='b'], 2, 2) = 'IC'">
<datafield ind1=" " ind2="4" tag="655">
<subfield code='a'>DAISY</subfield>
</datafield>
</xsl:when>
<xsl:when test="substring(./marc:subfield[@code='b'], 2, 2) = 'LC'">
<xsl:choose>
<xsl:when test="contains(../marc:datafield[@tag='500']/marc:subfield[@code='a'], 'Digital talbok')">
<datafield ind1=" " ind2="4" tag="655">
<subfield code='a'>DAISY text och ljud</subfield>
</datafield>
</xsl:when>
<xsl:when test="contains(../marc:datafield[@tag='500']/marc:subfield[@code='a'], 'E-textbok')">
<datafield ind1=" " ind2="4" tag="655">
<subfield code='a'>E-textbok</subfield>
</datafield>
</xsl:when>
</xsl:choose>
</xsl:when>
</xsl:choose>
</xsl:when>
<xsl:otherwise>
<datafield ind1=" " ind2=" " tag="887">
<xsl:apply-templates select="./marc:subfield"/>
</datafield>
</xsl:otherwise>
</xsl:choose>
</xsl:template>

<xsl:template match="marc:datafield[@tag='976']">
<datafield ind1=" " ind2="0" tag="976">
<subfield code='b'><xsl:value-of select="./marc:subfield[@code='b']"/></subfield>
</datafield>
</xsl:template>

<xsl:template name="holdings">
<record type="Holdings">
<leader>*****<xsl:value-of select="substring(./marc:leader, 6, 1)"/>x  a22*****1n 4500</leader>
<controlfield tag="008"><xsl:value-of select="substring(@date, 3, 6)"/>||0000|||||000||||||000000</controlfield>
<controlfield tag="001"><xsl:value-of select="./marc:datafield[@tag='029' and @ind1='3']/marc:subfield[@code='a']"/></controlfield>
<datafield ind1=" " ind2=" " tag="035">
<subfield code='a'>(MTM)<xsl:value-of select="./marc:datafield[@tag='029' and @ind1='3']/marc:subfield[@code='a']"/></subfield>
</datafield>
<datafield ind1=" " ind2=" " tag="599">
<subfield code='a'>Posten ändrad i Legimus <xsl:value-of select="@date"/></subfield>
</datafield>
<datafield ind1=" " ind2=" " tag="852">
<subfield code='b'>Mtm</subfield>
<subfield code='h'><xsl:value-of select="./marc:datafield[@tag='852']/marc:subfield[@code='h']"/></subfield>
</datafield>
<xsl:for-each select="./marc:datafield[@tag='856' and not(./marc:subfield[@code='s'] and (count(./marc:subfield) = '1'))]">
<datafield ind1="4" ind2="2" tag="856">
<xsl:for-each select="./marc:subfield">
<subfield code="{@code}"><xsl:value-of select="."/></subfield>
</xsl:for-each>
</datafield>
</xsl:for-each>
<datafield ind1="4" ind2="0" tag="856">
<subfield code='u'>http://www.legimus.se/work/redirect?MedieNr=<xsl:value-of select="./marc:datafield[@tag='029' and @ind1='3']/marc:subfield[@code='a']"/></subfield>
<subfield code='z'>Ladda ner. Tillgänglig för personer med läsnedsättning enligt § 17 Upphovsrättslagen</subfield>
</datafield>
</record>
</xsl:template>

<xsl:template name="delete_holdings">
<record type="Holdings">
<leader>*****dx  a22*****1n 4500</leader>
<controlfield tag="008"><xsl:value-of select="substring(@date, 3, 6)"/>||0000|||||000||||||000000</controlfield>
<datafield ind1=" " ind2=" " tag="852">
<subfield code='b'>Mtm</subfield>
</datafield>
</record>
</xsl:template>

<!-- identity without xmlns -->
<xsl:template match="node()">
<xsl:element name="{local-name(.)}">
<xsl:for-each select="@*"><xsl:copy/></xsl:for-each>
<xsl:apply-templates/>
</xsl:element>
</xsl:template>

<xsl:template match="@*|text()|processing-instruction()">
<xsl:copy/>
</xsl:template>

</xsl:stylesheet>
