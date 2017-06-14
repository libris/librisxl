<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:marc="http://www.loc.gov/MARC21/slim" xmlns:java="http://xml.apache.org/xslt/java" version="1.0" exclude-result-prefixes="marc java">
    <xsl:output method="xml" indent="yes"/>
    <!--<xsl:strip-space elements="marc:datafield[number(@tag) > 759 and number(@tag) &lt; 788]"/>-->
    <!--<xsl:strip-space elements="*"/>-->
    <xsl:template match="marc:collection">
        <!--<xsl:apply-templates/>-->
        <collection xmlns="http://www.loc.gov/MARC21/slim">
            <xsl:apply-templates select="marc:record"/>
        </collection>       
    </xsl:template>
    
    <!-- record -->
    <xsl:template match="marc:record">
        <!-- bibliographic record -->
        <record xmlns="http://www.loc.gov/MARC21/slim" type="Bibliographic">
            <xsl:apply-templates/>
            <xsl:call-template name="create042"/>
            <xsl:call-template name="create506"/>
        </record>
        <!-- holdings record -->
        <record xmlns="http://www.loc.gov/MARC21/slim" type="Holdings">
            <xsl:variable name="rawTimeStamp"><xsl:value-of select="java:format(java:java.text.SimpleDateFormat.new('yyyy-MM-dd'), java:java.util.Date.new())"/></xsl:variable>
            <xsl:variable name="timeStamp" select="substring(translate($rawTimeStamp, '-', ''), 3)"/>
            <xsl:variable name="tmp" select="substring(marc:leader, 6, 1)"/>
            <xsl:variable name="leader5">
                <xsl:choose>
                    <xsl:when test="$tmp = 'n' or $tmp = 'c' or $tmp = 'd'"><xsl:value-of select="$tmp"/></xsl:when>
                    <xsl:otherwise>n</xsl:otherwise>
                </xsl:choose>
            </xsl:variable>
            
            <leader xmlns="http://www.loc.gov/MARC21/slim">*****<xsl:value-of select="$leader5"/><xsl:text>x  a22*****1n 4500</xsl:text></leader>
            <controlfield xmlns="http://www.loc.gov/MARC21/slim" tag="008"><xsl:value-of select="$timeStamp"/>||0000|||||000||||||000000</controlfield>
            <xsl:apply-templates select="marc:controlfield[@tag='001']" mode="holdings_only"/>
            <xsl:apply-templates select="marc:datafield[@tag='024' and @ind1='7' and marc:subfield[@code='a'] and marc:subfield[@code='2' and normalize-space(.) = 'TPB']]" mode="holdings_only"/>
            <xsl:apply-templates select="marc:datafield[@tag='852' and marc:subfield[@code='h']]" mode="holdings_only"/>            
            <xsl:variable name="tpbid" select="marc:datafield[@tag='024' and marc:subfield[@code='2' and normalize-space(.) = 'TPB']]/marc:subfield[@code='a']"/>
            
            <xsl:apply-templates select="marc:datafield[@tag='886' and @ind1='0' and marc:subfield[@code='b' and (substring(normalize-space(.),2,2) = 'GL' or substring(normalize-space(.),2,2) = 'IC' or substring(normalize-space(.),2,2) = 'LC')]]" mode="holdings_only">
                <xsl:with-param name="ind2" select="'0'"/>
                <xsl:with-param name="comment" select="'Ladda ner.'"/>
                <xsl:with-param name="tpbid" select="$tpbid"/>
            </xsl:apply-templates>
            <!-- If 886 not exists, call template build_856 to create 856. If 886 exists the identity template will do the work -->
            <xsl:if test="not(marc:datafield[@tag='886' and @ind1='0' and marc:subfield[@code='b' and (substring(normalize-space(.),2,2) = 'GL' or substring(normalize-space(.),2,2) = 'IC' or substring(normalize-space(.),2,2) = 'LC')]])">
                <xsl:call-template name="build_856">
                    <xsl:with-param name="ind2" select="'2'"/>
                    <xsl:with-param name="comment" select="'TPB-katalogen.'"/>
                    <xsl:with-param name="tpbid" select="$tpbid"/>
                </xsl:call-template>
            </xsl:if>
            </record>
    </xsl:template>
    
    <!-- leader -->
    <xsl:template match="marc:leader">
        <xsl:variable name="tmp" select="substring(., 6, 1)"/>
        <xsl:variable name="leader5">
            <xsl:choose>
                <xsl:when test="$tmp = 'd'">c</xsl:when>
                <xsl:otherwise><xsl:value-of select="$tmp"/></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <leader xmlns="http://www.loc.gov/MARC21/slim">#####<xsl:value-of select="$leader5"/><xsl:value-of select="substring(., 7, 6)"/>#####<xsl:value-of select="substring(., 18)"/></leader>
    </xsl:template>
 
    <xsl:template match="marc:controlfield[@tag='001']">
     <datafield ind1=" " ind2=" " tag="035" xmlns="http://www.loc.gov/MARC21/slim">
         <subfield code="a"><xsl:if test="../marc:controlfield[@tag='003']">(<xsl:value-of select="../marc:controlfield[@tag='003']"/>)</xsl:if><xsl:value-of select="."/></subfield>
     </datafield>
     <xsl:copy-of select="."/>
 </xsl:template>
    
    <xsl:template match="marc:controlfield[@tag='001']" mode="holdings_only">
        <datafield ind1=" " ind2=" " tag="035" xmlns="http://www.loc.gov/MARC21/slim">
            <subfield code="a"><xsl:if test="../marc:controlfield[@tag='003']">(<xsl:value-of select="../marc:controlfield[@tag='003']"/>)</xsl:if><xsl:value-of select="."/></subfield>
        </datafield>
    </xsl:template>
    
    <xsl:template match="marc:controlfield[@tag='005']">
        <datafield ind1=" " ind2=" " tag="599" xmlns="http://www.loc.gov/MARC21/slim">
            <subfield code="a"><xsl:text>Posten ändrad i BURK </xsl:text><xsl:value-of select="."/></subfield>
        </datafield>
    </xsl:template>
    
    <xsl:template match="marc:controlfield[@tag='008']">
        <xsl:variable name="leader6" select="substring(../marc:leader, 7, 1)"/>
        <xsl:variable name="tmp">
            <xsl:choose>
                <xsl:when test="../marc:controlfield[@tag='007' and substring(., 1, 2) = 'cr']">o</xsl:when>
                <xsl:when test="../marc:controlfield[@tag='007' and substring(., 1, 1) = 'c']">q</xsl:when>
            </xsl:choose>
        </xsl:variable>
        <xsl:variable name="tmp2">
            <xsl:if test="$leader6 = 'e' or $leader6 = 'f' or $leader6 = 'g' or $leader6 = 'k' or $leader6 = 'o' or $leader6 = 'r'">True</xsl:if>
        </xsl:variable>
        <xsl:variable name="cf008_0-22" select="substring(., 1, 23)"/>
        <xsl:variable name="cf008_23">
            <xsl:choose>
                <xsl:when test="$tmp2 = 'True'"><xsl:value-of select="substring(., 24, 1)"/></xsl:when>
                <xsl:when test="$tmp != ''"><xsl:value-of select="$tmp"/></xsl:when>
                <xsl:otherwise><xsl:value-of select="substring(., 24, 1)"/></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <xsl:variable name="cf008_24-28" select="substring(., 25, 5)"/>
        <xsl:variable name="cf008_29">
            <xsl:choose>
                <xsl:when test="$tmp2 != 'True'"><xsl:value-of select="substring(., 30, 1)"/></xsl:when>
                <xsl:when test="$tmp != ''"><xsl:value-of select="$tmp"/></xsl:when>
                <xsl:otherwise><xsl:value-of select="substring(., 30, 1)"/></xsl:otherwise>
            </xsl:choose>
         </xsl:variable>
        <xsl:variable name="cf008_30-39" select="substring(., 31, 10)"/>
        <controlfield tag="008" xmlns="http://www.loc.gov/MARC21/slim"><xsl:value-of select="$cf008_0-22"/><xsl:value-of select="$cf008_23"/><xsl:value-of select="$cf008_24-28"/><xsl:value-of select="$cf008_29"/><xsl:value-of select="$cf008_30-39"/></controlfield>
    </xsl:template>
    
    <xsl:template match="marc:datafield[@tag='020']/marc:subfield[@code='a']">
        <subfield code="{@code}" xmlns="http://www.loc.gov/MARC21/slim"><xsl:value-of select="translate(normalize-space(.), '-', '')"/></subfield>
    </xsl:template>
    
    <xsl:template match="marc:datafield[@tag='024' and @ind1='7' and marc:subfield[@code='a'] and marc:subfield[@code='2' and normalize-space(.) = 'TPB']]">
        <datafield xmlns="http://www.loc.gov/MARC21/slim" ind1=" " ind2=" " tag="035">
            <subfield code='a'>(TPB)<xsl:value-of select="marc:subfield[@code='a']"/></subfield>
        </datafield>
        <xsl:copy-of select="."/>   
    </xsl:template>
    
    <xsl:template match="marc:datafield[@tag='024' and @ind1='7' and marc:subfield[@code='a'] and marc:subfield[@code='2' and normalize-space(.) = 'TPB']]" mode="holdings_only">
        <datafield xmlns="http://www.loc.gov/MARC21/slim" ind1=" " ind2=" " tag="035">
            <subfield code='a'>(TPB)<xsl:value-of select="marc:subfield[@code='a']"/></subfield>
        </datafield>
    </xsl:template>
    
    <xsl:template match="marc:datafield[@tag='024' and @ind1='8' and marc:subfield[@code='a' and starts-with(normalize-space(.), 'BTJ, häfte')]]"/>
    
    <xsl:template match="marc:datafield[@tag='084']/marc:subfield[@code='2' and normalize-space(.) = 'kssb']">
        <subfield code="{@code}" xmlns="http://www.loc.gov/MARC21/slim"><xsl:value-of select="."/><xsl:text>/8</xsl:text></subfield>
    </xsl:template>
    
    <xsl:template match="marc:datafield[@tag='500' and marc:subfield[@code='a' and (normalize-space(substring-after(., 'ISBN')) != '') and starts-with(normalize-space(.), 'Inläst ur') or starts-with(normalize-space(.), 'Kopierad från') or starts-with(normalize-space(.), 'Elektronisk version av')]]">
        <datafield xmlns="http://www.loc.gov/MARC21/slim" ind1="0" ind2="8" tag="776">
            <xsl:for-each select="marc:subfield[@code='a']">
                <xsl:variable name="label" select="substring-before(., ':')"/>
                <xsl:variable name="tmp1" select="normalize-space(substring-after(., 'ISBN'))"/>
                <xsl:variable name="isbn_raw" select="normalize-space(substring-before($tmp1, ','))"/>
                <xsl:variable name="isbn_dehyphenated" select="translate($isbn_raw, '-', '')"/>
                <xsl:variable name="title" select="../../marc:datafield[@tag='245']/marc:subfield[@code='a']"/>
                <subfield code="i" xmlns="http://www.loc.gov/MARC21/slim"><xsl:value-of select="$label"/></subfield>
                <subfield code="t" xmlns="http://www.loc.gov/MARC21/slim"><xsl:value-of select="$title"/></subfield>
                <subfield code="z" xmlns="http://www.loc.gov/MARC21/slim"><xsl:value-of select="$isbn_dehyphenated"/></subfield>
            </xsl:for-each>
        </datafield>
        <xsl:copy-of select="."/>   
    </xsl:template>
    
    <xsl:template match="marc:datafield[@tag='508']">
        <datafield xmlns="http://www.loc.gov/MARC21/slim" ind1="0" ind2=" " tag="511">
            <xsl:apply-templates/>
        </datafield>
    </xsl:template>
    
    <xsl:template match="marc:datafield[number(@tag) > 589 and number(@tag) &lt; 600]"/>
    
    <xsl:template match="marc:datafield[number(@tag) > 759 and number(@tag) &lt; 788]/marc:subfield[@code='w']"/>
    
    <xsl:template match="marc:datafield[@tag='852']"/>
    
    <xsl:template match="marc:datafield[@tag='852']" mode="holdings_only">
        <datafield xmlns="http://www.loc.gov/MARC21/slim" ind1=" " ind2=" " tag="852">
            <subfield code='b'>Mtm</subfield>
            <xsl:apply-templates select="marc:subfield[@code='h']"/>
       </datafield>
    </xsl:template>
    
    <xsl:template match="marc:datafield[@tag='886' and @ind1='0']">
        <xsl:variable name="has500dig" select="count(../marc:datafield[@tag='500' and marc:subfield[@code='a' and starts-with(normalize-space(.), 'Digital talbok')]])"/>
        <xsl:variable name="has500txt" select="count(../marc:datafield[@tag='500' and marc:subfield[@code='a' and starts-with(normalize-space(.), 'E-textbok')]])"/>
        <xsl:variable name="data">
            <xsl:if test="substring(normalize-space(marc:subfield[@code='b']), 2, 2) = 'AB'">Punktskrift</xsl:if>
            <xsl:if test="substring(normalize-space(marc:subfield[@code='b']), 2, 2) = 'GL'">Litteratur på teckenspråk</xsl:if>
            <xsl:if test="substring(normalize-space(marc:subfield[@code='b']), 2, 2) = 'IC'">DAISY</xsl:if>
            <xsl:if test="substring(normalize-space(marc:subfield[@code='b']), 2, 2) = 'LC' and $has500dig > 0">DAISY text och ljud</xsl:if>
            <xsl:if test="substring(normalize-space(marc:subfield[@code='b']), 2, 2) = 'LC' and $has500txt > 0">E-textbok</xsl:if>
        </xsl:variable>
        <xsl:if test="starts-with($data, 'DAISY')">
            <datafield xmlns="http://www.loc.gov/MARC21/slim" ind1=" " ind2="7" tag="655">
                <subfield code="a">Talböcker</subfield>
                <subfield code="2">saogf</subfield>
            </datafield>
        </xsl:if>
        <xsl:if test="$data != ''">
        <datafield xmlns="http://www.loc.gov/MARC21/slim" ind1=" " ind2="4" tag="655">
            <subfield code="a"><xsl:value-of select="$data"/></subfield>
        </datafield>
        </xsl:if>
        <xsl:copy-of select="."/> 
    </xsl:template>
    
    <!-- If matching 866 exists, apply this template, otherwise, call it -->
    <xsl:template match="marc:datafield[@tag='886']" name="build_856" mode="holdings_only">
        <xsl:param name="ind2"/>
        <xsl:param name="comment"/>
        <xsl:param name="tpbid"/>
        <datafield xmlns="http://www.loc.gov/MARC21/slim" ind1="4" ind2="{$ind2}" tag="856">
            <subfield code='u'>http://www.legimus.se/work/redirect?MedieNr=<xsl:value-of select="$tpbid"/></subfield>
            <!--<subfield code='u'>http://katalog.tpb.se/wsHitList.Asp?SCode1=TN&amp;SearchStr1=<xsl:value-of select="$tpbid"/></subfield>-->
            <subfield code='z'><xsl:value-of select="$comment"/><xsl:text> Tillgänglig för personer med läsnedsättning enligt § 17 Upphovsrättslagen</xsl:text></subfield>
        </datafield>
    </xsl:template>
    
    <xsl:template match="marc:datafield[number(@tag) > 899 and number(@tag) &lt; 945]"/>   
    <xsl:template match="marc:datafield[number(@tag) > 945 and number(@tag) &lt; 976]"/>
    <xsl:template match="marc:datafield[number(@tag) > 976 and number(@tag) &lt; 1000]"/>
    
    <xsl:template match="marc:datafield[@tag='945']">
        <datafield xmlns="http://www.loc.gov/MARC21/slim" ind1="1" ind2=" " tag="246">
            <subfield code="i">Föredragen titel</subfield>
            <xsl:apply-templates/>
        </datafield>
    </xsl:template>
    
    <!-- Copy template -->
    <xsl:template match="@* | node()">
        <xsl:copy>
            <xsl:apply-templates select="@* | node()"/>
        </xsl:copy>
    </xsl:template>
    
    <xsl:template name="create042">
        <datafield xmlns="http://www.loc.gov/MARC21/slim" ind1=" " ind2=" " tag="042">
            <subfield code="9">Mtm</subfield>
        </datafield>
    </xsl:template>
    
    <xsl:template name="create506">
       <datafield xmlns="http://www.loc.gov/MARC21/slim" ind1="1" ind2=" " tag="506">
           <subfield code="a">Tillgänglig för personer med läsnedsättning enligt § 17 Upphovsrättslagen</subfield>
           <subfield code="u">http://www.mtm.se/om-oss/uppdrag-och-lagar/upphovsrattslagen/</subfield>
        </datafield>
    </xsl:template>
</xsl:stylesheet>
