<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0"
    xmlns:marc="http://www.loc.gov/MARC21/slim" xmlns:java="http://xml.apache.org/xslt/java"
    exclude-result-prefixes="marc java">
    <xsl:output method="xml" omit-xml-declaration="no" indent="yes"/>
    <xsl:template match="marc:record[not(@type) or @type = 'Bibliographic']">
	<record xmlns="http://www.loc.gov/MARC21/slim" type="Bibliographic">
            <xsl:apply-templates/>
            <xsl:variable name="leader"><xsl:value-of select="marc:leader"/></xsl:variable>
            <xsl:variable name="leader17" select="substring($leader,18,1)"/>
            <xsl:if test="($leader17 = '5' or $leader17 = '8') and not(marc:datafield[@tag='599' and (contains(normalize-space(.), 'Maskinellt genererad') or contains(normalize-space(.), 'Upgraded record'))]) and not(marc:controlfield[@tag='003' and (normalize-space(.) = 'SE-LIBR' or normalize-space(.) = 'LIBRIS')])">
                <datafield ind1=" " ind2=" " tag="599" xmlns="http://www.loc.gov/MARC21/slim">
                    <subfield code="a">Maskinellt genererad post. Ändra kod för fullständighetsnivå (leader/17), annars kommer manuellt gjorda ändringar att försvinna.</subfield>
                </datafield>
            </xsl:if>
	</record>
        <xsl:if test="not(following-sibling::marc:record[1]/@type='Holdings') and marc:datafield[@tag='852' and marc:subfield[@code='b']]">
            <xsl:call-template name="createHolding"/>
        </xsl:if>
    </xsl:template>

    <!-- KP: special leader treatment -->
    <xsl:template match="marc:leader">
	<leader xmlns="http://www.loc.gov/MARC21/slim"><xsl:value-of select="substring(.,1,9)"/>a<xsl:value-of select="substring(.,11,7)"/>7<xsl:value-of select="substring(.,19,5)"/>0</leader>
    </xsl:template>

    <!-- KP: remove non 245 records -->
    <xsl:template match="marc:record[(not(@type) or @type = 'Bibliographic') and normalize-space(./marc:datafield[@tag='245']/marc:subfield[@code='a']) = '']">
    </xsl:template>

    <!-- KP: and, remove its non 245 holding -->
    <xsl:template match="marc:record[ @type = 'Holdings' and normalize-space(preceding-sibling::marc:record[not(@type) or @type = 'Bibliographic'][1]/marc:datafield[@tag='245' ]/marc:subfield[@code='a']) = '' ]">
    </xsl:template>

    <!-- Controlfield 001 -->
    <xsl:template match="marc:controlfield[@tag='001']">
        <!--<xsl:if test="not(../marc:datafield[@tag='035']) and not(../marc:controlfield[@tag='003' and (contains(normalize-space(.), 'SE-LIBR') or contains(normalize-space(.), 'LIBRIS'))])">-->
        <!--<xsl:if test="not(../marc:datafield[@tag='035']) and ../marc:controlfield[@tag='003'] and not(../marc:controlfield[@tag='003' and (normalize-space(.) = 'SE-LIBR' or normalize-space(.) = 'LIBRIS')])"> -->
            <xsl:variable name="cf001" select="normalize-space(translate(., ' -', ''))"/>
            <datafield ind1=" " ind2=" " tag="035" xmlns="http://www.loc.gov/MARC21/slim">
                <!--<subfield code="a"><xsl:if test="../marc:controlfield[@tag='003'] and string-length($cf001) > 8">(<xsl:value-of select="../marc:controlfield[@tag='003']"/>)</xsl:if><xsl:if test="../marc:controlfield[@tag='003'] and string-length($cf001) &lt; 9"><xsl:value-of select="../marc:controlfield[@tag='003']"/></xsl:if><xsl:value-of select="$cf001"/></subfield>-->
                <subfield code="a">SBCI<xsl:value-of select="$cf001"/></subfield>
            </datafield>
        <!--</xsl:if>-->
        <xsl:copy-of select="."/>
    </xsl:template>
    
    <xsl:template name="createHolding">
        <record xmlns="http://www.loc.gov/MARC21/slim" type="Holdings">
                <xsl:variable name="rawTimeStamp"><xsl:value-of select="java:format(java:java.text.SimpleDateFormat.new('yyyy-MM-dd'), java:java.util.Date.new())"/></xsl:variable>
                <xsl:variable name="timeStamp" select="substring(translate($rawTimeStamp, '-', ''), 3)"/>
                <leader xmlns="http://www.loc.gov/MARC21/slim"><xsl:text>*****nx  a22*****1n 4500</xsl:text></leader>
                <controlfield xmlns="http://www.loc.gov/MARC21/slim" tag="008"><xsl:value-of select="$timeStamp"/>||0000|||||000||||||000000</controlfield>
                <xsl:copy-of select="marc:datafield[@tag='852' and marc:subfield[@code='b']]"/>
            </record>
    </xsl:template>
    
    <xsl:template match="marc:datafield[@tag='852' and not(parent::marc:record/@type='Holdings')]"/>
        

    <xsl:template match="marc:datafield[@tag='020']">
        <xsl:variable name="isbn1" select="substring(normalize-space(marc:subfield[@code='a']), 1, 1)"/>
        <xsl:choose>
            <xsl:when test="string(number($isbn1)) = 'NaN'">
                <datafield ind1="8" ind2=" " tag="024" xmlns="http://www.loc.gov/MARC21/slim">
                    <subfield code="a"><xsl:value-of select="marc:subfield[@code='a']"/></subfield>
                </datafield>
            </xsl:when>
            <xsl:otherwise>
                <xsl:copy-of select="."/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    
    <!-- 150209 KP, filter some fields (match with no output) -->
    <xsl:template match="marc:datafield[(@tag='082' or @tag='084') and contains(./marc:subfield[@code='2'], 'machine generated')]">
    </xsl:template>

    <xsl:template match="marc:datafield[starts-with(@tag, '9')]">
    </xsl:template>

    <xsl:template match="marc:datafield[count(./marc:subfield[@code='5']) &gt; 0]">
    </xsl:template>

    <xsl:template match="marc:subfield[@code='0']">
	<xsl:variable name="p" select="../@tag"/>
	<xsl:if test="not($p = '100' or $p ='110' or $p ='111' or $p ='130' or $p ='600' or $p ='610' or $p ='611' or $p ='630' or $p ='648' or $p ='650' or $p ='651' or $p ='653' or $p ='654' or $p ='655' or $p ='700' or $p ='710' or $p ='711' or $p ='720' or $p ='730')">
		<xsl:copy-of select="."/>
	</xsl:if>
    </xsl:template>

    <!-- Copy template -->
    <xsl:template match="@* | node()">
        <xsl:copy>
            <xsl:apply-templates select="@* | node()"/>
        </xsl:copy>
    </xsl:template>


</xsl:stylesheet>
