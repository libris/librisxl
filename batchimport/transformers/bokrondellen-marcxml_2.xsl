<?xml version="1.0" encoding="UTF-8" ?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

    <xsl:param name="f008_00-05">050420</xsl:param>

    <xsl:template match="/artikelregister">
        <collection xmlns:marc="http://www.loc.gov/MARC21/slim">
	<!--<collection>-->
            <xsl:for-each select="artikel">
                <xsl:variable name="lang">
                    <xsl:choose>
                        <xsl:when test="sprak = 'Svenska'">swe</xsl:when>
                        <xsl:when test="sprak = 'Engelska'">eng</xsl:when>
			<xsl:when test="sprak = 'engelska'">eng</xsl:when>
			<xsl:when test="sprak = 'eng'">eng</xsl:when>
                        <xsl:when test="sprak = 'Tyska'">ger</xsl:when>
                        <xsl:otherwise>swe</xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
		<xsl:variable name="bandtyp">
			<xsl:choose>
				<xsl:when test="bandtyp = 'Inbunden'"> (inb.) </xsl:when>
				<xsl:when test="bandtyp = 'Sprial'"> (Spiralbunden) </xsl:when>
				<xsl:when test="bandtyp = 'Kartonage'"> (Kartonage) </xsl:when>
				<xsl:otherwise></xsl:otherwise>
			</xsl:choose>
		</xsl:variable>
                <xsl:variable name="leader_06-07">
			<xsl:choose>
				<xsl:when test="medietyp = 'Ljudbok'">im</xsl:when>
				<xsl:when test="medietyp = 'Bok'">am</xsl:when>
				<xsl:when test="medietyp = 'Multimedia'">mm</xsl:when>
				<xsl:when test="bandtyp = 'CD PC'">mm</xsl:when>
				<xsl:when test="bandtyp = 'Diskett'">mm</xsl:when>
				<xsl:when test="bandtyp = 'Häftad'">am</xsl:when>
				<xsl:when test="bandtyp = 'Karta'">em</xsl:when>
				<xsl:when test="bandtyp = 'Kassett'">im</xsl:when>
				<xsl:when test="bandtyp = 'Komplex produkt'">pm</xsl:when>
				<xsl:when test="bandtyp = 'Ljud-CD'">im</xsl:when>
				<xsl:when test="bandtyp = 'Multimedia'">mm</xsl:when>
				<xsl:when test="bandtyp = 'Online'">mm</xsl:when>
				<xsl:when test="bandtyp = 'Pocket'">am</xsl:when>
				<xsl:when test="bandtyp = 'Spiral'">am</xsl:when>
				<xsl:when test="bandtyp = 'Övrigt'">am</xsl:when>
				<xsl:when test="bandtyp = 'CD-skiva'">im</xsl:when>
				<xsl:when test="bandtyp = 'Kartonage'">am</xsl:when>
				<xsl:when test="bandtyp = 'Multipel/Bok'">am</xsl:when>
				<xsl:otherwise>am</xsl:otherwise>
			</xsl:choose>
		</xsl:variable>
		<xsl:variable name="f008_06-10">
                    <xsl:choose>
                        <xsl:when test="utgivningsdatum = 0">|||||</xsl:when>
                        <xsl:otherwise>s<xsl:value-of select="substring(utgivningsdatum, 1, 4)"/></xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>

		<!-- 008/22, 050720 PHH -->
		<!--xsl:variable name="f008_22">
                    <xsl:choose>
                        <xsl:when test="laromedel = 'Ja'">c</xsl:when>
                        <xsl:otherwise>&#32;</xsl:otherwise>
                    </xsl:choose>
                </xsl:variable--> 

		<!-- 008/22 061207 MM -->
		<xsl:variable name="f008_22">&#32;</xsl:variable>

		<xsl:variable name="f008_23">
                    <xsl:choose>
                        <xsl:when test="bandtyp = 'CD PC'">s</xsl:when>
			<xsl:when test="bandtyp = 'Diskett'">s</xsl:when>
			<xsl:when test="bandtyp = 'Multimedia'">s</xsl:when>
			<xsl:when test="bandtyp = 'Online'">s</xsl:when>
                        <xsl:otherwise>&#32;</xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
                <!-- Bibliographic record -->            
                <record type="Bibliographic">
                    <!-- leader -->
                    <leader>#####n<xsl:value-of select="$leader_06-07"/>  22#####8a 4500</leader>
                    
                    <!-- 008 -->
                    <!--<controlfield tag="008"><xsl:value-of select="$f008_00-05"/><xsl:value-of select="$f008_06-10"/>    sw ||||<xsl:value-of select="$f008_22"/>     |00| 0|-->
<controlfield tag="008"><xsl:value-of select="concat($f008_00-05, $f008_06-10, '&#32;&#32;&#32;&#32;', 'sw', '&#32;', '||||', $f008_22, $f008_23 ,'&#32;&#32;&#32;&#32;', '|00| 0|', normalize-space($lang), '&#32;&#32;')"/></controlfield>
                    <!-- 020/022 -->                    
                    <xsl:choose>
                        <xsl:when test="string-length(artikelnummer) = 10 or string-length(artikelnummer) = 13">
                            <datafield tag="020" ind1=" " ind2=" ">
                                <subfield code="a"><xsl:value-of select="artikelnummer"/> <xsl:value-of select="$bandtyp"/></subfield>
                            </datafield>
                        </xsl:when>
                        <xsl:when test="string-length(artikelnummer) = 8">
                            <datafield tag="022" ind1=" " ind2=" ">
                                <subfield code="a"><xsl:value-of select="artikelnummer"/></subfield>
                            </datafield>
                        </xsl:when>
                    </xsl:choose>
                    
                    <!-- 040 -->
                    <datafield tag="040" ind1=" " ind2=" ">
                        <subfield code="a">LGEN</subfield>
                    </datafield>
                    
                    <!-- 100 -->
                    <xsl:for-each select="medarbetare[@typ = 'forfattare']">
                        <xsl:if test="position() = 1">
                        <datafield tag="100" ind1="1" ind2=" ">
                            <subfield code="a"><xsl:value-of select="."/></subfield>
                        </datafield>
                        </xsl:if>
                    </xsl:for-each>
		    
                    <!-- 240, 050720 PHH -->
		    <xsl:if test="( originaltitel and originaltitel != '')">
                    <datafield tag="240" ind1=" " ind2=" ">		    	
                        <subfield code="a"><xsl:value-of select="originaltitel"/></subfield>
			<subfield code="l"><xsl:value-of select="$lang"/></subfield>
                    </datafield>
	            </xsl:if>
                    
		    <!-- 245 -->
                    <datafield tag="245" ind1="1" ind2="0">
                        <xsl:choose>
                            <xsl:when test="titel and titel != ''">
                                <subfield code="a"><xsl:value-of select="titel"/></subfield>
                            </xsl:when>
                            <xsl:when test="arbetstitel and arbetstitel != ''">
                                <subfield code="a"><xsl:value-of select="arbetstitel"/></subfield>
                            </xsl:when>
                            <xsl:otherwise>
                                <subfield code="a">[Titel saknas]</subfield>
                            </xsl:otherwise>        
                        </xsl:choose>
                    </datafield>
		    
                    <!-- 250,  050720 PHH-->
		    <xsl:if test="upplagenummer and upplagenummer != '' and upplagenummer!='0'">
                    <datafield tag="260" ind1=" " ind2=" ">
                        <subfield code="a"><xsl:value-of select="upplagenummer"/></subfield>                        
                    </datafield>
		    </xsl:if>
		    
                    <!-- 260 -->
                    <datafield tag="260" ind1=" " ind2=" ">
                        <subfield code="b"><xsl:value-of select="forlag"/>,</subfield>
                        <subfield code="c"><xsl:value-of select="substring(utgivningsdatum, 1, 4)"/></subfield>
                    </datafield>

                    <!-- 263 -->
                    <xsl:if test="utgivningsdatum and utgivningsdatum != ''">
                    <datafield tag="263" ind1=" " ind2=" ">
                        <subfield code="a"><xsl:value-of select="substring(utgivningsdatum, 1, 6)"/></subfield>
                    </datafield>
	            </xsl:if>
		    
		    <!-- 300, 050720 PHH -->
		    <xsl:if test="( omfang and omfang != '' and omfang[@enhet = 'sidor']) or ( hojd and hojd != '' and hojd != '0') or ( illustrerad and illustrerad = 'Ja')">
                    <datafield tag="300" ind1=" " ind2=" ">
                       <xsl:if test="( omfang and omfang != '' and omfang[@enhet = 'sidor'])"><subfield code="a"><xsl:value-of select="omfang"/> s.</subfield></xsl:if>
		       <xsl:if test="( illustrerad and illustrerad = 'Ja' )"><subfield code="b">ill.</subfield></xsl:if>
		       <xsl:if test="( hojd and hojd != '0' and hojd != '' )">
		       <xsl:variable name="cm_hojd">
		       	<xsl:choose>
				<xsl:when test="substring(hojd, string-length(hojd)) != 0"><xsl:value-of select="substring(hojd,0,string-length(hojd)) + 1"/></xsl:when>
				<xsl:otherwise><xsl:value-of select="substring(hojd,0,string-length(hojd))"/></xsl:otherwise>
			</xsl:choose>
		       </xsl:variable>
		       <!--<subfield code="c"><xsl:value-of select="$cm_hojd"/> cm(<xsl:value-of select="hojd"/>)[<xsl:value-of select="substring(hojd, string-length(hojd))"/>]</subfield>-->
		       <subfield code="c"><xsl:value-of select="$cm_hojd"/> cm</subfield>
		       </xsl:if>
		       
                    </datafield>
	            </xsl:if>
		    
		    
		    
		    <!-- 490, 050720 PHH -->
		    <xsl:if test="( serie and serie != '')">
                    <datafield tag="490" ind1=" " ind2=" ">
                        <subfield code="a"><xsl:value-of select="serie"/></subfield>
                    </datafield>
	            </xsl:if>
		    
		    <!-- 520, 050720 PHH -->
		    <xsl:if test="( saga and saga != '')">
                    <datafield tag="520" ind1=" " ind2=" ">
                        <subfield code="a">"<xsl:value-of select="saga"/>" maskininläst förlagsinformation</subfield>
                    </datafield>
	            </xsl:if>
		    
		    <!-- 520, 050720 PHH -->
		    <xsl:if test="( katalogtext and katalogtext != '')">
                    <datafield tag="520" ind1=" " ind2=" ">
                        <subfield code="a">"<xsl:value-of select="katalogtext"/>" maskininläst förlagsinformation</subfield>
                    </datafield>
	            </xsl:if>
		    
		    <!-- 599, 050720 PHH -->
		    <xsl:if test="( lasordning and lasordning != '')">
                    <datafield tag="599" ind1=" " ind2=" ">
                        <subfield code="a">Läsordning <xsl:value-of select="lasordning"/></subfield>
                    </datafield>
	            </xsl:if>
		    
		    <!-- 655, 050720 PHH -->
		    <xsl:if test="genre and genre != ''">
                    <datafield tag="655" ind1=" " ind2="4">
                        <subfield code="a"><xsl:value-of select="genre"/></subfield>
                    </datafield>
	            </xsl:if>
		    
                    <!-- 700 -->
                    <xsl:for-each select="medarbetare[@typ = 'forfattare']">
                        <xsl:if test="position() != 1">
                        <datafield tag="700" ind1="1" ind2=" ">
                            <subfield code="a"><xsl:value-of select="."/></subfield>
                        </datafield>
                        </xsl:if>
                    </xsl:for-each>
		    
		    <!-- 856, 050720 PHH-->
		    <xsl:if test="omslagsbild and omslagsbild != ''">		    
			<datafield tag="856" ind1=" " ind2=" ">
				<subfield code="z">Omslagsbild</subfield>
				<subfield code="u"><xsl:value-of select="omslagsbild"/></subfield>
			</datafield>
		    </xsl:if>
                </record>

                <!-- Holdings record -->
                <record type="Holdings">
                    <!-- leader -->
                    <leader>#####nx   22#####1n 4500</leader>
                    
                    <!-- 008 -->
                    <controlfield tag="008"><xsl:value-of select="$f008_00-05"/>||0000|||||001||||||000000</controlfield>

                    <!-- 852 -->
                    <datafield tag="852" ind1=" " ind2=" ">
                        <subfield code="b">BOKR</subfield>
                    </datafield>                    
                </record>
            </xsl:for-each>
        </collection>
    </xsl:template>
    
    <xsl:template name="bandtyp">
    	
    </xsl:template>
</xsl:stylesheet> 
