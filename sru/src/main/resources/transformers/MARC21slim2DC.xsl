<!-- <?xml version="1.0" encoding="UTF-8"?> -->
<xsl:stylesheet version="1.0" 
        xmlns:marc="http://www.loc.gov/MARC21/slim"
        xmlns:dc="http://purl.org/dc/elements/1.1/"
        xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
        exclude-result-prefixes="marc">
	<xsl:output method="xml" indent="yes" omit-xml-declaration="yes"/>
	
	<xsl:template match="/">
			<xsl:apply-templates/>
	</xsl:template>

	<xsl:template match="marc:collection"> <!-- For xsearch only? -->
            <dc_collection xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <xsl:apply-templates select="marc:record"/>
            </dc_collection>
        </xsl:template>

	<xsl:template match="marc:record">
		<xsl:variable name="leader" select="marc:leader"/>
		<xsl:variable name="leader6" select="substring($leader,7,1)"/>
		<xsl:variable name="leader7" select="substring($leader,8,1)"/>
		<xsl:variable name="controlField008" select="marc:controlfield[@tag=008]"/>
                <dc xmlns="http://purl.org/dc/elements/1.1/">
                        <xsl:for-each select="marc:datafield[@tag=245]">
				<title>
					<xsl:call-template name="subfieldSelect">
						<xsl:with-param name="codes">abfghk</xsl:with-param>
					</xsl:call-template>
				</title>
			</xsl:for-each>

	
			<xsl:for-each select="marc:datafield[@tag=100]|marc:datafield[@tag=110]|marc:datafield[@tag=111]|marc:datafield[@tag=700]|marc:datafield[@tag=710]|marc:datafield[@tag=711]|marc:datafield[@tag=720]">
				<creator>
					<xsl:value-of select="./marc:subfield[@code='a']"/>
				</creator>
			</xsl:for-each>

			<type>		
				<xsl:if test="$leader7='c'">
					<xsl:attribute name="collection">yes</xsl:attribute>
				</xsl:if>

				<xsl:if test="$leader6='d' or $leader6='f' or $leader6='p' or $leader6='t'">
					<xsl:attribute name="manuscript">yes</xsl:attribute>
				</xsl:if>

				<xsl:choose>
					<xsl:when test="$leader6='a' or $leader6='t'">text</xsl:when>
					<xsl:when test="$leader6='e' or $leader6='f'">cartographic</xsl:when>
					<xsl:when test="$leader6='c' or $leader6='d'">notated music</xsl:when>
					<xsl:when test="$leader6='i' or $leader6='j'">sound recording</xsl:when>
					<xsl:when test="$leader6='k'">still image</xsl:when>
					<xsl:when test="$leader6='g'">moving image</xsl:when>
					<xsl:when test="$leader6='r'">three dimensional object</xsl:when>
					<xsl:when test="$leader6='m'">software, multimedia</xsl:when>
					<xsl:when test="$leader6='p'">mixed material</xsl:when>
				</xsl:choose>
			</type>

			<xsl:for-each select="marc:datafield[@tag=655]">
				<type>
					<xsl:value-of select="./marc:subfield[@code='a']"/>
				</type>
			</xsl:for-each>

			<xsl:for-each select="marc:datafield[@tag=260]">
				<publisher>
					<xsl:call-template name="subfieldSelect">
						<xsl:with-param name="codes">ab</xsl:with-param>
					</xsl:call-template>
				</publisher>
			</xsl:for-each>

			<xsl:for-each select="marc:datafield[@tag=260]/marc:subfield[@code='c']">
				<date>
					<xsl:value-of select="."/>
				</date>				
			</xsl:for-each>

			<language>
				<xsl:value-of select="substring($controlField008,36,3)"/>
			</language>

			<xsl:for-each select="marc:datafield[@tag=856]/marc:subfield[@code='q']">
				<format>
					<xsl:value-of select="."/>
				</format>
			</xsl:for-each>

			<xsl:for-each select="marc:datafield[@tag=520]">
				<description>
					<xsl:value-of select="marc:subfield[@code='a']"/>
				</description>
			</xsl:for-each>

			<xsl:for-each select="marc:datafield[@tag=521]">
				<description>
					<xsl:value-of select="marc:subfield[@code='a']"/>
				</description>
			</xsl:for-each>

			<xsl:for-each select="marc:datafield[500&lt;@tag][@tag&lt;=599][not(@tag=506 or @tag=530 or @tag=540 or @tag=546)]">
				<description>
					<xsl:value-of select="marc:subfield[@code='a']"/>
				</description>
			</xsl:for-each>

			<xsl:for-each select="marc:datafield[@tag=600]">
				<subject>
					<xsl:call-template name="subfieldSelect">
						<xsl:with-param name="codes">abcdq</xsl:with-param>
					</xsl:call-template>
				</subject>
			</xsl:for-each>

			<xsl:for-each select="marc:datafield[@tag=610]">
				<subject>
					<xsl:call-template name="subfieldSelect">
						<xsl:with-param name="codes">abcdq</xsl:with-param>
					</xsl:call-template>
				</subject>
			</xsl:for-each>

			<xsl:for-each select="marc:datafield[@tag=611]">
				<subject>
					<xsl:call-template name="subfieldSelect">
						<xsl:with-param name="codes">abcdq</xsl:with-param>
					</xsl:call-template>
				</subject>
			</xsl:for-each>
		
			<xsl:for-each select="marc:datafield[@tag=630]">
				<subject>
					<xsl:call-template name="subfieldSelect">
						<xsl:with-param name="codes">abcdq</xsl:with-param>
					</xsl:call-template>
				</subject>
			</xsl:for-each>

			<xsl:for-each select="marc:datafield[@tag=650]">
				<subject>
					<xsl:call-template name="subfieldSelect">
						<xsl:with-param name="codes">abcdq</xsl:with-param>
					</xsl:call-template>
				</subject>
			</xsl:for-each>

			<xsl:for-each select="marc:datafield[@tag=653]">
				<subject>
					<xsl:call-template name="subfieldSelect">
						<xsl:with-param name="codes">abcdq</xsl:with-param>
					</xsl:call-template>
				</subject>
			</xsl:for-each>

			<xsl:for-each select="marc:datafield[@tag=752]">
				<coverage>
					<xsl:call-template name="subfieldSelect">
						<xsl:with-param name="codes">abcd</xsl:with-param>
					</xsl:call-template>
				</coverage>
			</xsl:for-each>

			<xsl:for-each select="marc:datafield[@tag=530]">
				<relation type="original">
					<xsl:call-template name="subfieldSelect">
						<xsl:with-param name="codes">abcdu</xsl:with-param>
					</xsl:call-template>
				</relation>	
			</xsl:for-each>

			<xsl:for-each select="marc:datafield[@tag=760]|marc:datafield[@tag=762]|marc:datafield[@tag=765]|marc:datafield[@tag=767]|marc:datafield[@tag=770]|marc:datafield[@tag=772]|marc:datafield[@tag=773]|marc:datafield[@tag=774]|marc:datafield[@tag=775]|marc:datafield[@tag=776]|marc:datafield[@tag=777]|marc:datafield[@tag=780]|marc:datafield[@tag=785]|marc:datafield[@tag=786]|marc:datafield[@tag=787]">
				<relation>
					<xsl:call-template name="subfieldSelect">
						<xsl:with-param name="codes">ot</xsl:with-param>
					</xsl:call-template>
				</relation>	
			</xsl:for-each>

			<xsl:for-each select="marc:datafield[@tag=856]">
				<identifier>
					<xsl:value-of select="marc:subfield[@code='u']"/>
				</identifier>
			</xsl:for-each>

			<xsl:for-each select="marc:datafield[@tag=506]">
				<rights>
					<xsl:value-of select="marc:subfield[@code='a']"/>
				</rights>
			</xsl:for-each>

			<xsl:for-each select="marc:datafield[@tag=540]">
				<rights>
					<xsl:value-of select="marc:subfield[@code='a']"/>
				</rights>
			</xsl:for-each>
		</dc>
	</xsl:template>

	<xsl:template name="datafield">
		<xsl:param name="tag"/>
		<xsl:param name="ind1"><xsl:text> </xsl:text></xsl:param>
		<xsl:param name="ind2"><xsl:text> </xsl:text></xsl:param>
		<xsl:param name="subfields"/>
		<xsl:element name="datafield">
			<xsl:attribute name="tag">
				<xsl:value-of select="$tag"/>
			</xsl:attribute>
			<xsl:attribute name="ind1">
				<xsl:value-of select="$ind1"/>
			</xsl:attribute>
			<xsl:attribute name="ind2">
				<xsl:value-of select="$ind2"/>
			</xsl:attribute>
			<xsl:copy-of select="$subfields"/>
		</xsl:element>
	</xsl:template>

	<xsl:template name="subfieldSelect">
		<xsl:param name="codes"/>
		<xsl:param name="delimeter"><xsl:text> </xsl:text></xsl:param>
		<xsl:variable name="str">
			<xsl:for-each select="marc:subfield">
				<xsl:if test="contains($codes, @code)">
					<xsl:value-of select="text()"/><xsl:value-of select="$delimeter"/>
				</xsl:if>
			</xsl:for-each>
		</xsl:variable>
		<xsl:value-of select="substring($str,1,string-length($str)-string-length($delimeter))"/>
	</xsl:template>

	<xsl:template name="buildSpaces">
		<xsl:param name="spaces"/>
		<xsl:param name="char"><xsl:text> </xsl:text></xsl:param>
		<xsl:if test="$spaces>0">
			<xsl:value-of select="$char"/>
			<xsl:call-template name="buildSpaces">
				<xsl:with-param name="spaces" select="$spaces - 1"/>
				<xsl:with-param name="char" select="$char"/>
			</xsl:call-template>
		</xsl:if>
	</xsl:template>

	<xsl:template name="chopPunctuation">
		<xsl:param name="chopString"/>
		<xsl:param name="punctuation"><xsl:text>.:,;/ </xsl:text></xsl:param>
		<xsl:variable name="length" select="string-length($chopString)"/>
		<xsl:choose>
			<xsl:when test="$length=0"/>
			<xsl:when test="contains($punctuation, substring($chopString,$length,1))">
				<xsl:call-template name="chopPunctuation">
					<xsl:with-param name="chopString" select="substring($chopString,1,$length - 1)"/>
					<xsl:with-param name="punctuation" select="$punctuation"/>
				</xsl:call-template>
			</xsl:when>
			<xsl:when test="not($chopString)"/>
			<xsl:otherwise><xsl:value-of select="$chopString"/></xsl:otherwise>
		</xsl:choose>
	</xsl:template>

	<xsl:template name="chopPunctuationFront">
		<xsl:param name="chopString"/>
		<xsl:variable name="length" select="string-length($chopString)"/>
		<xsl:choose>
			<xsl:when test="$length=0"/>
			<xsl:when test="contains('.:,;/[ ', substring($chopString,1,1))">
				<xsl:call-template name="chopPunctuationFront">
					<xsl:with-param name="chopString" select="substring($chopString,2,$length - 1)"/>
				</xsl:call-template>
			</xsl:when>
			<xsl:when test="not($chopString)"/>
			<xsl:otherwise><xsl:value-of select="$chopString"/></xsl:otherwise>
		</xsl:choose>
	</xsl:template>

	<xsl:template match="*|@*|text()">
        	<xsl:copy>
            		<xsl:apply-templates select="*|@*|text()"/>
        	</xsl:copy>
    	</xsl:template>
</xsl:stylesheet>
