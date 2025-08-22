<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" 
                xmlns:marc="http://www.loc.gov/MARC21/slim"
                xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xmlns:dc="http://purl.org/dc/elements/1.1/"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                exclude-result-prefixes="marc">
    <xsl:output method="text" encoding="UTF-8" indent="no"/>
    <!--<xsl:key name="sigel" match="marc:datafield[@tag = '856' and marc:subfield[@code='5'] and starts-with(marc:subfield[@code = 'u'], 'http')]" use="normalize-space(marc:subfield[@code='5'])"/>-->
    <xsl:template match="/xsearch">
        <xsl:text>{"xsearch": {&#xd;&#xa;</xsl:text>
        <xsl:text>"from": </xsl:text>
	<xsl:choose>
	<xsl:when test="normalize-space(@from) = ''">null</xsl:when>
	<xsl:when test="normalize-space(@from) = 'NaN'">null</xsl:when>
	<xsl:otherwise>	
	<xsl:value-of select="@from"/>	
	</xsl:otherwise>
	</xsl:choose>
        <xsl:text>,&#xd;&#xa;"to": </xsl:text>
	<xsl:choose>
	<xsl:when test="normalize-space(@to) = ''">null</xsl:when>
	<xsl:when test="normalize-space(@to) = 'NaN'">null</xsl:when>
	<xsl:otherwise>	
	<xsl:value-of select="@to"/>
	</xsl:otherwise>
	</xsl:choose>
        <xsl:text>,&#xd;&#xa;"records": </xsl:text><xsl:value-of select="@records"/>
        <xsl:text>,&#xd;&#xa;</xsl:text>
        <xsl:apply-templates/>
        <xsl:text>}}</xsl:text>
    </xsl:template>
    
    <xsl:template match="marc:collection">
        <xsl:text>"list": [&#xd;&#xa;</xsl:text>
        <xsl:variable name="list">
            <xsl:apply-templates select="marc:record"/>
        </xsl:variable>

        <xsl:call-template name="chopPunctuation">
            <xsl:with-param name="chopString" select="$list"/>
            <xsl:with-param name="punctuation"><xsl:text>.:,;/ &#xd;&#xa;</xsl:text></xsl:with-param>
        </xsl:call-template>
        <xsl:text>&#xd;&#xa;]&#xd;&#xa;</xsl:text>
    </xsl:template>
    
    <xsl:template match="marc:record">
        <xsl:variable name="leader" select="marc:leader"/>
        <xsl:variable name="leader6" select="substring($leader,7,1)"/>
        <xsl:variable name="leader7" select="substring($leader,8,1)"/>
        <xsl:variable name="leader6-7" select="substring(marc:leader, 7, 2)"/>
        <xsl:variable name="cf007" select="substring(marc:controlfield[@tag = '007'], 1, 2)"/>
        <xsl:variable name="controlField008" select="marc:controlfield[@tag=008]"/>
        
        <xsl:text>{&#xd;&#xa;</xsl:text>
            
            <xsl:variable name="list_record">
                <xsl:choose>
                    <xsl:when test="marc:datafield[@tag=042]/marc:subfield[@code=9][. ='SwePub']">
                        <xsl:text>"identifier": "http://swepub.kb.se/bib/swepub:</xsl:text><xsl:value-of select="marc:controlfield[@tag=001]"/><xsl:text>",&#xd;&#xa;</xsl:text>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:text>"identifier": "http://libris.kb.se/bib/</xsl:text><xsl:value-of select="marc:controlfield[@tag=001]"/><xsl:text>",&#xd;&#xa;</xsl:text>
                    </xsl:otherwise>
                </xsl:choose>
                
                
                <!-- TITLE -->
		<xsl:choose>
                <xsl:when test="count(marc:datafield[@tag=245]) = 1">
                <xsl:text>"title": "</xsl:text>
                <xsl:for-each select="marc:datafield[@tag=245]">
                    <xsl:variable name="quotbaby">
                        <xsl:call-template name="chopPunctuation">
                            <xsl:with-param name="chopString">
                                <xsl:call-template name="subfieldSelect">
                                    <xsl:with-param name="codes">abfghk</xsl:with-param>
                                </xsl:call-template>
                            </xsl:with-param>
                            <xsl:with-param name="punctuation"><xsl:text>.:,;/ </xsl:text></xsl:with-param>
                        </xsl:call-template>
                    </xsl:variable>
                    <xsl:call-template name="replace-character">
                        <xsl:with-param name="word" select="$quotbaby"/>
                    </xsl:call-template>
                    <xsl:text>",&#xd;&#xa;</xsl:text>
                </xsl:for-each>
		</xsl:when>
                <xsl:when test="count(marc:datafield[@tag=245]) > 1">
                <xsl:text>"title": [&#xd;&#xa;</xsl:text>
                <xsl:for-each select="marc:datafield[@tag=245]">
                    <xsl:variable name="quotbaby">
                        <xsl:call-template name="chopPunctuation">
                            <xsl:with-param name="chopString">
                                <xsl:call-template name="subfieldSelect">
                                    <xsl:with-param name="codes">abfghk</xsl:with-param>
                                </xsl:call-template>
                            </xsl:with-param>
                            <xsl:with-param name="punctuation"><xsl:text>.:,;/ </xsl:text></xsl:with-param>
                        </xsl:call-template>
                    </xsl:variable>
                    <xsl:text>"</xsl:text>
                    <xsl:call-template name="replace-character">
                        <xsl:with-param name="word" select="$quotbaby"/>
                    </xsl:call-template>
                    <xsl:text>"</xsl:text>
                    <xsl:if test="position() != count(../marc:datafield[@tag=245])">
                    <xsl:text>, </xsl:text>
                    </xsl:if>
                </xsl:for-each>
                <xsl:text>&#xd;&#xa;],&#xd;&#xa;</xsl:text>
		</xsl:when>
		</xsl:choose>

                <!-- CREATOR -->
                <xsl:if test="count(marc:datafield[@tag=100]|marc:datafield[@tag=110]|marc:datafield[@tag=111]|marc:datafield[@tag=700]|marc:datafield[@tag=710]|marc:datafield[@tag=711]|marc:datafield[@tag=720]) > 0">
                    <xsl:variable name="tmp_creators">
                        <xsl:for-each select="marc:datafield[@tag=100]|marc:datafield[@tag=110]|marc:datafield[@tag=111]|marc:datafield[@tag=700]|marc:datafield[@tag=710]|marc:datafield[@tag=711]|marc:datafield[@tag=720]">
                            <xsl:text>"</xsl:text>
                            <xsl:variable name="df100"><xsl:for-each select="marc:subfield[@code = 'a' or @code = 'b' or @code = 'c' or @code = 'd']"><xsl:value-of select="concat(., ' ')"/></xsl:for-each></xsl:variable>
                            <xsl:call-template name="replace-character">
                                <xsl:with-param name="word" select="normalize-space($df100)"/>
                            </xsl:call-template>
                            <xsl:text>",&#xd;&#xa;</xsl:text>
                        </xsl:for-each>
                    </xsl:variable>
                    <xsl:if test="count(marc:datafield[@tag=100]|marc:datafield[@tag=110]|marc:datafield[@tag=111]|marc:datafield[@tag=700]|marc:datafield[@tag=710]|marc:datafield[@tag=711]|marc:datafield[@tag=720]) > 1">
                        <xsl:text>"creator": [&#xd;&#xa;</xsl:text>
                        <xsl:call-template name="chopPunctuation">
                            <xsl:with-param name="chopString" select="$tmp_creators"/>
                            <xsl:with-param name="punctuation"><xsl:text>.:,;/ &#xd;&#xa;</xsl:text></xsl:with-param>
                        </xsl:call-template>
                        <xsl:text>&#xd;&#xa;]</xsl:text>
                    </xsl:if>
                    <xsl:if test="count(marc:datafield[@tag=100]|marc:datafield[@tag=110]|marc:datafield[@tag=111]|marc:datafield[@tag=700]|marc:datafield[@tag=710]|marc:datafield[@tag=711]|marc:datafield[@tag=720]) = 1">
                        <xsl:text>"creator": </xsl:text>
                        <xsl:call-template name="chopPunctuation">
                            <xsl:with-param name="chopString" select="$tmp_creators"/>
                            <xsl:with-param name="punctuation"><xsl:text>.:,;/ &#xd;&#xa;</xsl:text></xsl:with-param>
                        </xsl:call-template>
                    </xsl:if>
                    <xsl:text>,&#xd;&#xa;</xsl:text>
                </xsl:if>
                
                <!-- ISBN -->
                <xsl:choose>
                    <xsl:when test="count(marc:datafield[@tag=020]/marc:subfield[@code='a' or @code='z']) = 1">
                        <xsl:for-each select="marc:datafield[@tag=020]/marc:subfield[@code='a' or @code='z']">
                            <xsl:text>"isbn": "</xsl:text>
                            <xsl:apply-templates select="." mode="normalizeIsbn"/><xsl:if test="@code='z'"><xsl:text> (invalid)</xsl:text></xsl:if>
                            <xsl:text>",&#xd;&#xa;</xsl:text>
                        </xsl:for-each>
                    </xsl:when>
                    <xsl:when test="count(marc:datafield[@tag=020]/marc:subfield[@code='a' or @code='z']) > 1">
                        <xsl:text>"isbn": [&#xd;&#xa;</xsl:text>
                        <xsl:variable name="tmp_isbn">
                            <xsl:for-each select="marc:datafield[@tag=020]/marc:subfield[@code='a' or @code='z']">
                                <xsl:text>"</xsl:text>
                                <xsl:apply-templates select="." mode="normalizeIsbn"/><xsl:if test="@code='z'"><xsl:text> (invalid)</xsl:text></xsl:if>
                                <xsl:text>",&#xd;&#xa;</xsl:text>
                            </xsl:for-each>
                        </xsl:variable>
                        <xsl:call-template name="chopPunctuation">
                            <xsl:with-param name="chopString" select="$tmp_isbn"/>
                            <xsl:with-param name="punctuation"><xsl:text>.:,;/ &#xd;&#xa;</xsl:text></xsl:with-param>
                        </xsl:call-template>
                        <xsl:text>&#xd;&#xa;],&#xd;&#xa;</xsl:text>
                    </xsl:when>
                </xsl:choose>
                
                <!-- ISSN -->
                <xsl:choose>
                    <xsl:when test="count(marc:datafield[@tag=022]/marc:subfield[@code='a']) = 1">
                        <xsl:for-each select="marc:datafield[@tag=022]/marc:subfield[@code='a']">
                            <xsl:text>"issn": "</xsl:text>
                            <xsl:apply-templates select="." mode="normalizeIssn"/>
                            <xsl:text>",&#xd;&#xa;</xsl:text>
                        </xsl:for-each>
                    </xsl:when>
                    <xsl:when test="count(marc:datafield[@tag=022]/marc:subfield[@code='a']) > 1">
                        <xsl:text>"issn": [&#xd;&#xa;</xsl:text>
                        <xsl:variable name="tmp_issn">
                            <xsl:for-each select="marc:datafield[@tag=022]/marc:subfield[@code='a']">
                                <xsl:text>"</xsl:text>
                                <xsl:apply-templates select="." mode="normalizeIssn"/>
                                <xsl:text>",&#xd;&#xa;</xsl:text>
                            </xsl:for-each>
                        </xsl:variable>
                        <xsl:call-template name="chopPunctuation">
                            <xsl:with-param name="chopString" select="$tmp_issn"/>
                            <xsl:with-param name="punctuation"><xsl:text>.:,;/ &#xd;&#xa;</xsl:text></xsl:with-param>
                        </xsl:call-template>
                        <xsl:text>&#xd;&#xa;],&#xd;&#xa;</xsl:text>
                    </xsl:when>
                </xsl:choose>
                
                <!-- TYPE -->
                <xsl:variable name="type1">
                    <xsl:if test="$leader7='c'">
                        <xsl:attribute name="collection">yes</xsl:attribute>
                    </xsl:if>
                    <xsl:if test="$leader6='d' or $leader6='f' or $leader6='p' or $leader6='t'">
                        <xsl:attribute name="manuscript">yes</xsl:attribute>
                    </xsl:if>
                    <xsl:choose>
                        <xsl:when test="$leader6-7 = 'am' and $cf007 = 'cr'">E-book</xsl:when>
                        <xsl:when test="$leader6-7 = 'am' and $cf007 != 'cr'">book</xsl:when>
                        <xsl:when test="($leader6-7 = 'aa' or $leader6-7 = 'ab') and $cf007 = 'cr'">E-article</xsl:when>
                        <xsl:when test="($leader6-7 = 'aa' or $leader6-7 = 'ab') and $cf007 != 'cr'">article</xsl:when>
                        <xsl:when test="$leader6-7 = 'as' and $cf007 = 'cr'">E-journal</xsl:when>
                        <xsl:when test="$leader6-7 = 'as' and $cf007 != 'cr'">journal</xsl:when>
                        <xsl:when test="$leader6='t'">manuscript</xsl:when>
                        <xsl:when test="$leader6='a'">text</xsl:when>
                        <xsl:when test="$leader6='e' or $leader6='f'">cartographic</xsl:when>
                        <xsl:when test="$leader6='c' or $leader6='d'">notated music</xsl:when>
                        <xsl:when test="$leader6='i'">sound recording</xsl:when>
                        <xsl:when test="$leader6='j'">musical sound recording</xsl:when>
                        <xsl:when test="$leader6='k'">still image</xsl:when>
                        <xsl:when test="$leader6='g'">moving image</xsl:when>
                        <xsl:when test="$leader6='r'">three dimensional object</xsl:when>
                        <xsl:when test="$leader6='m'">software, multimedia</xsl:when>
                        <xsl:when test="$leader6='p'">mixed material</xsl:when>
                        <xsl:when test="$leader6='o'">kit</xsl:when>
                    </xsl:choose>
                </xsl:variable>
                <xsl:text>"type": "</xsl:text>
                    <xsl:value-of select="normalize-space($type1)"/>
                <xsl:text>",&#xd;&#xa;</xsl:text>
                
                <!-- PUBLISHER -->
                <xsl:choose>
                    <xsl:when test="count(marc:datafield[@tag=260 or @tag=264]) = 1">
                        <xsl:text>"publisher": "</xsl:text>
                        <xsl:for-each select="marc:datafield[@tag=260 or @tag=264]">
                            <xsl:variable name="quotbaby">
                            <xsl:call-template name="chopPunctuation">
                                <xsl:with-param name="chopString">
                                    <xsl:call-template name="subfieldSelect">
                                        <xsl:with-param name="codes">ab</xsl:with-param>
                                    </xsl:call-template>
                                </xsl:with-param>
                                <xsl:with-param name="punctuation"><xsl:text>.:,;/ </xsl:text></xsl:with-param>
                            </xsl:call-template>
                            </xsl:variable>
                            <xsl:call-template name="replace-character">
                            <xsl:with-param name="word" select="$quotbaby"/>
                        </xsl:call-template>
                        </xsl:for-each>
                        <xsl:text>",&#xd;&#xa;</xsl:text>
                    </xsl:when>
                    <xsl:when test="count(marc:datafield[@tag=260 or @tag=264]) > 1">
                        <xsl:text>"publisher": [&#xd;&#xa;</xsl:text>
                        <xsl:variable name="tmp_publishers">
                            <xsl:for-each select="marc:datafield[@tag=260 or @tag=264]">
                                <xsl:text>"</xsl:text>
                                <xsl:variable name="quotbaby">
                                <xsl:call-template name="chopPunctuation">
                                    <xsl:with-param name="chopString">
                                        <xsl:call-template name="subfieldSelect">
                                            <xsl:with-param name="codes">ab</xsl:with-param>
                                        </xsl:call-template>
                                    </xsl:with-param>
                                    <xsl:with-param name="punctuation"><xsl:text>.:,;/ </xsl:text></xsl:with-param>
                                </xsl:call-template>
                                </xsl:variable>
                                <xsl:call-template name="replace-character">
                            <xsl:with-param name="word" select="$quotbaby"/>
                        </xsl:call-template>
                                <xsl:text>",&#xd;&#xa;</xsl:text>
                            </xsl:for-each>
                        </xsl:variable>
                        <xsl:call-template name="chopPunctuation">
                            <xsl:with-param name="chopString" select="$tmp_publishers"/>
                            <xsl:with-param name="punctuation"><xsl:text>.:,;/ &#xd;&#xa;</xsl:text></xsl:with-param>
                        </xsl:call-template>
                      <xsl:text>&#xd;&#xa;],&#xd;&#xa;</xsl:text>
                    </xsl:when>
                </xsl:choose>

                <!-- DATE -->
                <xsl:choose>
                    <xsl:when test="count(marc:datafield[@tag=260 or @tag=264]/marc:subfield[@code='c']) = 1">
                        <xsl:for-each select="marc:datafield[@tag=260 or @tag=264]/marc:subfield[@code='c']">
                            <xsl:text>"date": "</xsl:text>
                            <xsl:variable name="quotbaby">
                                <xsl:call-template name="chopPunctuation">
                                    <xsl:with-param name="chopString">
                                        <xsl:value-of select="."/>
                                    </xsl:with-param>
                                    <xsl:with-param name="punctuation"><xsl:text>.:,;/ </xsl:text></xsl:with-param>
                                </xsl:call-template>
                                </xsl:variable>
                                <xsl:call-template name="replace-character">
                            <xsl:with-param name="word" select="$quotbaby"/>
                        </xsl:call-template>
                            <xsl:text>",&#xd;&#xa;</xsl:text>
                        </xsl:for-each>
                    </xsl:when>
                    <xsl:when test="count(marc:datafield[@tag=260 or @tag=264]/marc:subfield[@code='c']) > 1">
                        <xsl:text>"date": [&#xd;&#xa;</xsl:text>
                        <xsl:variable name="tmp_dates">
                            <xsl:for-each select="marc:datafield[@tag=260 or @tag=264]/marc:subfield[@code='c']">
                                <xsl:text>"</xsl:text>
                                <xsl:variable name="quotbaby">
                                    <xsl:call-template name="chopPunctuation">
                                        <xsl:with-param name="chopString">
                                            <xsl:value-of select="."/>
                                        </xsl:with-param>
                                        <xsl:with-param name="punctuation"><xsl:text>.:,;/ </xsl:text></xsl:with-param>
                                    </xsl:call-template>
                                    </xsl:variable>
                                    <xsl:call-template name="replace-character">
                            <xsl:with-param name="word" select="$quotbaby"/>
                        </xsl:call-template>
                                <xsl:text>",&#xd;&#xa;</xsl:text>
                            </xsl:for-each>
                        </xsl:variable>
                        <xsl:call-template name="chopPunctuation">
                            <xsl:with-param name="chopString" select="$tmp_dates"/>
                            <xsl:with-param name="punctuation"><xsl:text>.:,;/ &#xd;&#xa;</xsl:text></xsl:with-param>
                        </xsl:call-template>
                        <xsl:text>&#xd;&#xa;],&#xd;&#xa;</xsl:text>
                    </xsl:when>
                </xsl:choose>

                <!-- LANGUAGE -->
                <xsl:text>"language": "</xsl:text>
                    <xsl:value-of select="substring($controlField008,36,3)"/>
                <xsl:text>",&#xd;&#xa;</xsl:text>

                <!-- FORMAT -->
                <xsl:choose>
                    <xsl:when test="count(marc:datafield[@tag=856]/marc:subfield[@code='q']) = 1">
                        <xsl:for-each select="marc:datafield[@tag=856]/marc:subfield[@code='q']">
                            <xsl:text>"format": "</xsl:text>
                                <xsl:value-of select="."/>
                            <xsl:text>",&#xd;&#xa;</xsl:text>
                        </xsl:for-each>
                    </xsl:when>
                    <xsl:when test="count(marc:datafield[@tag=856]/marc:subfield[@code='q']) > 1">
                        <xsl:text>"format": [&#xd;&#xa;</xsl:text>
                        <xsl:variable name="tmp_formats">
                            <xsl:for-each select="marc:datafield[@tag=856]/marc:subfield[@code='q']">
                                <xsl:text>"</xsl:text>
                                <xsl:value-of select="."/>
                                <xsl:text>",&#xd;&#xa;</xsl:text>
                            </xsl:for-each>
                        </xsl:variable>
                        <xsl:call-template name="chopPunctuation">
                            <xsl:with-param name="chopString" select="$tmp_formats"/>
                            <xsl:with-param name="punctuation"><xsl:text>.:,;/ &#xd;&#xa;</xsl:text></xsl:with-param>
                        </xsl:call-template>
                        <xsl:text>&#xd;&#xa;],&#xd;&#xa;</xsl:text>
                    </xsl:when>
                </xsl:choose>
                
                <!-- DESCRIPTION -->
                <xsl:variable name="tmp_descr">
                    <xsl:for-each select="marc:datafield[@tag=520]">
                        <xsl:text>1</xsl:text>
                    </xsl:for-each>
                    <xsl:for-each select="marc:datafield[@tag=521]">
                        <xsl:text>1</xsl:text>
                    </xsl:for-each>
                    <xsl:for-each select="marc:datafield[500&lt;@tag][@tag&lt;=599][(not(@tag=506 or @tag=530 or @tag=540 or @tag=546 or @tag=520 or @tag=521)) and not(marc:subfield[@code='5'])]">
                        <xsl:text>1</xsl:text>
                    </xsl:for-each>
                </xsl:variable>
                <xsl:choose>
                    <xsl:when test="string-length($tmp_descr) = 1">
                        <xsl:for-each select="marc:datafield[@tag=520]">
                            <xsl:text>"description": "</xsl:text>
                                <xsl:variable name="cleanedquotes">
                                    <xsl:call-template name="replace-character">
                                        <xsl:with-param name="word" select="marc:subfield[@code='a']"/>
                                    </xsl:call-template>
                                </xsl:variable>
                                <xsl:value-of select="$cleanedquotes"/>
                            <xsl:text>",&#xd;&#xa;</xsl:text>
                        </xsl:for-each>
                        <xsl:for-each select="marc:datafield[@tag=521]">
                            <xsl:text>"description": "</xsl:text>
                                <xsl:variable name="cleanedquotes">
                                    <xsl:call-template name="replace-character">
                                        <xsl:with-param name="word" select="marc:subfield[@code='a']"/>
                                    </xsl:call-template>
                                </xsl:variable>
                                <xsl:value-of select="$cleanedquotes"/>
                            <xsl:text>",&#xd;&#xa;</xsl:text>
                        </xsl:for-each>
                        <xsl:for-each select="marc:datafield[500&lt;@tag][@tag&lt;=599][(not(@tag=506 or @tag=530 or @tag=540 or @tag=546 or @tag=520 or @tag=521)) and not(marc:subfield[@code='5'])]">
                            <xsl:text>"description": "</xsl:text>
                                <xsl:variable name="cleanedquotes">
                                    <xsl:call-template name="replace-character">
                                        <xsl:with-param name="word" select="marc:subfield[@code='a']"/>
                                    </xsl:call-template>
                                </xsl:variable>
                                <xsl:value-of select="$cleanedquotes"/>
                            <xsl:text>",&#xd;&#xa;</xsl:text>
                        </xsl:for-each>
                    </xsl:when>
                    <xsl:when test="string-length($tmp_descr) > 1">
                        <xsl:variable name="tmp_descr2">
                            <xsl:for-each select="marc:datafield[@tag=520]">
                                <xsl:text>"</xsl:text>
                                    <xsl:variable name="cleanedquotes">
                                    <xsl:call-template name="replace-character">
                                        <xsl:with-param name="word" select="marc:subfield[@code='a']"/>
                                    </xsl:call-template>
                                </xsl:variable>
                                <xsl:value-of select="$cleanedquotes"/>
                                <xsl:text>",&#xd;&#xa;</xsl:text>
                            </xsl:for-each>
                            <xsl:for-each select="marc:datafield[@tag=521]">
                                <xsl:text>"</xsl:text>
                                    <xsl:variable name="cleanedquotes">
                                    <xsl:call-template name="replace-character">
                                        <xsl:with-param name="word" select="marc:subfield[@code='a']"/>
                                    </xsl:call-template>
                                </xsl:variable>
                                <xsl:value-of select="$cleanedquotes"/>
                                <xsl:text>",&#xd;&#xa;</xsl:text>
                            </xsl:for-each>
                            <xsl:for-each select="marc:datafield[500&lt;@tag][@tag&lt;=599][(not(@tag=506 or @tag=530 or @tag=540 or @tag=546 or @tag=520 or @tag=521)) and not(marc:subfield[@code='5'])]">
                                <xsl:text>"</xsl:text>
                                    <xsl:variable name="cleanedquotes">
                                    <xsl:call-template name="replace-character">
                                        <xsl:with-param name="word" select="marc:subfield[@code='a']"/>
                                    </xsl:call-template>
                                </xsl:variable>
                                <xsl:value-of select="$cleanedquotes"/>
                                <xsl:text>",&#xd;&#xa;</xsl:text>
                            </xsl:for-each>
                        </xsl:variable>
                        <xsl:text>"description": [&#xd;&#xa;</xsl:text>
                        <xsl:call-template name="chopPunctuation">
                            <xsl:with-param name="chopString" select="$tmp_descr2"/>
                            <xsl:with-param name="punctuation"><xsl:text>.:,;/ &#xd;&#xa;</xsl:text></xsl:with-param>
                        </xsl:call-template>
                        <xsl:text>&#xd;&#xa;],&#xd;&#xa;</xsl:text>
                    </xsl:when>
                </xsl:choose>

            <!-- SUBJECT -->
                <xsl:variable name="tmp_subjects">
                    <xsl:for-each select="marc:datafield[@tag=600]">
                        <xsl:text>1</xsl:text>
                    </xsl:for-each>
                    <xsl:for-each select="marc:datafield[@tag=610]">
                        <xsl:text>1</xsl:text>
                    </xsl:for-each>
                    <xsl:for-each select="marc:datafield[@tag=611]">
                        <xsl:text>1</xsl:text>
                    </xsl:for-each>
                    <xsl:for-each select="marc:datafield[@tag=630]">
                        <xsl:text>1</xsl:text>
                    </xsl:for-each>
                    <xsl:for-each select="marc:datafield[@tag=650]">
                        <xsl:text>1</xsl:text>
                    </xsl:for-each>
                    <xsl:for-each select="marc:datafield[@tag=653]">
                        <xsl:text>1</xsl:text>
                    </xsl:for-each>
                </xsl:variable>
                <xsl:choose>
                    <xsl:when test="string-length($tmp_subjects) = 1">
                        <xsl:for-each select="marc:datafield[@tag=600]">
                            <xsl:text>"subject": "</xsl:text>
                                <xsl:variable name="quotbaby">
                                    <xsl:call-template name="subfieldSelect">
                                        <xsl:with-param name="codes">abcdq</xsl:with-param>
                                    </xsl:call-template>
                                </xsl:variable>
                                <xsl:call-template name="replace-character">
                                    <xsl:with-param name="word" select="$quotbaby"/>
                                </xsl:call-template>
                            <xsl:text>",&#xd;&#xa;</xsl:text>
                        </xsl:for-each>
                        <xsl:for-each select="marc:datafield[@tag=610]">
                            <xsl:text>"subject": "</xsl:text>
                                <xsl:variable name="quotbaby">
                                    <xsl:call-template name="subfieldSelect">
                                        <xsl:with-param name="codes">abcdq</xsl:with-param>
                                    </xsl:call-template>
                                </xsl:variable>
                                <xsl:call-template name="replace-character">
                                    <xsl:with-param name="word" select="$quotbaby"/>
                                </xsl:call-template>
                            <xsl:text>",&#xd;&#xa;</xsl:text>
                        </xsl:for-each>
                        <xsl:for-each select="marc:datafield[@tag=611]">
                            <xsl:text>"subject": "</xsl:text>
                                <xsl:variable name="quotbaby">
                                    <xsl:call-template name="subfieldSelect">
                                        <xsl:with-param name="codes">abcdq</xsl:with-param>
                                    </xsl:call-template>
                                </xsl:variable>
                                <xsl:call-template name="replace-character">
                                    <xsl:with-param name="word" select="$quotbaby"/>
                                </xsl:call-template>
                            <xsl:text>",&#xd;&#xa;</xsl:text>
                        </xsl:for-each>
                        <xsl:for-each select="marc:datafield[@tag=630]">
                            <xsl:text>"subject": "</xsl:text>
                                <xsl:variable name="quotbaby">
                                    <xsl:call-template name="subfieldSelect">
                                        <xsl:with-param name="codes">abcdq</xsl:with-param>
                                    </xsl:call-template>
                                </xsl:variable>
                                <xsl:call-template name="replace-character">
                                    <xsl:with-param name="word" select="$quotbaby"/>
                                </xsl:call-template>
                            <xsl:text>",&#xd;&#xa;</xsl:text>
                        </xsl:for-each>
                        <xsl:for-each select="marc:datafield[@tag=650]">
                            <xsl:text>"subject": "</xsl:text>
                                <xsl:variable name="quotbaby">
                                    <xsl:call-template name="subfieldSelect">
                                        <xsl:with-param name="codes">abcdq</xsl:with-param>
                                    </xsl:call-template>
                                </xsl:variable>
                                <xsl:call-template name="replace-character">
                                    <xsl:with-param name="word" select="$quotbaby"/>
                                </xsl:call-template>
                            <xsl:text>",&#xd;&#xa;</xsl:text>
                        </xsl:for-each>
                        <xsl:for-each select="marc:datafield[@tag=653]">
                            <xsl:text>"subject": "</xsl:text>
                                <xsl:variable name="quotbaby">
                                    <xsl:call-template name="subfieldSelect">
                                        <xsl:with-param name="codes">abcdq</xsl:with-param>
                                    </xsl:call-template>
                                </xsl:variable>
                                <xsl:call-template name="replace-character">
                                    <xsl:with-param name="word" select="$quotbaby"/>
                                </xsl:call-template>
                            <xsl:text>",&#xd;&#xa;</xsl:text>
                        </xsl:for-each>
                    </xsl:when>
                    <xsl:when test="string-length($tmp_subjects) > 1">
                        <xsl:variable name="tmp_subjects2">
                            <xsl:for-each select="marc:datafield[@tag=600]">
                                <xsl:text>"</xsl:text>
                                <xsl:variable name="quotbaby">
                                    <xsl:call-template name="subfieldSelect">
                                        <xsl:with-param name="codes">abcdq</xsl:with-param>
                                    </xsl:call-template>
                                </xsl:variable>
                                <xsl:call-template name="replace-character">
                                    <xsl:with-param name="word" select="$quotbaby"/>
                                </xsl:call-template>
                                <xsl:text>",&#xd;&#xa;</xsl:text>
                            </xsl:for-each>
                            <xsl:for-each select="marc:datafield[@tag=610]">
                                <xsl:text>"</xsl:text>
                                    <xsl:variable name="quotbaby">
                                    <xsl:call-template name="subfieldSelect">
                                        <xsl:with-param name="codes">abcdq</xsl:with-param>
                                    </xsl:call-template>
                                    </xsl:variable>
                                    <xsl:call-template name="replace-character">
                                        <xsl:with-param name="word" select="$quotbaby"/>
                                    </xsl:call-template>
                                <xsl:text>",&#xd;&#xa;</xsl:text>
                            </xsl:for-each>
                            <xsl:for-each select="marc:datafield[@tag=611]">
                                <xsl:text>"</xsl:text>
                                    <xsl:variable name="quotbaby">
                                    <xsl:call-template name="subfieldSelect">
                                        <xsl:with-param name="codes">abcdq</xsl:with-param>
                                    </xsl:call-template>
                                    </xsl:variable>
                                    <xsl:call-template name="replace-character">
                                        <xsl:with-param name="word" select="$quotbaby"/>
                                    </xsl:call-template>
                                <xsl:text>",&#xd;&#xa;</xsl:text>
                            </xsl:for-each>
                            <xsl:for-each select="marc:datafield[@tag=630]">
                                <xsl:text>"</xsl:text>
                                    <xsl:variable name="quotbaby">
                                    <xsl:call-template name="subfieldSelect">
                                        <xsl:with-param name="codes">abcdq</xsl:with-param>
                                    </xsl:call-template>
                                    </xsl:variable>
                                    <xsl:call-template name="replace-character">
                                        <xsl:with-param name="word" select="$quotbaby"/>
                                    </xsl:call-template>
                                <xsl:text>",&#xd;&#xa;</xsl:text>
                            </xsl:for-each>
                            <xsl:for-each select="marc:datafield[@tag=650]">
                                <xsl:text>"</xsl:text>
                                    <xsl:variable name="quotbaby">
                                    <xsl:call-template name="subfieldSelect">
                                        <xsl:with-param name="codes">abcdq</xsl:with-param>
                                    </xsl:call-template>
                                    </xsl:variable>
                                    <xsl:call-template name="replace-character">
                                        <xsl:with-param name="word" select="$quotbaby"/>
                                    </xsl:call-template>
                                <xsl:text>",&#xd;&#xa;</xsl:text>
                            </xsl:for-each>
                            <xsl:for-each select="marc:datafield[@tag=653]">
                                <xsl:text>"</xsl:text>
                                    <xsl:variable name="quotbaby">
                                    <xsl:call-template name="subfieldSelect">
                                        <xsl:with-param name="codes">abcdq</xsl:with-param>
                                    </xsl:call-template>
                                    </xsl:variable>
                                    <xsl:call-template name="replace-character">
                                        <xsl:with-param name="word" select="$quotbaby"/>
                                    </xsl:call-template>
                                <xsl:text>",&#xd;&#xa;</xsl:text>
                            </xsl:for-each>
                        </xsl:variable>
                        <xsl:text>"subject": [&#xd;&#xa;</xsl:text>
                        <xsl:call-template name="chopPunctuation">
                            <xsl:with-param name="chopString" select="$tmp_subjects2"/>
                            <xsl:with-param name="punctuation"><xsl:text>.:,;/ &#xd;&#xa;</xsl:text></xsl:with-param>
                        </xsl:call-template>
                        <xsl:text>&#xd;&#xa;],&#xd;&#xa;</xsl:text>

                    </xsl:when>
                </xsl:choose>

                <!-- COVERAGE -->
                <xsl:choose>
                    <xsl:when test="count(marc:datafield[@tag=752]) = 1">
                        <xsl:for-each select="marc:datafield[@tag=752]">
                            <xsl:text>"coverage": "</xsl:text>
                                    <xsl:variable name="quotbaby">
                                    <xsl:call-template name="subfieldSelect">
                                        <xsl:with-param name="codes">abcd</xsl:with-param>
                                    </xsl:call-template>
                                    </xsl:variable>
                                    <xsl:call-template name="replace-character">
                                        <xsl:with-param name="word" select="$quotbaby"/>
                                    </xsl:call-template>
                            <xsl:text>",&#xd;&#xa;</xsl:text>
                        </xsl:for-each>
                    </xsl:when>
                    <xsl:when test="count(marc:datafield[@tag=752]) > 1">
                        <xsl:variable name="tmp_coverages">
                            <xsl:for-each select="marc:datafield[@tag=752]">
                                <xsl:text>"</xsl:text>
                                    <xsl:variable name="quotbaby">
                                    <xsl:call-template name="subfieldSelect">
                                        <xsl:with-param name="codes">abcd</xsl:with-param>
                                    </xsl:call-template>
                                    </xsl:variable>
                                    <xsl:call-template name="replace-character">
                                        <xsl:with-param name="word" select="$quotbaby"/>
                                    </xsl:call-template>
                                <xsl:text>",&#xd;&#xa;</xsl:text>
                            </xsl:for-each>
                        </xsl:variable>
                        <xsl:text>"coverage": [&#xd;&#xa;</xsl:text>
                        <xsl:call-template name="chopPunctuation">
                            <xsl:with-param name="chopString" select="$tmp_coverages"/>
                            <xsl:with-param name="punctuation"><xsl:text>.:,;/ &#xd;&#xa;</xsl:text></xsl:with-param>
                        </xsl:call-template>
                        <xsl:text>&#xd;&#xa;],&#xd;&#xa;</xsl:text>
                    </xsl:when>                   
                </xsl:choose>

                <!-- RELATION -->
                <xsl:variable name="tmp_relations">
                    <xsl:for-each select="marc:datafield[@tag=530]">
                        <xsl:text>1</xsl:text>
                    </xsl:for-each>
                    <xsl:for-each select="marc:datafield[@tag=760]|marc:datafield[@tag=762]|marc:datafield[@tag=765]|marc:datafield[@tag=767]|marc:datafield[@tag=770]|marc:datafield[@tag=772]|marc:datafield[@tag=773]|marc:datafield[@tag=774]|marc:datafield[@tag=775]|marc:datafield[@tag=776]|marc:datafield[@tag=777]|marc:datafield[@tag=780]|marc:datafield[@tag=785]|marc:datafield[@tag=786]|marc:datafield[@tag=787]">
                        <xsl:text>1</xsl:text>
                    </xsl:for-each> 
                </xsl:variable> 
                <xsl:choose>
                    <xsl:when test="string-length($tmp_relations) = 1">
                        <xsl:for-each select="marc:datafield[@tag=530]">
                            <xsl:text>"relation": "</xsl:text>
                                <xsl:text>",&#xd;&#xa;</xsl:text>
                        </xsl:for-each>
                        <xsl:for-each select="marc:datafield[@tag=760]|marc:datafield[@tag=762]|marc:datafield[@tag=765]|marc:datafield[@tag=767]|marc:datafield[@tag=770]|marc:datafield[@tag=772]|marc:datafield[@tag=773]|marc:datafield[@tag=774]|marc:datafield[@tag=775]|marc:datafield[@tag=776]|marc:datafield[@tag=777]|marc:datafield[@tag=780]|marc:datafield[@tag=785]|marc:datafield[@tag=786]|marc:datafield[@tag=787]">
                            <xsl:text>"relation": "</xsl:text>
                                <xsl:variable name="quotbaby">
                                    <xsl:call-template name="subfieldSelect">
                                        <xsl:with-param name="codes">ot</xsl:with-param>
                                    </xsl:call-template>
                                    </xsl:variable>
                                    <xsl:call-template name="replace-character">
                                        <xsl:with-param name="word" select="$quotbaby"/>
                                    </xsl:call-template>
                            <xsl:text>",&#xd;&#xa;</xsl:text>
                        </xsl:for-each>
                    </xsl:when>
                    <xsl:when test="string-length($tmp_relations) > 1">
                        <xsl:variable name="tmp_relations2">
                            <xsl:for-each select="marc:datafield[@tag=530]">
                                <xsl:text>"</xsl:text>
                                    <xsl:variable name="quotbaby">
                                    <xsl:call-template name="subfieldSelect">
                                        <xsl:with-param name="codes">abcdu</xsl:with-param>
                                    </xsl:call-template>
                                    </xsl:variable>
                                    <xsl:call-template name="replace-character">
                                        <xsl:with-param name="word" select="$quotbaby"/>
                                    </xsl:call-template>
                                <xsl:text>",&#xd;&#xa;</xsl:text>
                            </xsl:for-each>
                            <xsl:for-each select="marc:datafield[@tag=760]|marc:datafield[@tag=762]|marc:datafield[@tag=765]|marc:datafield[@tag=767]|marc:datafield[@tag=770]|marc:datafield[@tag=772]|marc:datafield[@tag=773]|marc:datafield[@tag=774]|marc:datafield[@tag=775]|marc:datafield[@tag=776]|marc:datafield[@tag=777]|marc:datafield[@tag=780]|marc:datafield[@tag=785]|marc:datafield[@tag=786]|marc:datafield[@tag=787]">
                                <xsl:text>"</xsl:text>
                                    <xsl:variable name="quotbaby">
                                    <xsl:call-template name="subfieldSelect">
                                        <xsl:with-param name="codes">ot</xsl:with-param>
                                    </xsl:call-template>
                                    </xsl:variable>
                                    <xsl:call-template name="replace-character">
                                        <xsl:with-param name="word" select="$quotbaby"/>
                                    </xsl:call-template>
                                <xsl:text>",&#xd;&#xa;</xsl:text>
                            </xsl:for-each>
                        </xsl:variable>
                        <xsl:text>"relation": [&#xd;&#xa;</xsl:text>
                         
                        <xsl:call-template name="chopPunctuation">
                            <xsl:with-param name="chopString" select="$tmp_relations2"/>
                            <xsl:with-param name="punctuation"><xsl:text>.:,;/ &#xd;&#xa;</xsl:text></xsl:with-param>
                        </xsl:call-template>
                        <xsl:text>&#xd;&#xa;],&#xd;&#xa;</xsl:text>
                    </xsl:when>
                </xsl:choose>
                
                <!-- CLASSIFICATION CODE -->
                <xsl:if test="marc:datafield[@tag = '050' and marc:subfield[@code = 'a'] and not(marc:subfield[@code = '5'])] or marc:datafield[@tag = '080' and marc:subfield[@code = 'a'] and not(marc:subfield[@code = '5'])] or marc:datafield[@tag = '082' and marc:subfield[@code = 'a'] and not(marc:subfield[@code = '5'])] or marc:datafield[@tag = '084' and marc:subfield[@code = 'a'] and not(marc:subfield[@code = '5']) and marc:subfield[@code = '2' and starts-with(.,'kssb')]]">
                   <xsl:text>"classification": {&#xd;&#xa;</xsl:text>
                   <xsl:if test="marc:datafield[@tag = '050' and marc:subfield[@code = 'a'] and not(marc:subfield[@code = '5'])]">
                        <xsl:text>"lcc": [&#xd;&#xa;</xsl:text>
                        <xsl:variable name="tmp_lcc">
                            <xsl:for-each select="marc:datafield[@tag = '050' and marc:subfield[@code = 'a'] and not(marc:subfield[@code = '5'])]/marc:subfield[@code = 'a']">
                                <xsl:text>"</xsl:text>
                                <xsl:call-template name="replace-character"><xsl:with-param name="word" select="."/></xsl:call-template><xsl:if test="../marc:subfield[@code = '2']"><xsl:text> [</xsl:text><xsl:value-of select="../marc:subfield[@code = '2']"/><xsl:text>]</xsl:text></xsl:if>
                                <xsl:text>",&#xd;&#xa;</xsl:text>
                            </xsl:for-each>
                        </xsl:variable>
                        <xsl:call-template name="chopPunctuation">
                            <xsl:with-param name="chopString" select="$tmp_lcc"/>
                            <xsl:with-param name="punctuation"><xsl:text>.:,;/ &#xd;&#xa;</xsl:text></xsl:with-param>
                        </xsl:call-template>
                        <xsl:choose>
                            <xsl:when test="marc:datafield[@tag = '080' and marc:subfield[@code = 'a'] and not(marc:subfield[@code = '5'])] or marc:datafield[@tag = '082' and marc:subfield[@code = 'a'] and not(marc:subfield[@code = '5'])] or marc:datafield[@tag = '084' and marc:subfield[@code = 'a'] and not(marc:subfield[@code = '5']) and marc:subfield[@code = '2' and starts-with(.,'kssb')]]"><xsl:text>&#xd;&#xa;],&#xd;&#xa;</xsl:text></xsl:when>
                            <xsl:otherwise><xsl:text>&#xd;&#xa;]&#xd;&#xa;</xsl:text></xsl:otherwise>
                        </xsl:choose>
                    </xsl:if>
                   <xsl:if test="marc:datafield[@tag = '080' and marc:subfield[@code = 'a'] and not(marc:subfield[@code = '5'])]">
                        <xsl:text>"udk": [&#xd;&#xa;</xsl:text>
                        <xsl:variable name="tmp_udk">
                            <xsl:for-each select="marc:datafield[@tag = '080' and marc:subfield[@code = 'a'] and not(marc:subfield[@code = '5'])]/marc:subfield[@code = 'a']">
                                <xsl:text>"</xsl:text>
                                <xsl:call-template name="replace-character"><xsl:with-param name="word" select="."/></xsl:call-template><xsl:if test="../marc:subfield[@code = '2']"><xsl:text> [</xsl:text><xsl:value-of select="../marc:subfield[@code = '2']"/><xsl:text>]</xsl:text></xsl:if>
                                <xsl:text>",&#xd;&#xa;</xsl:text>
                            </xsl:for-each>
                        </xsl:variable>
                        <xsl:call-template name="chopPunctuation">
                            <xsl:with-param name="chopString" select="$tmp_udk"/>
                            <xsl:with-param name="punctuation"><xsl:text>.:,;/ &#xd;&#xa;</xsl:text></xsl:with-param>
                        </xsl:call-template>
                        <xsl:choose>
                            <xsl:when test="marc:datafield[@tag = '082' and marc:subfield[@code = 'a'] and not(marc:subfield[@code = '5'])] or marc:datafield[@tag = '084' and marc:subfield[@code = 'a'] and not(marc:subfield[@code = '5']) and marc:subfield[@code = '2' and starts-with(.,'kssb')]]"><xsl:text>&#xd;&#xa;],&#xd;&#xa;</xsl:text></xsl:when>
                            <xsl:otherwise><xsl:text>&#xd;&#xa;]&#xd;&#xa;</xsl:text></xsl:otherwise>
                        </xsl:choose>
                    </xsl:if>
                    <xsl:if test="marc:datafield[@tag = '082' and marc:subfield[@code = 'a'] and not(marc:subfield[@code = '5'])]">
                        <xsl:text>"ddk": [&#xd;&#xa;</xsl:text>
                        <xsl:variable name="tmp_dewey">
                            <xsl:for-each select="marc:datafield[@tag = '082' and marc:subfield[@code = 'a'] and not(marc:subfield[@code = '5'])]/marc:subfield[@code = 'a']">
                                <xsl:text>"</xsl:text>
                                <xsl:value-of select="."/><xsl:if test="../marc:subfield[@code = '2']"><xsl:text> [</xsl:text><xsl:value-of select="../marc:subfield[@code = '2']"/><xsl:text>]</xsl:text></xsl:if>
                                <xsl:text>",&#xd;&#xa;</xsl:text>
                            </xsl:for-each>
                        </xsl:variable>
                        <xsl:call-template name="chopPunctuation">
                            <xsl:with-param name="chopString" select="$tmp_dewey"/>
                            <xsl:with-param name="punctuation"><xsl:text>.:,;/ &#xd;&#xa;</xsl:text></xsl:with-param>
                        </xsl:call-template>
                        <xsl:choose>
                            <xsl:when test="marc:datafield[@tag = '084' and marc:subfield[@code = 'a'] and not(marc:subfield[@code = '5']) and marc:subfield[@code = '2' and starts-with(.,'kssb')]]"><xsl:text>&#xd;&#xa;],&#xd;&#xa;</xsl:text></xsl:when>
                            <xsl:otherwise><xsl:text>&#xd;&#xa;]&#xd;&#xa;</xsl:text></xsl:otherwise>
                        </xsl:choose>
                    </xsl:if>
                <!--</xsl:choose>-->
                
                <!--<xsl:choose>
                    <xsl:when test="count(marc:datafield[@tag = '084' and marc:subfield[@code = 'a'] and not(marc:subfield[@code = '5']) and marc:subfield[@code = '2' and starts-with(.,'kssb')]]) = 1">
                        <xsl:for-each select="marc:datafield[@tag = '084' and marc:subfield[@code = 'a'] and not(marc:subfield[@code = '5']) and marc:subfield[@code = '2' and starts-with(.,'kssb')]]/marc:subfield[@code = 'a']">
                            <xsl:text>"sab": "</xsl:text>
                                <xsl:value-of select="."/>
                            <xsl:choose>
                                    <xsl:when test="position() = last()"><xsl:text>"&#xd;&#xa;</xsl:text></xsl:when>
                                    <xsl:otherwise><xsl:text>",&#xd;&#xa;</xsl:text></xsl:otherwise>
                            </xsl:choose>    
                        </xsl:for-each>
                    </xsl:when>-->
                    <xsl:if test="marc:datafield[@tag = '084' and marc:subfield[@code = 'a'] and not(marc:subfield[@code = '5']) and marc:subfield[@code = '2' and starts-with(.,'kssb')]]">
                        <xsl:text>"sab": [&#xd;&#xa;</xsl:text>
                        <xsl:variable name="tmp_sab">
                            <xsl:for-each select="marc:datafield[@tag = '084' and marc:subfield[@code = 'a'] and not(marc:subfield[@code = '5']) and marc:subfield[@code = '2' and starts-with(.,'kssb')]]/marc:subfield[@code = 'a']">
                                <xsl:text>"</xsl:text>
                                <xsl:value-of select="."/><xsl:if test="../marc:subfield[@code = '2']"><xsl:text> [</xsl:text><xsl:value-of select="../marc:subfield[@code = '2']"/><xsl:text>]</xsl:text></xsl:if>
                                <xsl:text>",&#xd;&#xa;</xsl:text>
                            </xsl:for-each>
                        </xsl:variable>
                        <xsl:call-template name="chopPunctuation">
                            <xsl:with-param name="chopString" select="$tmp_sab"/>
                            <xsl:with-param name="punctuation"><xsl:text>.:,;/ &#xd;&#xa;</xsl:text></xsl:with-param>
                        </xsl:call-template>
                        <xsl:text>&#xd;&#xa;]&#xd;&#xa;</xsl:text>
                    </xsl:if>
                <!--</xsl:choose>-->
                <xsl:choose>
                    <xsl:when test="marc:datafield[(@tag='506' or @tag='540') or @tag = '856' and starts-with(marc:subfield[@code = 'u'], 'http')
                    and (marc:subfield[@code='5'] or not(marc:subfield[@code='5' or @code='3']) and (@ind2='0' or @ind2='1'))]"><xsl:text>},&#xd;&#xa;</xsl:text></xsl:when>
                    <xsl:otherwise><xsl:text>}&#xd;&#xa;</xsl:text></xsl:otherwise>
                </xsl:choose>
            </xsl:if>

               <!-- URL -->
                <xsl:if test="marc:datafield[@tag = '856' and marc:subfield[@code='5'] and starts-with(marc:subfield[@code = 'u'], 'http')]">
                   <xsl:text>"urls": {&#xd;&#xa;</xsl:text>
                   <!-- contact[not(surname = preceding-sibling::contact/surname)] 
                        <xsl:apply-templates select="/records/contact[surname = current()/surname]" /> -->
                   <xsl:for-each select="marc:datafield[@tag = '856' and marc:subfield[@code='5'] and starts-with(marc:subfield[@code = 'u'], 'http')
                   and not(marc:subfield[@code='5'] = preceding-sibling::marc:datafield[@tag = '856' and marc:subfield[@code='5'] and starts-with(marc:subfield[@code = 'u'], 'http')]/marc:subfield[@code='5'])]">
                        <xsl:text>"</xsl:text><xsl:value-of select="marc:subfield[@code='5']"/><xsl:text>": [&#xd;&#xa;</xsl:text>
                        <xsl:for-each select="../marc:datafield[@tag = '856' and marc:subfield[@code='5'] and starts-with(marc:subfield[@code = 'u'], 'http') and marc:subfield[@code='5'] = current()/marc:subfield[@code='5']]">
                           <xsl:text>["</xsl:text><xsl:value-of select="marc:subfield[@code='u']"/><xsl:text>"</xsl:text>
                           <xsl:if test="marc:subfield[@code='z']">
                               <xsl:text>, [</xsl:text>
                               <xsl:for-each select="marc:subfield[@code='z']">
                                    <xsl:text>"</xsl:text><xsl:call-template name="replace-character"><xsl:with-param name="word" select="."/></xsl:call-template><xsl:text>"</xsl:text><xsl:if test="position() != last()"><xsl:text>, </xsl:text></xsl:if> 
                               </xsl:for-each>
                               <xsl:text>]</xsl:text>
                           </xsl:if>
                           <xsl:text>]</xsl:text><xsl:if test="position() != last()"><xsl:text>, </xsl:text></xsl:if><xsl:text>&#xd;&#xa;</xsl:text>
                       </xsl:for-each>
                       <xsl:text>]</xsl:text><xsl:if test="position() != last()"><xsl:text>, </xsl:text></xsl:if><xsl:text>&#xd;&#xa;</xsl:text>
                        
                   </xsl:for-each>
                   <!-- Key funkade inte när det var fler poster i träfflistan, av någon anledning. Ingen muenchian grouping alltså, buhu! -->
                   <!--<xsl:for-each select="marc:datafield[generate-id() = generate-id(key('sigel', marc:subfield[@code='5'])[1])]">
                       <xsl:text>"</xsl:text><xsl:value-of select="marc:subfield[@code='5']"/><xsl:text>":[&#xd;&#xa;</xsl:text>
                       <xsl:for-each select="key('sigel', marc:subfield[@code='5'])">
                           <xsl:text>["</xsl:text><xsl:value-of select="marc:subfield[@code='u']"/><xsl:text>"</xsl:text>
                           <xsl:if test="marc:subfield[@code='z']">
                               <xsl:text>, [</xsl:text>
                               <xsl:for-each select="marc:subfield[@code='z']">
                                    <xsl:text>"</xsl:text><xsl:value-of select="."/><xsl:text>"</xsl:text><xsl:if test="position() != last()"><xsl:text>, </xsl:text></xsl:if> 
                               </xsl:for-each>
                               <xsl:text>]</xsl:text>
                           </xsl:if>
                           <xsl:text>]</xsl:text><xsl:if test="position() != last()"><xsl:text>, </xsl:text></xsl:if><xsl:text>&#xd;&#xa;</xsl:text>
                       </xsl:for-each>
                       <xsl:text>]</xsl:text><xsl:if test="position() != last()"><xsl:text>, </xsl:text></xsl:if><xsl:text>&#xd;&#xa;</xsl:text>
                   </xsl:for-each>-->
                   <xsl:choose>
                        <xsl:when test="marc:datafield[(@tag='506' or @tag='540') or @tag = '856' and not(marc:subfield[@code='5' or @code='3']) and starts-with(marc:subfield[@code = 'u'], 'http') and (@ind2='0' or @ind2='1')]"><xsl:text>},&#xd;&#xa;</xsl:text></xsl:when>
                        <xsl:otherwise><xsl:text>}&#xd;&#xa;</xsl:text></xsl:otherwise>
                    </xsl:choose>
                   
                </xsl:if>
                
                <!-- FREE URL -->
                <xsl:if test="marc:datafield[@tag = '856' and not(marc:subfield[@code='5' or @code='3']) and starts-with(marc:subfield[@code = 'u'], 'http') and (@ind2='0' or @ind2='1')]">
                   <xsl:text>"free": [</xsl:text>
                   <xsl:for-each select="marc:datafield[@tag = '856' and not(marc:subfield[@code='5' or @code='3']) and starts-with(marc:subfield[@code = 'u'], 'http') and (@ind2='0' or @ind2='1')]">
                        <xsl:text>"</xsl:text><xsl:value-of select="marc:subfield[@code='u']"/><xsl:text>"</xsl:text>
                        <xsl:if test="marc:subfield[@code='z']">
                                <xsl:text>, [</xsl:text>
                                <xsl:for-each select="marc:subfield[@code='z']">
                                    <xsl:text>"</xsl:text><xsl:call-template name="replace-character"><xsl:with-param name="word" select="."/></xsl:call-template><xsl:text>"</xsl:text><xsl:if test="position() != last()"><xsl:text>, </xsl:text></xsl:if> 
                                </xsl:for-each>
                                <xsl:text>]</xsl:text>
                        </xsl:if>
                        <xsl:if test="position() != last()"><xsl:text>,&#xd;&#xa;</xsl:text></xsl:if>
                   </xsl:for-each>
                   <xsl:choose>
                        <xsl:when test="marc:datafield[@tag='506' or @tag='540']"><xsl:text>],&#xd;&#xa;</xsl:text></xsl:when>
                        <xsl:otherwise><xsl:text>]&#xd;&#xa;</xsl:text></xsl:otherwise>
                    </xsl:choose>
                                      
                </xsl:if>
                <!--<xsl:if test="count(marc:datafield[@tag = '856' and not(marc:subfield[@code='5' or @code='3']) and starts-with(marc:subfield[@code = 'u'], 'http') and (@ind2='0' or @ind2='1')]) > 1">
                   <xsl:text>"free": [&#xd;&#xa;</xsl:text>
                   <xsl:for-each select="marc:datafield[@tag = '856' and not(marc:subfield[@code='5' or @code='3']) and starts-with(marc:subfield[@code = 'u'], 'http') and (@ind2='0' or @ind2='1')]">
                        <xsl:text>["</xsl:text><xsl:value-of select="substring(marc:subfield[@code='u'], 1, 10)"/><xsl:text>"</xsl:text>
                           <xsl:if test="marc:subfield[@code='z']">
                               <xsl:text>, [</xsl:text>
                               <xsl:for-each select="marc:subfield[@code='z']">
                                    <xsl:text>"</xsl:text><xsl:value-of select="."/><xsl:text>"</xsl:text><xsl:if test="position() != last()"><xsl:text>, </xsl:text></xsl:if> 
                               </xsl:for-each>
                               <xsl:text>]</xsl:text>
                           </xsl:if>
                           <xsl:text>]</xsl:text><xsl:if test="position() != last()"><xsl:text>, </xsl:text></xsl:if><xsl:text>&#xd;&#xa;</xsl:text>
                   </xsl:for-each>
                   <xsl:if test="marc:datafield[@tag = '856' and not(marc:subfield[@code='5' or @code='3']) and starts-with(marc:subfield[@code = 'u'], 'http') and (@ind2='0' or @ind2='1')]/marc:subfield[@code='z']">
                        <xsl:text>, [</xsl:text>
                        <xsl:for-each select="marc:datafield[@tag = '856' and not(marc:subfield[@code='5' or @code='3']) and starts-with(marc:subfield[@code = 'u'], 'http') and (@ind2='0' or @ind2='1')]/marc:subfield[@code='z']">
                            <xsl:text>"</xsl:text><xsl:value-of select="."/><xsl:text>"</xsl:text><xsl:if test="position() != last()"><xsl:text>, </xsl:text></xsl:if> 
                        </xsl:for-each>
                        <xsl:text>]</xsl:text>
                   </xsl:if>
                   <xsl:text>&#xd;&#xa;</xsl:text>
              </xsl:if>-->
             
             <!-- RIGHTS -->
                <xsl:variable name="tmp_rights">
                    <xsl:for-each select="marc:datafield[@tag=506]">
                       <xsl:text>1</xsl:text>
                    </xsl:for-each>
                    <xsl:for-each select="marc:datafield[@tag=540]">
                        <xsl:text>1</xsl:text>
                    </xsl:for-each>
                </xsl:variable>
                <xsl:choose>
                    <xsl:when test="string-length($tmp_rights) = 1">
                        <xsl:for-each select="marc:datafield[@tag=506]">
                           <xsl:text>"rights": "</xsl:text>
                           <xsl:call-template name="replace-character">
                            <xsl:with-param name="word" select="marc:subfield[@code='a']"/>
                           </xsl:call-template>
                           <!--<xsl:value-of select="marc:subfield[@code='a']"/>-->
                           <xsl:text>",&#xd;&#xa;</xsl:text>
                        </xsl:for-each>
                        <xsl:for-each select="marc:datafield[@tag=540]">
                            <xsl:text>"rights": "</xsl:text>
                                <xsl:call-template name="replace-character">
                            <xsl:with-param name="word" select="marc:subfield[@code='a']"/>
                           </xsl:call-template>
                           <!--<xsl:value-of select="marc:subfield[@code='a']"/>-->
                            <xsl:text>",&#xd;&#xa;</xsl:text>
                        </xsl:for-each>
                    </xsl:when>
                    <xsl:when test="string-length($tmp_rights) > 1">   
                        <xsl:variable name="tmp_rights2">
                            <xsl:for-each select="marc:datafield[@tag=506]">
                               <xsl:text>"</xsl:text>
                                    <xsl:call-template name="replace-character">
                            <xsl:with-param name="word" select="marc:subfield[@code='a']"/>
                           </xsl:call-template>
                           <!--<xsl:value-of select="marc:subfield[@code='a']"/>-->
                               <xsl:text>",&#xd;&#xa;</xsl:text>
                            </xsl:for-each>
                            <xsl:for-each select="marc:datafield[@tag=540]">
                                <xsl:text>"</xsl:text>
                                    <xsl:call-template name="replace-character">
                            <xsl:with-param name="word" select="marc:subfield[@code='a']"/>
                           </xsl:call-template>
                           <!--<xsl:value-of select="marc:subfield[@code='a']"/>-->
                                <xsl:text>",&#xd;&#xa;</xsl:text>
                            </xsl:for-each>
                        </xsl:variable>
                        <xsl:text>"rights": [&#xd;&#xa;</xsl:text>
                        <xsl:call-template name="chopPunctuation">
                            <xsl:with-param name="chopString" select="$tmp_rights2"/>
                            <xsl:with-param name="punctuation"><xsl:text>.:,;/ &#xd;&#xa;</xsl:text></xsl:with-param>
                        </xsl:call-template>
                        <xsl:text>&#xd;&#xa;],&#xd;&#xa;</xsl:text>
                    </xsl:when>
                </xsl:choose>   
            </xsl:variable>
            
            <xsl:call-template name="chopPunctuation">
                    <!--<xsl:with-param name="chopString" select="normalize-space($list_record)"/>-->
                    <xsl:with-param name="chopString" select="$list_record"/>
                    <xsl:with-param name="punctuation"><xsl:text>.:,;/ &#xd;&#xa;</xsl:text></xsl:with-param>
            </xsl:call-template>
            <!--xsl:value-of select="$list_record-->
            
        <xsl:text>&#xd;&#xa;},&#xd;&#xa;</xsl:text>
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

    <!--
    <xsl:template name="quote_escape">
        <xsl:param name="string"/>
        <xsl:choose>
            <xsl:when test="contains($string, '&quot;')">
                <xsl:variable name="before" select="substring-before($string, '&quot;')"/>
                <xsl:variable name="after" select="substring-after($string, '&quot;')"/>
                <xsl:choose>
                    <xsl:when test="contains($after, '&quot;')">
                            <xsl:variable name="recursive">
                                <xsl:call-template name="quote_escape">
                                    <xsl:with-param name="string" select="$after"/>
                                </xsl:call-template>
                            </xsl:variable>
                            <xsl:value-of select="concat($before, '&quot;', $recursive)"/>
                    </xsl:when>
                    <xsl:otherwise>
                            <xsl:value-of select="concat($before, '\&quot;', $after)"/>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:when>
            <xsl:otherwise>
                    <xsl:value-of select="$string"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>-->

<!-- KP 160104 Escapes json controlchars -->
<!-- not proper, assumes that no jsonescaped chars are used, only '\' is escaped. -->
    <xsl:template name="json-esc">
        <xsl:param name="word"/>
        <xsl:variable name="this" select="'\'"/>
        <xsl:variable name="that" select="'\\'"/>
        <xsl:choose>
            <xsl:when test="contains($word,$this)">
                <xsl:value-of select="concat(substring-before($word,$this),$that)"/>
                <xsl:call-template name="json-esc">
                    <xsl:with-param name="word" select="substring-after($word,$this)"/>
                </xsl:call-template>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="$word"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

<!-- Fix strings -->
    <xsl:template name="replace-character">
        <xsl:param name="word"/>
		<xsl:variable name="escaped">
                <xsl:call-template name="json-esc">
                    <xsl:with-param name="word" select="$word"/>
                </xsl:call-template>
		</xsl:variable>
                <xsl:call-template name="replace-characters">
                    <xsl:with-param name="word" select="$escaped"/>
                </xsl:call-template>
		<!--<xsl:value-of select="$escaped"/>-->
    </xsl:template>

<!-- Escapes forbidden characters -->
    <xsl:template name="replace-characters">
        <xsl:param name="word"/>
        <xsl:variable name="replace_this" select="'&quot;'"/>
        <xsl:variable name="replace_with" select="'\&quot;'"/>
        <xsl:choose>
            <xsl:when test="contains($word,$replace_this)">
                <xsl:value-of select="concat(substring-before($word,$replace_this),$replace_with)"/>
                <xsl:call-template name="replace-characters">
                    <xsl:with-param name="word" select="substring-after($word,$replace_this)"/>
                </xsl:call-template>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="$word"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
            <!-- Denna harang kan vara bra att spara utifall behov uppkommer -->
            <!--<xsl:when test="$char != 'A' and $char != 'a' and $char != 'B' and $char != 'b' and $char != 'C' and $char != 'c' and $char != 'D' and $char != 'd' and $char != 'E' and $char != 'e' and $char != 'F' and $char != 'f' and $char != 'G' and $char != 'g'
             and $char != 'H' and $char != 'h' and $char != 'I' and $char != 'i' and $char != 'J' and $char != 'j' and $char != 'K' and $char != 'k' and $char != 'L' and $char != 'l' and $char != 'M' and $char != 'm' and $char != 'N' and $char != 'n'
             and $char != 'O' and $char != 'o' and $char != 'P' and $char != 'p' and $char != 'Q' and $char != 'q' and $char != 'R' and $char != 'r' and $char != 'S' and $char != 's' and $char != 'T' and $char != 't' and $char != 'U' and $char != 'u'
             and $char != 'V' and $char != 'v' and $char != 'W' and $char != 'w' and $char != 'X' and $char != 'x' and $char != 'Y' and $char != 'y' and $char != 'Z' and $char != 'z' and $char != '0' and $char != '1' and $char != '2' and $char != '3'
             and $char != '4' and $char != '5' and $char != '6' and $char != '7' and $char != '8' and $char != '9' and $char != '.' and $char != ',' and $char != ':' and $char != ';' and $char != '-' and $char != ' ' and $char != '/' and $char != '&#38;'
              and $char != '(' and $char != ')'"></xsl:when>-->
   
    <xsl:template name="chopPunctuation">
        <xsl:param name="chopString"/>
        <xsl:param name="punctuation"><xsl:text>.:,;/ &#xd;&#xa;</xsl:text></xsl:param>
         
        <!--<xsl:variable name="chopString_escaped">
            <xsl:call-template name="quote_escape">
                <xsl:with-param name="string" select="$chopString"/>
            </xsl:call-template>
        </xsl:variable>-->

        <xsl:variable name="length" select="string-length($chopString)"/>
        <!--
        length:<xsl:value-of select="$length"/>;substring:2-<xsl:value-of select="substring($chopString,$length - 2,1)"/>-2;1-<xsl:value-of select="substring($chopString,$length - 1,1)"/>-1;0-<xsl:value-of select="substring($chopString,$length,1)"/>-0.
        -->
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
    
    <xsl:template match="marc:subfield[@code='a' or @code='z']" mode="normalizeIsbn">
        <xsl:variable name="tmp" select="normalize-space(translate(., '-', ''))"/>
        <xsl:choose>
            <xsl:when test="contains($tmp, '(')"><xsl:value-of select="normalize-space(substring-before($tmp, '('))"/></xsl:when>
            <xsl:otherwise><xsl:value-of select="$tmp"/></xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    
    <xsl:template match="marc:subfield[@code='a']" mode="normalizeIssn">
        <xsl:variable name="tmp" select="normalize-space(.)"/>
        <xsl:choose>
            <xsl:when test="contains($tmp, '(')"><xsl:value-of select="normalize-space(substring-before($tmp, '('))"/></xsl:when>
            <xsl:otherwise><xsl:value-of select="$tmp"/></xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    
    <xsl:template match="*|@*|text()">
        <xsl:copy>
            <xsl:apply-templates select="*|@*|text()"/>
        </xsl:copy>
    </xsl:template>
</xsl:stylesheet><!-- Stylus Studio meta-information - (c)1998-2002 eXcelon Corp.
<metaInformation>
<scenarios ><scenario default="no" name="MODS Website Samples" userelativepaths="yes" externalpreview="no" url="..\xml\MARC21slim\modswebsitesamples.xml" htmlbaseurl="" outputurl="" processortype="internal" commandline="" additionalpath="" additionalclasspath="" postprocessortype="none" postprocesscommandline="" postprocessadditionalpath="" postprocessgeneratedext=""/><scenario default="no" name="Ray Charles" userelativepaths="yes" externalpreview="no" url="..\xml\MARC21slim\raycharles.xml" htmlbaseurl="" outputurl="" processortype="internal" commandline="" additionalpath="" additionalclasspath="" postprocessortype="none" postprocesscommandline="" postprocessadditionalpath="" postprocessgeneratedext=""/><scenario default="yes" name="s6" userelativepaths="yes" externalpreview="no" url="..\ifla\sally6.xml" htmlbaseurl="" outputurl="" processortype="internal" commandline="" additionalpath="" additionalclasspath="" postprocessortype="none" postprocesscommandline="" postprocessadditionalpath="" postprocessgeneratedext=""/><scenario default="no" name="s7" userelativepaths="yes" externalpreview="no" url="..\ifla\sally7.xml" htmlbaseurl="" outputurl="" processortype="internal" commandline="" additionalpath="" additionalclasspath="" postprocessortype="none" postprocesscommandline="" postprocessadditionalpath="" postprocessgeneratedext=""/><scenario default="no" name="s12" userelativepaths="yes" externalpreview="no" url="..\ifla\sally12.xml" htmlbaseurl="" outputurl="" processortype="internal" commandline="" additionalpath="" additionalclasspath="" postprocessortype="none" postprocesscommandline="" postprocessadditionalpath="" postprocessgeneratedext=""/></scenarios><MapperInfo srcSchemaPath="" srcSchemaRoot="" srcSchemaPathIsRelative="yes" srcSchemaInterpretAsXML="no" destSchemaPath="" destSchemaRoot="" destSchemaPathIsRelative="yes" destSchemaInterpretAsXML="no"/>
</metaInformation>
-->
