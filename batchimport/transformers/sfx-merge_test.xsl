<?xml version="1.0" encoding="UTF-8" ?>
<xsl:stylesheet version="1.0"
	xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
	xmlns:marc="http://www.loc.gov/MARC21/slim">
    <xsl:output method="xml" omit-xml-declaration="no" indent="yes"/>
    
    <xsl:template match="/">
        <xsl:apply-templates select="merge"/>
    </xsl:template>
    
    <xsl:template match="merge">
        <collection xmlns="http://www.loc.gov/MARC21/slim">
            <record type="Bibliographic">
                <xsl:variable name="leader"><xsl:value-of select="old_record/marc:record[@type='Bibliographic']/marc:leader"/></xsl:variable>
                <xsl:copy-of select="old_record/marc:record[@type='Bibliographic']/marc:leader"/>
                <xsl:copy-of select="old_record/marc:record[@type='Bibliographic']/marc:controlfield"/>
                <xsl:for-each select="old_record/marc:record[@type='Bibliographic']/marc:datafield">
                    <xsl:sort select="@tag"/>
                    <xsl:choose>
                        <xsl:when test="@tag='022' or @tag='222' or @tag='245' or @tag='776'">
                            <xsl:choose>
                                <xsl:when test="substring($leader,18,1)='5'">
                                    <xsl:variable name="tag" select="@tag"/>
                                    <xsl:if test="count(preceding-sibling::marc:datafield[@tag = $tag])=0">
                                        <xsl:for-each select="../../../new_record/marc:record/marc:datafield[@tag = $tag]">
                                            <xsl:copy-of select="."/>  
                                        </xsl:for-each>
                                    </xsl:if>
                                </xsl:when>
                                <xsl:otherwise>
                                    <xsl:copy-of select="."/>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:when>
                        
                        <xsl:when test="@tag='040'">
                            <xsl:choose>
                                <xsl:when test="count(../../../new_record/marc:record/marc:datafield[@tag='040']/marc:subfield[@code='9'])=0 and count(marc:subfield[@code='9'])=1 and count(marc:subfield)=1"/>
                                <xsl:otherwise>
                                    <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
                                        <xsl:for-each select="marc:subfield">
                                            <xsl:choose>
                                                <xsl:when test="@code='9'">
                                                    <xsl:if test="count(preceding-sibling::marc:subfield[@code='9'])=0">
                                                        <xsl:for-each select="../../../../new_record/marc:record/marc:datafield[@tag='040']/marc:subfield[@code='9']">
                                                            <xsl:copy-of select="."/>  
                                                        </xsl:for-each>
                                                    </xsl:if>
                                                </xsl:when>
                                                <xsl:otherwise>
                                                    <xsl:copy-of select="."/>  
                                                </xsl:otherwise>
                                            </xsl:choose>
                                        </xsl:for-each>
                                    </datafield>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:when>
                                
                        <xsl:when test="@tag = '650'">
                            <xsl:choose>
                                <xsl:when test="count(marc:subfield[@code = '2'][. = 'sfxc' or . = 'SFXC']) > 0">
                                    <xsl:if test="count(preceding-sibling::marc:datafield[@tag='650']/marc:subfield[@code='2'][. = 'sfxc' or . = 'SFXC'])=0">
                                        <xsl:for-each select="../../../new_record/marc:record/marc:datafield[@tag='650']">
                                            <xsl:if test="count(marc:subfield[@code='2'][. = 'sfxc' or . = 'SFXC'])&gt;0">
                                                <xsl:copy-of select="."/>  
                                            </xsl:if>
                                        </xsl:for-each>
                                    </xsl:if>
                                </xsl:when>
                                <xsl:otherwise>
                                    <xsl:copy-of select="."/>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:when>
                                                             
                        <xsl:when test="@tag='856'">
                            <xsl:if test="count(../../../new_record/marc:record/marc:datafield[@tag='040']/marc:subfield[@code='9'][. = 'FREE' or . = 'free'])&gt;0">
                                <xsl:if test="count(preceding-sibling::marc:datafield[@tag='856'])=0">
                                    <xsl:for-each select="../../../new_record/marc:record/datafield[@tag='856']">
                                        <xsl:copy-of select="."/>  
                                    </xsl:for-each>
                                </xsl:if>
                            </xsl:if>
                            <xsl:if test="count(../../../new_record/marc:record/marc:datafield[@tag='040']/marc:subfield[@code='9'][. = 'AVTL' or . = 'avtl'])&gt;0">
                                <xsl:copy-of select="."/>
                            </xsl:if>
                        </xsl:when>
                       
                        <xsl:otherwise>
                            <xsl:copy-of select="."/>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:for-each>
                
                <xsl:copy-of select="new_record/marc:record/marc:datafield[@tag = '650']"/>
            </record>
<!--
            <xsl:for-each select="new_record/record[@type='Holdings']">
                <xsl:variable name="sfxcodeb" select="datafield/subfield[@code='b']"/>
                <xsl:variable name="leader05"><xsl:value-of select="normalize-space(leader)"/></xsl:variable>
                <xsl:variable name="sfxpos" select="position()"/>
                <xsl:for-each select="../../old_record/record[@type='Holdings']">
                    <xsl:variable name="libcodeb" select="datafield/subfield[@code='b']"/>
                    
                    <xsl:if test="$libcodeb = $sfxcodeb">
                        <xsl:if test="substring($leader05,6,1)!='d'">
                            <xsl:copy-of select="../../new_record/record[@type='Holdings'][$sfxpos]"/>
                        </xsl:if>
                    </xsl:if>
                    
                    <xsl:if test="$sfxpos = '1'">
                        <xsl:if test="not($libcodeb) or count(../../new_record/record[@type='Holdings']/datafield[@tag='852']/subfield[@code='b'][. = $libcodeb])=0">
                            <xsl:copy-of select="."/>
                        </xsl:if>
                    
                    </xsl:if>
                    
                </xsl:for-each>
                
                <xsl:if test="count(../../old_record/record[@type='Holdings']/datafield[@tag='852']/subfield[@code='b'][. = $sfxcodeb])=0">
                    <xsl:if test="substring($leader05,6,1)!='d' and $sfxcodeb">
                        <xsl:copy-of select="."/>
                    </xsl:if>
                </xsl:if>
            </xsl:for-each>
-->
        </collection>
    </xsl:template>
</xsl:stylesheet>
