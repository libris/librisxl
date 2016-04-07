<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:marc="http://www.loc.gov/MARC21/slim"
                exclude-result-prefixes="marc">
    <xsl:output method="xml" omit-xml-declaration="no" indent="yes"/>
    
    <xsl:template match="/">
        <xsl:apply-templates select="merge"/>
    </xsl:template>
    
    <xsl:template match="merge">
        <collection xmlns="http://www.loc.gov/MARC21/slim">
            <xsl:choose>
                <xsl:when test="old_record/marc:record[@type='Bibliographic' and marc:datafield[@tag = '599' and marc:subfield[@code = 'a' and contains(normalize-space(.), 'Dawson. Upgraded') or contains(normalize-space(.), 'Dawson. Updated')]]]">
                    <xsl:copy-of select="new_record/marc:record[@type='Bibliographic']"/>
                </xsl:when>           
                <xsl:otherwise>
                    <record xmlns="http://www.loc.gov/MARC21/slim" type="Bibliographic">
                    <xsl:variable name="leader"><xsl:value-of select="old_record/marc:record[@type='Bibliographic']/marc:leader"/></xsl:variable>
                        <xsl:variable name="leader17" select="substring($leader,18,1)"/>

                        <xsl:for-each select="old_record/marc:record[@type='Bibliographic']/marc:leader">
                            <leader><xsl:value-of select="."/></leader>
                        </xsl:for-each>

                        <xsl:for-each select="old_record/marc:record[@type='Bibliographic']/marc:controlfield">
                            <controlfield tag="{@tag}"><xsl:value-of select="."/></controlfield>
                        </xsl:for-each>

                        <!-- datafield tag < 080 -->   
                        <xsl:apply-templates select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag &lt; '080']" mode="copying-datafields">
                            <xsl:sort select="@tag"/>
                        </xsl:apply-templates>

                        <!-- datafield tag = 080 -->
                        <xsl:apply-templates select="new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '080']" mode="copying-datafields"/>

                        <xsl:for-each select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '080']">
                            <xsl:call-template name="doubletCheck">
                                <xsl:with-param name="compField" select="../../../new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag='080']"/>
                                <xsl:with-param name="isInUse" select="'false'"/>
                            </xsl:call-template>
                        </xsl:for-each>

                        <!-- datafield tag = 082 -->
                        <xsl:apply-templates select="new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '082']" mode="copying-datafields"/>

                        <xsl:for-each select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '082']">
                            <xsl:call-template name="doubletCheck">
                                <xsl:with-param name="compField" select="../../../new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag='082']"/>
                                <xsl:with-param name="isInUse" select="'false'"/>
                            </xsl:call-template>
                        </xsl:for-each>

                        <!-- datafield tag = 084 -->
                        <xsl:apply-templates select="new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '084']" mode="copying-datafields"/>

                        <xsl:for-each select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '084']">
                            <xsl:call-template name="doubletCheck">
                                <xsl:with-param name="compField" select="../../../new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag='084']"/>
                                <xsl:with-param name="isInUse" select="'false'"/>
                            </xsl:call-template>
                        </xsl:for-each>

                        <!-- datafield tag = 085 - tag = 099 transformation -->   
                        <xsl:apply-templates select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag > '084' and @tag &lt; '100']" mode="copying-datafields">
                            <xsl:sort select="@tag"/>
                        </xsl:apply-templates>

                        <!-- datafield 100 merge -->
                        <xsl:for-each select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '100']">
                            <xsl:call-template name="authorityMatch">
                                <xsl:with-param name="newDatafield" select="../../../new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag='886']"/>
                                <xsl:with-param name="used" select="'false'"/>
                            </xsl:call-template>
                        </xsl:for-each>

                        <!-- datafield 110 merge -->                                
                        <xsl:for-each select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '110']">
                            <xsl:call-template name="authorityMatch">
                                <xsl:with-param name="newDatafield" select="../../../new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag='886']"/>
                                <xsl:with-param name="used" select="'false'"/>
                            </xsl:call-template>
                        </xsl:for-each>

                        <!-- datafield 111 merge -->                        
                        <xsl:for-each select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '111']">
                            <xsl:call-template name="authorityMatch">
                                <xsl:with-param name="newDatafield" select="../../../new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag='886']"/>
                                <xsl:with-param name="used" select="'false'"/>
                            </xsl:call-template>
                        </xsl:for-each>

                        <!-- datafield 130 merge -->                
                        <xsl:for-each select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '130']">
                            <xsl:call-template name="authorityMatch">
                                <xsl:with-param name="newDatafield" select="../../../new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag='886']"/>
                                <xsl:with-param name="used" select="'false'"/>
                            </xsl:call-template>
                        </xsl:for-each>

                        <!-- datafield tag = 131 - tag = 239 transformation -->   
                        <xsl:apply-templates select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag > '130' and @tag &lt; '240']" mode="copying-datafields">
                            <xsl:sort select="@tag"/>
                        </xsl:apply-templates>

                        <!-- datafield 240 merge -->               
                        <xsl:for-each select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '240']">
                            <xsl:call-template name="authorityMatch">
                                <xsl:with-param name="newDatafield" select="../../../new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag='886']"/>
                                <xsl:with-param name="used" select="'false'"/>
                            </xsl:call-template>
                        </xsl:for-each>

                        <!-- datafield tag = 241 - tag = 259 transformation -->   
                        <xsl:apply-templates select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag > '240' and @tag &lt; '260']" mode="copying-datafields">
                            <xsl:sort select="@tag"/>
                        </xsl:apply-templates>

                        <!-- Ändrat 12 juni 2007 -->
                        <!-- datafield 260 merge -->
                        <xsl:apply-templates select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '260']" mode="copying-datafields"/>
                        <xsl:if test="count(old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '260']) = 0 and $leader17 !=' ' and $leader17 !='1' and $leader17 !='2' and $leader17 !='3' and $leader17 !='7'">
                            <xsl:apply-templates select="new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '260']" mode="copying-datafields"/>
                        </xsl:if>
                        <!-- Slut ändrat -->

                        <!-- datafield tag = 261 - tag = 299 transformation -->   
                        <xsl:apply-templates select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag > '260' and @tag &lt; '300']" mode="copying-datafields">
                            <xsl:sort select="@tag"/>
                        </xsl:apply-templates>

                        <!-- Ändrat 12 juni 2007 -->
                        <!-- datafield 300 merge -->
                        <xsl:apply-templates select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '300']" mode="copying-datafields"/>
                        <xsl:if test="count(old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '300']) = 0 and $leader17 !=' ' and $leader17 !='1' and $leader17 !='2' and $leader17 !='3' and $leader17 !='7'">
                            <xsl:apply-templates select="new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '300']" mode="copying-datafields"/>
                        </xsl:if>
                        <!-- Slut ändrat -->

                        <!-- datafield tag = 301 - tag = 599 transformation -->   
                        <xsl:apply-templates select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag > '300' and @tag &lt; '600']" mode="copying-datafields">
                            <xsl:sort select="@tag"/>
                        </xsl:apply-templates>

                        <!-- datafield tag = 599 transformation -->
                        <xsl:if test="count(new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '599'][marc:subfield[@code='a'][. = 'Dawson']]) > 0 and count(old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '599'][marc:subfield[@code='a'][. = 'Dawson']]) = 0">
                            <xsl:apply-templates select="new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '599'][marc:subfield[@code='a'][. = 'Dawson']]" mode="copying-datafields"/>
                        </xsl:if>

                        <!-- datafield 600 merge -->
                        <xsl:for-each select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '600']">
                            <xsl:call-template name="authorityMatch">
                                <xsl:with-param name="newDatafield" select="../../../new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag='886']"/>
                                <xsl:with-param name="used" select="'false'"/>
                            </xsl:call-template>
                        </xsl:for-each>

                        <!-- datafield 610 merge -->                
                        <xsl:for-each select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '610']">
                            <xsl:call-template name="authorityMatch">
                                <xsl:with-param name="newDatafield" select="../../../new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag='886']"/>
                                <xsl:with-param name="used" select="'false'"/>
                            </xsl:call-template>
                        </xsl:for-each>

                        <!-- datafield 611 merge -->                
                        <xsl:for-each select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '611']">
                            <xsl:call-template name="authorityMatch">
                                <xsl:with-param name="newDatafield" select="../../../new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag='886']"/>
                                <xsl:with-param name="used" select="'false'"/>
                            </xsl:call-template>
                        </xsl:for-each>

                        <!-- datafield 630 merge -->
                        <xsl:for-each select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '630']">
                            <xsl:call-template name="authorityMatch">
                                <xsl:with-param name="newDatafield" select="../../../new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag='886']"/>
                                <xsl:with-param name="used" select="'false'"/>
                            </xsl:call-template>
                        </xsl:for-each>

                        <!-- datafield tag = 631 - tag = 649 transformation -->   
                        <xsl:apply-templates select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag > '630' and @tag &lt; '650']" mode="copying-datafields">
                            <xsl:sort select="@tag"/>
                        </xsl:apply-templates>

                        <!-- datafield 650 merge -->
                        <xsl:apply-templates select="new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '650']" mode="copying-datafields"/>

                        <xsl:for-each select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '650']">
                            <xsl:call-template name="doubletCheck">
                                <xsl:with-param name="compField" select="../../../new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag='650']"/>
                                <xsl:with-param name="isInUse" select="'false'"/>
                            </xsl:call-template>
                        </xsl:for-each>

                        <!-- datafield tag = 651 - tag = 699 transformation -->   
                        <xsl:apply-templates select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag > '650' and @tag &lt; '700']" mode="copying-datafields">
                            <xsl:sort select="@tag"/>
                        </xsl:apply-templates>

                        <!-- datafield 700 merge -->
                        <xsl:for-each select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '700']">
                            <xsl:call-template name="authorityMatch">
                                <xsl:with-param name="newDatafield" select="../../../new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag='886']"/>
                                <xsl:with-param name="used" select="'false'"/>
                            </xsl:call-template>
                        </xsl:for-each>

                        <!-- datafield 710 merge -->
                        <xsl:for-each select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '710']">
                            <xsl:call-template name="authorityMatch">
                                <xsl:with-param name="newDatafield" select="../../../new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag='886']"/>
                                <xsl:with-param name="used" select="'false'"/>
                            </xsl:call-template>
                        </xsl:for-each>

                        <!-- datafield 711 merge -->
                        <xsl:for-each select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '711']">
                            <xsl:call-template name="authorityMatch">
                                <xsl:with-param name="newDatafield" select="../../../new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag='886']"/>
                                <xsl:with-param name="used" select="'false'"/>
                            </xsl:call-template>
                        </xsl:for-each>

                        <!-- datafield 730 merge -->
                        <xsl:for-each select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag = '730']">
                            <xsl:call-template name="authorityMatch">
                                <xsl:with-param name="newDatafield" select="../../../new_record/marc:record[@type='Bibliographic']/marc:datafield[@tag='886']"/>
                                <xsl:with-param name="used" select="'false'"/>
                            </xsl:call-template>
                        </xsl:for-each>

                        <!-- datafield tag = 731 - tag = 998 transformation -->   
                        <xsl:apply-templates select="old_record/marc:record[@type='Bibliographic']/marc:datafield[@tag > '730' and @tag &lt; '999']" mode="copying-datafields">
                            <xsl:sort select="@tag"/>
                        </xsl:apply-templates>

                    </record>
                </xsl:otherwise>
            </xsl:choose>
            
            <!--<xsl:for-each select="new_record/marc:record[@type='Holdings']">
                <record type="Holdings">
                    <xsl:for-each select="marc:datafield">
                        <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
                            <xsl:for-each select="marc:subfield">
                                <subfield code="{@code}">
                                    <xsl:value-of select="normalize-space(.)"/>
                                </subfield>
                            </xsl:for-each>
                        </datafield>
                    </xsl:for-each>
                </record>
            </xsl:for-each>
            
            <xsl:variable name="sigel" select="new_record/marc:record[@type='Holdings']/marc:datafield[@tag='852']/marc:subfield[@code='b']"/>
            <xsl:for-each select="old_record/marc:record[@type='Holdings']">        
            <xsl:if test="marc:datafield[@tag='852']/marc:subfield[@code='b'] != $sigel">
            <xsl:copy-of select="."/>
            </xsl:if>
            </xsl:for-each>-->
            </collection>
    </xsl:template>
    
    
    <!-- Copying datafields - general rules -->
    <xsl:template match="*" mode="copying-datafields">
        <xsl:if test="count(marc:subfield[normalize-space(.)]) > '0'">
            <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
                <xsl:for-each select="marc:subfield">
                    <subfield code="{@code}">
                        <xsl:value-of select="normalize-space(.)"/>
                    </subfield>
                </xsl:for-each>
            </datafield>
        </xsl:if>
    </xsl:template>
    
    <!-- Copying datafields - check for two a's rules -->
    <xsl:template match="marc:datafield" mode="check-a-subfields">
        <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
            <xsl:choose>
                <xsl:when test="count(marc:subfield[@code = 'a']) > 1">
                    <xsl:for-each select="marc:subfield[@code = 'a'][2]/preceding-sibling::marc:subfield[normalize-space(.)]">
                        <subfield code="{@code}"><xsl:value-of select="."/></subfield>
                    </xsl:for-each>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:for-each select="marc:subfield">
                        <subfield code="{@code}"><xsl:value-of select="."/></subfield>
                    </xsl:for-each>
                </xsl:otherwise>
            </xsl:choose>
        </datafield>
    </xsl:template>
    
    <!-- Datafields tag = 080, 082, 084, 650 -->
    <xsl:template name="doubletCheck">
        <xsl:param name="compField"/>
        <xsl:param name="isInUse"/>
        
        <!-- If new record with same tag exists, compare subfields without punctuation signs and store eventual mismatch in variable $compare -->
        <xsl:if test="$isInUse='false'">
            <xsl:choose>
                <xsl:when test="$compField">
                    <xsl:variable name="compSubfields" select="$compField[1]/marc:subfield"/>
                    <xsl:variable name="compare">
                    <xsl:variable name="mTest"/>
                        <xsl:for-each select="marc:subfield">
                            <xsl:variable name="pos" select="position()"/>
                            <xsl:variable name="temp1" select="normalize-space(.)"/>
                            <xsl:variable name="temp2" select="translate($temp1,'.','')"/>
                            <xsl:variable name="temp3" select="translate($temp2,',','')"/>
                            <xsl:variable name="compTemp1" select="normalize-space($compSubfields[$pos])"/>
                            <xsl:variable name="compTemp2" select="translate($compTemp1,'.','')"/>
                            <xsl:variable name="compTemp3" select="translate($compTemp2,',','')"/>
                            <xsl:if test="$temp3 != $compTemp3 or @code != $compSubfields[$pos]/@code">
                                <xsl:value-of select="concat($mTest,'noMatch')"/>
                            </xsl:if>
                        </xsl:for-each>
                    </xsl:variable>
                    <!-- If the concatenated variable contains the string 'noMatch', the node sets are unequal. In that case, 
                    output the content of old datafield. -->
                    <xsl:choose> 
                        <xsl:when test="contains($compare,'noMatch')">
                            <xsl:choose>
                                <xsl:when test="count($compField[2]) = 0">
                                    <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
                                        <xsl:for-each select="marc:subfield[normalize-space(.)]">
                                            <subfield code="{@code}"><xsl:value-of select="."/></subfield>
                                        </xsl:for-each>
                                    </datafield>
                                </xsl:when>
                                <xsl:otherwise>
                                    <xsl:call-template name="doubletCheck">
                                        <xsl:with-param name="compField" select="$compField[position() > 1]"/>
                                        <xsl:with-param name="isInUse" select="'false'"/>
                                    </xsl:call-template>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:when>
                        
                        <!-- Otherwise, call template recursively if there are more new datafields -->
                        <xsl:otherwise>
                            <xsl:if test="count($compField[2]) > 0">
                                <xsl:call-template name="doubletCheck">
                                    <xsl:with-param name="compField" select="$compField[position() > 1]"/>
                                    <xsl:with-param name="isInUse" select="'true'"/>
                                </xsl:call-template>
                            </xsl:if>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:when>
                
                <!-- If there is no new datafield at all, output old datafield -->
                <xsl:otherwise>
                    <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
                        <xsl:for-each select="marc:subfield[normalize-space(.)]">
                            <subfield code="{@code}"><xsl:value-of select="."/></subfield>
                        </xsl:for-each>
                    </datafield>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:if>
    </xsl:template>   
    
    
    <!-- Datafields tag = 100, 110, 111, 130, 240, 600, 610, 611, 630, 700, 710, 711, 730  -->
    <xsl:template name="authorityMatch">
        <xsl:param name="newDatafield"/>
        <xsl:param name="used"/>
        
        <xsl:if test="$used='false'">
            <!-- If new record with same tag exists and has two subfields @a, compare subfields without punctuation signs and store eventual mismatch in variable $comp -->
            <xsl:choose>
                <xsl:when test="$newDatafield">
                    <xsl:choose>
                        <xsl:when test="count($newDatafield[1]/marc:subfield[@code = 'a']) > 1">
                            <xsl:variable name="oldSubfields" select="marc:subfield"/>
                            <xsl:variable name="comp">
                            <xsl:variable name="matchTest"/>
                                <xsl:for-each select="$newDatafield[1]/marc:subfield[@code = 'a'][3] | $newDatafield[1]/marc:subfield[@code = 'a'][3]/following-sibling::marc:subfield">
                                    <xsl:variable name="pos" select="position()"/>
                                    <xsl:variable name="compTemp1" select="normalize-space(.)"/>
                                    <xsl:variable name="compTemp2" select="translate($compTemp1,'.','')"/>
                                    <xsl:variable name="compTemp3" select="translate($compTemp2,',','')"/>
                                    <xsl:variable name="shortTemp1" select="normalize-space($oldSubfields[$pos])"/>
                                    <xsl:variable name="shortTemp2" select="translate($shortTemp1,'.','')"/>
                                    <xsl:variable name="shortTemp3" select="translate($shortTemp2,',','')"/>
                                    <xsl:if test="$compTemp3 != $shortTemp3 or /@code != marc:subfield[$pos]/@code">
                                        <xsl:value-of select="concat($matchTest,'noMatch')"/>
                                    </xsl:if>
                                </xsl:for-each>
                                <xsl:if test="$newDatafield[1]/marc:subfield[@code = 'a'][1] != @tag">
                                    <xsl:value-of select="concat($matchTest,'noMatch')"/>
                                </xsl:if>
                            </xsl:variable>
                            
                            <!-- If the concatenated variable contains the string 'noMatch', the node sets are unequal. In that case, 
                            output the content of old datafield. -->
                            <!--<xsl:if test="@tag = '730'">
                            <testa1><xsl:value-of select="$comp"/></testa1>
                            </xsl:if>-->
                            <xsl:choose> 
                                <xsl:when test="contains($comp,'noMatch')">
                                    <xsl:choose>
                                        <xsl:when test="count($newDatafield[2]) = 0">
                                            <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
                                                <xsl:for-each select="marc:subfield[normalize-space(.)]">
                                                    <subfield code="{@code}"><xsl:value-of select="."/></subfield>
                                                </xsl:for-each>
                                            </datafield>
                                        </xsl:when>
                                        <xsl:otherwise>
                                            <xsl:call-template name="authorityMatch">
                                                <xsl:with-param name="newDatafield" select="$newDatafield[position() > 1]"/>
                                                <xsl:with-param name="used" select="'false'"/>
                                            </xsl:call-template>
                                        </xsl:otherwise>
                                    </xsl:choose>
                                </xsl:when>
                                
                                <!-- Otherwise, output datafield 100/110 etc with 886 a/2 and call template recursively if there are more new datafields -->
                                <xsl:otherwise>
                                    <!--<xsl:variable name="cnt" select="count($newDatafield[1]/preceding-sibling::marc:subfield[@code = 'a'][3])"/>-->
                                    <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
                                        <xsl:for-each select="$newDatafield[1]/marc:subfield[@code = 'a'][3]/preceding-sibling::marc:subfield">
                                            <xsl:if test="position() > 2">
                                                <subfield code="{@code}"><xsl:value-of select="."/></subfield>
                                            </xsl:if>
                                        </xsl:for-each>
                                    </datafield>
                                    <xsl:if test="count($newDatafield[2]) > 0">
                                        <xsl:call-template name="authorityMatch">
                                            <xsl:with-param name="newDatafield" select="$newDatafield[position() > 1]"/>
                                            <xsl:with-param name="used" select="'true'"/>
                                        </xsl:call-template>
                                    </xsl:if>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:when>
                        
                        <!-- If new datafield has only one subfield @a and is the last, output old datafield -->                        
                        <xsl:otherwise>
                            <xsl:choose>
                                <xsl:when test="count($newDatafield[2]) = 0">
                                    <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
                                        <xsl:for-each select="marc:subfield[normalize-space(.)]">
                                            <subfield code="{@code}"><xsl:value-of select="."/></subfield>
                                        </xsl:for-each>
                                    </datafield>
                                </xsl:when>
                                <xsl:otherwise>
                                    <xsl:call-template name="authorityMatch">
                                        <xsl:with-param name="newDatafield" select="$newDatafield[position() > 1]"/>
                                        <xsl:with-param name="used" select="'false'"/>
                                    </xsl:call-template>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:when>
                
                <!-- If there is no new datafield at all, output old datafield -->
                <xsl:otherwise>
                    <datafield ind1="{@ind1}" ind2="{@ind2}" tag="{@tag}">
                        <xsl:for-each select="marc:subfield[normalize-space(.)]">
                            <subfield code="{@code}"><xsl:value-of select="."/></subfield>
                        </xsl:for-each>
                    </datafield>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:if>
    </xsl:template>
    
</xsl:stylesheet>


