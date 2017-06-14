<?xml version="1.0" encoding="UTF-8"?>

<!--
    Document   : kurdbib.xsl
    Created on : March 5, 2010, 3:15 PM
    Author     : pelle
    Description:
        Purpose of transformation follows.
-->

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0"
                xmlns:marc="http://www.loc.gov/MARC21/slim"
                xmlns:java="http://xml.apache.org/xslt/java"
                xmlns:fmpxml="http://www.filemaker.com/fmpxmlresult"
                exclude-result-prefixes="marc java fmpxml">
    <xsl:output method="xml" indent="yes" encoding="UTF-8"/>

    <!-- TODO customize transformation rules 
         syntax recommendation http://www.w3.org/TR/xslt 
    -->
    <xsl:template match="/fmpxml:FMPXMLRESULT">
        <collection xmlns="http://www.loc.gov/MARC21/slim">
            <xsl:apply-templates select="fmpxml:RESULTSET/fmpxml:ROW"/>
        </collection>
    </xsl:template>

    <xsl:template match="fmpxml:ROW">
        <record xmlns="http://www.loc.gov/MARC21/slim" type="Bibliographic">
            <xsl:variable name="title_245a" select="normalize-space(fmpxml:COL[1]/fmpxml:DATA)"/>
            <xsl:variable name="remainder_245b" select="normalize-space(fmpxml:COL[2]/fmpxml:DATA)"/>
            <xsl:variable name="origin_245c" select="normalize-space(fmpxml:COL[3]/fmpxml:DATA)"/>
            <xsl:variable name="edition_250a" select="normalize-space(fmpxml:COL[4]/fmpxml:DATA)"/>
            <xsl:variable name="place_260a" select="normalize-space(fmpxml:COL[5]/fmpxml:DATA)"/>
            <xsl:variable name="publisher_260b" select="normalize-space(fmpxml:COL[6]/fmpxml:DATA)"/>
            <xsl:variable name="year_260c" select="normalize-space(fmpxml:COL[7]/fmpxml:DATA)"/>
            <xsl:variable name="pages_300a" select="normalize-space(fmpxml:COL[8]/fmpxml:DATA)"/>
            <xsl:variable name="illustrations_300b" select="normalize-space(fmpxml:COL[9]/fmpxml:DATA)"/>
            <xsl:variable name="isbn_020a" select="normalize-space(fmpxml:COL[10]/fmpxml:DATA)"/>
            <xsl:variable name="first_author_700a" select="normalize-space(fmpxml:COL[11]/fmpxml:DATA)"/>
            <xsl:variable name="class_code_084a" select="normalize-space(fmpxml:COL[12]/fmpxml:DATA)"/>
            <xsl:variable name="language_041a" select="normalize-space(fmpxml:COL[13]/fmpxml:DATA)"/>
            <xsl:variable name="language_240l" select="normalize-space(fmpxml:COL[14]/fmpxml:DATA)"/>
            <xsl:variable name="abstract_lang_041b" select="normalize-space(fmpxml:COL[15]/fmpxml:DATA)"/>
            <xsl:variable name="institution_710a" select="normalize-space(fmpxml:COL[16]/fmpxml:DATA)"/>
            <xsl:variable name="varying_title_246a" select="normalize-space(fmpxml:COL[17]/fmpxml:DATA)"/>
            <xsl:variable name="accompanying_300e" select="normalize-space(fmpxml:COL[18]/fmpxml:DATA)"/>
            <xsl:variable name="author_100a" select="normalize-space(fmpxml:COL[19]/fmpxml:DATA)"/>
            <xsl:variable name="second_author_700a" select="normalize-space(fmpxml:COL[20]/fmpxml:DATA)"/>
            <xsl:variable name="organization_710a" select="normalize-space(fmpxml:COL[21]/fmpxml:DATA)"/>
            <xsl:variable name="first_translator_700a" select="normalize-space(fmpxml:COL[22]/fmpxml:DATA)"/>
            <xsl:variable name="second_translator_700a" select="normalize-space(fmpxml:COL[23]/fmpxml:DATA)"/>
            <xsl:variable name="first_illustrator_700a" select="normalize-space(fmpxml:COL[24]/fmpxml:DATA)"/>
            <xsl:variable name="second_illustrator_700a" select="normalize-space(fmpxml:COL[25]/fmpxml:DATA)"/>
            <xsl:variable name="first_editor_700a" select="normalize-space(fmpxml:COL[26]/fmpxml:DATA)"/>
            <xsl:variable name="second_editor_700a" select="normalize-space(fmpxml:COL[27]/fmpxml:DATA)"/>
            <xsl:variable name="format_008" select="normalize-space(fmpxml:COL[28]/fmpxml:DATA)"/>
            <xsl:variable name="country_008" select="normalize-space(fmpxml:COL[29]/fmpxml:DATA)"/>
            <xsl:variable name="first_origin_language_041h" select="normalize-space(fmpxml:COL[30]/fmpxml:DATA)"/>
            <xsl:variable name="second_origin_language_041h" select="normalize-space(fmpxml:COL[31]/fmpxml:DATA)"/>
            <xsl:variable name="first_work_language_041a" select="normalize-space(fmpxml:COL[32]/fmpxml:DATA)"/>
            <xsl:variable name="second_work_language_041a" select="normalize-space(fmpxml:COL[33]/fmpxml:DATA)"/>
            <xsl:variable name="material_000" select="normalize-space(fmpxml:COL[34]/fmpxml:DATA)"/>
            <xsl:variable name="registration_date_008" select="normalize-space(fmpxml:COL[35]/fmpxml:DATA)"/>
            <xsl:variable name="original_title_240a" select="normalize-space(fmpxml:COL[36]/fmpxml:DATA)"/>

            <xsl:variable name="date1" select="substring(normalize-space(translate($registration_date_008, '-', '')), 3)"/>
            <xsl:variable name="date2" select="substring(normalize-space(translate($year_260c, '-()[]?', '')), 1, 4)"/>
            <xsl:variable name="country" select="substring(normalize-space(translate($country_008, '[]', '')), 1, 2)"/>
            <xsl:variable name="cf008_35-37"><xsl:call-template name="format_lang"><xsl:with-param name="victim" select="$language_041a"/></xsl:call-template></xsl:variable>
            <xsl:variable name="language1"><xsl:call-template name="format_lang"><xsl:with-param name="victim" select="$first_work_language_041a"/></xsl:call-template></xsl:variable>
            <xsl:variable name="language2"><xsl:call-template name="format_lang"><xsl:with-param name="victim" select="$second_work_language_041a"/></xsl:call-template></xsl:variable>
            <xsl:variable name="abs_language"><xsl:call-template name="format_lang"><xsl:with-param name="victim" select="$abstract_lang_041b"/></xsl:call-template></xsl:variable>
            <xsl:variable name="orig_language1"><xsl:call-template name="format_lang"><xsl:with-param name="victim" select="$first_origin_language_041h"/></xsl:call-template></xsl:variable>
            <xsl:variable name="orig_language2"><xsl:call-template name="format_lang"><xsl:with-param name="victim" select="$second_origin_language_041h"/></xsl:call-template></xsl:variable>

            <xsl:variable name="cf008_00-05">
                <xsl:choose>
                    <xsl:when test="string-length($date1) = 6"><xsl:value-of select="$date1"/></xsl:when>
                    <xsl:otherwise>100319</xsl:otherwise>
                </xsl:choose>
            </xsl:variable>
            <xsl:variable name="cf008_06-10">
                <xsl:choose>
                    <xsl:when test="string-length($date2) = 4">s<xsl:value-of select="$date2"/></xsl:when>
                    <xsl:otherwise>nuuuu</xsl:otherwise>
                </xsl:choose>
            </xsl:variable>
            <xsl:variable name="cf008_15-16">
                <xsl:choose>
                    <xsl:when test="string-length($country) = 2"><xsl:value-of select="translate($country, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')"/></xsl:when>
                    <xsl:otherwise>xx</xsl:otherwise>
                </xsl:choose>
            </xsl:variable>
            <xsl:variable name="cf008_22">
                <xsl:choose>
                    <xsl:when test="normalize-space($material_000) = 'Teaching media'">c</xsl:when>
                    <xsl:when test="normalize-space($material_000) = 'Juvenile'">j</xsl:when>
                    <xsl:otherwise>|</xsl:otherwise>
                </xsl:choose>   
            </xsl:variable>
            <xsl:variable name="cf008_33">
                <xsl:choose>
                    <xsl:when test="normalize-space($material_000) = 'Fiction'">1</xsl:when>
                    <xsl:otherwise>0</xsl:otherwise>
                </xsl:choose>
            </xsl:variable>
            
            <leader><xsl:text>     nam a22     3  4500</xsl:text></leader>

            <controlfield tag="008"><xsl:value-of select="$cf008_00-05"/><xsl:value-of select="$cf008_06-10"/><xsl:text>    </xsl:text><xsl:value-of select="$cf008_15-16"/><xsl:text> </xsl:text>||||<xsl:value-of select="$cf008_22"/><xsl:text> |||||||| </xsl:text><xsl:value-of select="$cf008_33"/>|<xsl:value-of select="$cf008_35-37"/><xsl:text> c</xsl:text></controlfield>

            <xsl:if test="$isbn_020a != ''">
                <datafield ind1=" " ind2=" " tag="020">
                    <subfield code="a"><xsl:value-of select="$isbn_020a"/></subfield>
                </datafield>
            </xsl:if>

            <datafield ind1=" " ind2=" " tag="040">
                <subfield code="a">Kub</subfield>
            </datafield>

            <xsl:if test="string-length($cf008_35-37) = 3 or string-length($language1) = 3 or string-length($language2) = 3 or string-length($abs_language) = 3 or string-length($orig_language1) = 3 or string-length($orig_language2) = 3">
                <xsl:variable name="ind1">
                    <xsl:choose>
                        <xsl:when test="string-length($orig_language1) = 3 or string-length($orig_language2) = 3">1</xsl:when>
                        <xsl:when test="string-length($orig_language1) != 3 and string-length($orig_language2) != 3">0</xsl:when>
                        <xsl:otherwise><xsl:text> </xsl:text></xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
                <datafield ind1="{$ind1}" ind2=" " tag="041">
                    <xsl:if test="string-length($cf008_35-37) = 3 and $cf008_35-37 != 'und'">
                        <subfield code="a"><xsl:value-of select="$cf008_35-37"/></subfield>
                    </xsl:if>
                    <xsl:if test="string-length($language1) = 3 and $language1 != 'und'">
                        <subfield code="a"><xsl:value-of select="$language1"/></subfield>
                    </xsl:if>
                    <xsl:if test="string-length($language2) = 3 and $language2 != 'und'">
                        <subfield code="a"><xsl:value-of select="$language2"/></subfield>
                    </xsl:if>
                    <xsl:if test="string-length($abs_language) = 3 and $abs_language != 'und'">
                        <subfield code="b"><xsl:value-of select="$abs_language"/></subfield>
                    </xsl:if>
                    <xsl:if test="string-length($orig_language1) = 3 and $orig_language1 != 'und' and $orig_language1 != $language1 and $orig_language1 != $language2 and $orig_language1 != $cf008_35-37">
                        <subfield code="h"><xsl:value-of select="$orig_language1"/></subfield>
                    </xsl:if>
                    <xsl:if test="string-length($orig_language2) = 3 and $orig_language2 != 'und' and $orig_language2 != $language1 and $orig_language2 != $language2 and $orig_language2 != $cf008_35-37">
                        <subfield code="h"><xsl:value-of select="$orig_language2"/></subfield>
                    </xsl:if>
                </datafield>
            </xsl:if>
            
            <xsl:if test="$class_code_084a != ''">
                <datafield ind1=" " ind2=" " tag="084">
                    <xsl:choose>
                        <xsl:when test="contains($class_code_084a, ' ')">
                            <subfield code="a"><xsl:value-of select="substring-before($class_code_084a, ' ')"/></subfield>
                        </xsl:when>
                        <xsl:otherwise>
                            <subfield code="a"><xsl:value-of select="$class_code_084a"/></subfield>
                        </xsl:otherwise>
                    </xsl:choose>
                    <subfield code="2">kssb/8</subfield>
                </datafield>
            </xsl:if>
            
            <xsl:if test="$author_100a != ''">
            <xsl:variable name="ind1"><xsl:call-template name="filing_indicator"><xsl:with-param name="offer" select="$author_100a"/></xsl:call-template></xsl:variable>
                <datafield ind1="{$ind1}" ind2=" " tag="100">
                    <subfield code="a"><xsl:value-of select="$author_100a"/></subfield>
                </datafield>
            </xsl:if>
            
            
            <xsl:if test="$original_title_240a != ''">
                <datafield ind1="1" ind2="0" tag="240">
                    <subfield code="a"><xsl:value-of select="$original_title_240a"/><xsl:if test="$language_240l != ''">.</xsl:if></subfield>
                    <xsl:if test="$language_240l != ''">
                        <subfield code="l"><xsl:value-of select="$language_240l"/></subfield>
                    </xsl:if>
                </datafield>
            </xsl:if>
            
            <xsl:if test="$title_245a != ''">
                <datafield ind1="1" ind2="0" tag="245">
                    <subfield code="a"><xsl:value-of select="$title_245a"/><xsl:choose><xsl:when test="$remainder_245b != ''"><xsl:text> :</xsl:text></xsl:when><xsl:when test="$origin_245c != ''"><xsl:text> /</xsl:text></xsl:when></xsl:choose></subfield>
                    <xsl:if test="$remainder_245b != ''">
                        <subfield code="b"><xsl:value-of select="$remainder_245b"/><xsl:if test="$origin_245c != ''"><xsl:text> /</xsl:text></xsl:if></subfield>
                    </xsl:if>
                    <xsl:if test="$origin_245c != ''">
                        <subfield code="c"><xsl:value-of select="$origin_245c"/></subfield>
                    </xsl:if>
                </datafield>
            </xsl:if>
            
            <xsl:if test="$varying_title_246a != ''">
                <datafield ind1="1" ind2=" " tag="246">
                    <subfield code="a"><xsl:value-of select="$varying_title_246a"/></subfield>
                </datafield>
            </xsl:if>
            
            
            <xsl:if test="$edition_250a != ''">
                <datafield ind1=" " ind2=" " tag="250">
                    <subfield code="a"><xsl:value-of select="$edition_250a"/></subfield>
                </datafield>
            </xsl:if>

            <xsl:if test="$place_260a != ''">
                <datafield ind1=" " ind2=" " tag="260">
                    <subfield code="a"><xsl:value-of select="$place_260a"/><xsl:choose><xsl:when test="$publisher_260b != ''"><xsl:text>: </xsl:text></xsl:when><xsl:when test="$year_260c != ''">,</xsl:when></xsl:choose></subfield>
                    <xsl:if test="$publisher_260b != ''">
                        <subfield code="b"><xsl:value-of select="$publisher_260b"/><xsl:if test="$year_260c != ''">,</xsl:if></subfield>
                    </xsl:if>
                    <xsl:if test="$year_260c != ''">
                        <subfield code="c"><xsl:value-of select="$year_260c"/></subfield>
                    </xsl:if>
                </datafield>
            </xsl:if>
            
            
            <xsl:if test="$pages_300a != ''">
                <datafield ind1=" " ind2=" " tag="300">
                    <subfield code="a"><xsl:value-of select="$pages_300a"/><xsl:text> s.</xsl:text><xsl:choose><xsl:when test="$illustrations_300b != ''"><xsl:text> :</xsl:text></xsl:when><xsl:when test="$accompanying_300e != ''"><xsl:text> +</xsl:text></xsl:when></xsl:choose></subfield>
                    <xsl:if test="$illustrations_300b != ''">
                        <subfield code="b"><xsl:value-of select="$illustrations_300b"/><xsl:if test="$accompanying_300e != ''"><xsl:text> +</xsl:text></xsl:if></subfield>
                    </xsl:if>
                    <xsl:if test="$accompanying_300e != ''">
                        <subfield code="e"><xsl:value-of select="$accompanying_300e"/></subfield>
                    </xsl:if>
                </datafield>
            </xsl:if>


            <xsl:if test="$first_author_700a != ''">
                <xsl:variable name="ind1"><xsl:call-template name="filing_indicator"><xsl:with-param name="offer" select="$first_author_700a"/></xsl:call-template></xsl:variable>
                <datafield ind1="{$ind1}" ind2=" " tag="700">
                    <subfield code="a"><xsl:value-of select="$first_author_700a"/></subfield>
                    <subfield code="4">aut</subfield>
                </datafield>
            </xsl:if>

            <xsl:if test="$second_author_700a != ''">
                <xsl:variable name="ind1"><xsl:call-template name="filing_indicator"><xsl:with-param name="offer" select="$second_author_700a"/></xsl:call-template></xsl:variable>
                <datafield ind1="{$ind1}" ind2=" " tag="700">
                    <subfield code="a"><xsl:value-of select="$second_author_700a"/></subfield>
                    <subfield code="4">aut</subfield>
                </datafield>
            </xsl:if>

            <xsl:if test="$first_translator_700a != ''">
                <xsl:variable name="ind1"><xsl:call-template name="filing_indicator"><xsl:with-param name="offer" select="$first_translator_700a"/></xsl:call-template></xsl:variable>
                <datafield ind1="{$ind1}" ind2=" " tag="700">
                    <subfield code="a"><xsl:value-of select="$first_translator_700a"/></subfield>
                    <subfield code="4">trl</subfield>
                </datafield>
            </xsl:if>

            <xsl:if test="$second_translator_700a != ''">
                <xsl:variable name="ind1"><xsl:call-template name="filing_indicator"><xsl:with-param name="offer" select="$second_translator_700a"/></xsl:call-template></xsl:variable>
                <datafield ind1="{$ind1}" ind2=" " tag="700">
                    <subfield code="a"><xsl:value-of select="$second_translator_700a"/></subfield>
                    <subfield code="4">trl</subfield>
                </datafield>
            </xsl:if>
            
            <xsl:if test="$first_illustrator_700a != ''">
                <xsl:variable name="ind1"><xsl:call-template name="filing_indicator"><xsl:with-param name="offer" select="$first_illustrator_700a"/></xsl:call-template></xsl:variable>
                <datafield ind1="{$ind1}" ind2=" " tag="700">
                    <subfield code="a"><xsl:value-of select="$first_illustrator_700a"/></subfield>
                    <subfield code="4">ill</subfield>
                </datafield>
            </xsl:if>

            <xsl:if test="$second_illustrator_700a != ''">
                <xsl:variable name="ind1"><xsl:call-template name="filing_indicator"><xsl:with-param name="offer" select="$second_illustrator_700a"/></xsl:call-template></xsl:variable>
                <datafield ind1="{$ind1}" ind2=" " tag="700">
                    <subfield code="a"><xsl:value-of select="$second_illustrator_700a"/></subfield>
                    <subfield code="4">ill</subfield>
                </datafield>
            </xsl:if>

            <xsl:if test="$first_editor_700a != ''">
                <xsl:variable name="ind1"><xsl:call-template name="filing_indicator"><xsl:with-param name="offer" select="$first_editor_700a"/></xsl:call-template></xsl:variable>
                <datafield ind1="{$ind1}" ind2=" " tag="700">
                    <subfield code="a"><xsl:value-of select="$first_editor_700a"/></subfield>
                    <subfield code="4">edt</subfield>
                </datafield>
            </xsl:if>
            <xsl:if test="$second_editor_700a != ''">
                <xsl:variable name="ind1"><xsl:call-template name="filing_indicator"><xsl:with-param name="offer" select="$second_editor_700a"/></xsl:call-template></xsl:variable>
                <datafield ind1="{$ind1}" ind2=" " tag="700">
                    <subfield code="a"><xsl:value-of select="$second_editor_700a"/></subfield>
                    <subfield code="4">edt</subfield>
                </datafield>
            </xsl:if>
            
            
            <xsl:if test="$organization_710a != ''">
                <datafield ind1="2" ind2=" " tag="710">
                    <subfield code="a"><xsl:value-of select="$organization_710a"/></subfield>
                </datafield>
            </xsl:if>

            <xsl:if test="$institution_710a != ''">
                <datafield ind1="2" ind2=" " tag="710">
                    <subfield code="a"><xsl:value-of select="$institution_710a"/></subfield>
                </datafield>
            </xsl:if>
        </record>
        <record xmlns="http://www.loc.gov/MARC21/slim" type="Holdings">
            <leader><xsl:text>     nx  a22     1  4500</xsl:text></leader>
            <controlfield tag="008">100331||0000|||||001||||||000000</controlfield>
            <datafield ind1=" " ind2=" " tag="852">
                <subfield code="b">Kub</subfield>
            </datafield>
        </record>
    </xsl:template>

    <xsl:template name="format_lang">
        <xsl:param name="victim"/>
        <xsl:variable name="lang" select="substring(normalize-space($victim), 1, 3)"/>
        <xsl:choose>
            <xsl:when test="string-length($lang) = 3"><xsl:value-of select="translate($lang, 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ', 'abcdefghijklmnopqrstuvwxyzåäö')"/></xsl:when>
            <xsl:otherwise>und</xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <xsl:template name="filing_indicator">
        <xsl:param name="offer"/>
        <xsl:choose>
            <xsl:when test="contains($offer, ', ')">1</xsl:when>
            <xsl:otherwise>0</xsl:otherwise>
        </xsl:choose>
    </xsl:template>
</xsl:stylesheet>
