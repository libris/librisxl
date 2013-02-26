#!/usr/bin/env python
# -*- coding: utf-8 -*-

from filering import stoppwords
import json

class Spill:
    def __init__(self, json_data):
        self.marc = json_data['fields']
        self.leader = json_data['leader']
        self.spill = {}

    def __str__(self):
        return json.dumps(self.spill)


    def render_spill(self):
        self.f_leader()
        for d in self.marc:
            for code, field in d.items():
                if code == '008':
                    self.f_008(field)
                if code == '260':
                    self.year(field)
                elif code == '245':
                    self.f_245(field)
                elif code == '245':
                    #kolla isbn-centralen
                    self.get_isbn_info(field)
                #elif code == '362':
                #    self.year(code, field)
                elif code in ['041', '240']:
                    self.get_lang(code, field)
                    


        self.f_leader()

    def get_spill(self):
        self.render_spill()
        return self.__str__()

    def year(self, code, field):
        try:
            for sf in field['subfields']:
                for code, sfield in sf.items():
                    if code == 'c':
                        self.spill['year'] = sfield.strip(" ;:")
        except Exception as e:
            print "year exception", e

        

    def f_245(self, field):
        print "in 245"
        try:
            ind1 = field["ind1"]
            ind2 = field["ind2"]
            for sf in field['subfields']:
                for code, sfield in sf.items():
                    if code == 'a':
                        self.spill['240'] = {'a': "%s." % sfield.strip(" /")}
                        if sfield.split()[0] in stoppwords:
                            self.spill['filering'] = len(sfield.split()[0]) + 1

        except:
           1

    def get_lang(self, code, field):
        if code == '041':
            try:
                for sf in field['subfields']:
                    for code, sfield in sf.items():
                        if code == 'a':
                            self.spill['language'] = sfield.strip()


    def f_008(self, field):
        print "in 008"
        try:
            d = {}
            d['00_date_entered'] = field[0:6]
            d['06_type_of_date'] = field[6]
            d['07_date_1'] = field[7:11]
            d['11_date_2'] = field[11:15]
            d['15_place'] = field[15:18]
            d['18_illustrations'] = field[18:22]
            d['22_target_audience'] = field[22]
            d['23_form_of_item'] = field[23]
            d['24_nature_of_contents'] = field[24:28]
            d['28_government_publication'] = field[28]
            d['29_conference_publication'] = field[29]
            d['30_festschrift'] = field[30]
            d['31_index'] = field[31]
            d['32_undefined'] = field[32]
            d['33_literary_form'] = field[33]
            d['34_biography'] = field[34]
            d['35_language'] = field[35:38]
            d['38_modified_record'] = field[38]
            d['39_cataloging_source'] = field[39]

        except Exception as e:
            print "008 exception", e  
        self.spill['008'] = d
        #self.spill['year'] = d['07_date_1'] if d['07_date_1'].strip()

    def f_leader(self):
        print "in leader"
        try:
            d = {}
            d['00_record_length'] = self.leader[0:5]
            d['05_record_status'] = self.leader[5]
            d['06_type_of_record'] = self.leader[6]
            d['07_bibliographic_level'] = self.leader[7]
            d['08_type_of_control'] = self.leader[8]
            d['09_character_coding_scheme'] = self.leader[9]
            d['10_indicator_count'] = self.leader[10]
            d['11_subself.leader_code_count'] = self.leader[11]
            d['12_base_address_of_data'] = self.leader[12:17]
            d['17_encoding_level'] = self.leader[17]
            d['18_descriptive_cataloging_form'] = self.leader[18]
            d['19_descriptive_cataloging_form'] = self.leader[19]
            d['20_multipart_resource_record_level'] = self.leader[20]
            d['21_length_of_the_starting_character_position_portion'] = self.leader[21]
            d['22_length_of_the_implementation_defined_portion'] = self.leader[22]
            d['23_undefined'] = self.leader[23]
        except Exception as e:
            print "leader exception", e
        self.spill['leader'] = d

        def f_006(self, field):
            print "in 006 for books, a-m"
            d = {}
            d['00_form_of_material'] = field[0]
            d['01_illustrations'] = field[1..4]
            d['05_target_audience'] = field[5]
            d['06_form_of_item'] = field[6]
            d['07_nature_of_contents'] = field[7..10]
            d['11_government_publication'] = field[11]
            d['12_conference_publication'] = field[12]
            d['13_festschrift'] = field[13]
            d['14_index'] = field[14]
            d['15_undefined'] = field[15]
            d['16_literary_form'] = field[16]
            d['17_biography'] = field[17]

        def f_006(self, field):
            print "in 006 for music, c-"
            d = {}
            d['00_form_of_material'] = field[0]
            d['01_form_of_composition'] = field[1..2]
            d['03_format_of_music'] = field[3]
            d['04_music_parts'] = field[4]
            d['05_target_audience'] = field[5]
            d['06_form_of_item'] = field[6]
            d['07_accompanying_matter'] = field[7..12]
            d['13_literary_text_for_for_sound_recordings'] = field[13..14]
            d['15_undefined'] = field[15]
            d['16_transposition_and_arrangement'] = field[16]
            d['17_undefined'] = field[17]

        def f_006(self, field):
            print "in 006 for continuing resources"
            d = {}
            d['00_form_of_material'] = field[0]
            d['01_frequency'] = field[1]
            d['02_regularity'] = field[2]
            d['03_undefined'] = field[3]
            d['04_type_of_continuing_resource'] = field[4]
            d['05_form_of_original_item'] = field[5]
            d['06_form_of_item'] = field[6]
            d['07_nature_of_entire_work'] = field[7]
            d['08_nature_of_contents'] = field[8..10]
            d['11_government_publication'] = field[11]
            d['12_conference_publication'] = field[12]
            d['13_undefined'] = field[13..15]
            d['16_original_alphabet_or_script_of_title'] = field[16]
            d['17_entry_convention'] = field[17]


        def f_006(self, field):
            print "in 006 for computer files/electronic resources"
            d = {}
            d['00_form_of_material'] = field[0]
            d['01_undefined'] = field[1..4]
            d['05_target_audience'] = field[5]
            d['06_form_of_item'] = field[6]
            d['07_undefined'] = field[7..8]
            d['08_type_of_computer_file'] = field[9]
            d['10_undefined'] = field[10]
            d['11_government_publication'] = field[11]
            d['12_undefined'] = field[12..17]

        def f_006(self, field):
            print "in 006 for maps"
            d = {}
            d['00_form_of_material'] = field[0]
            d['01_relief'] = field[1..4]
            d['05_projection'] = field[5..6]
            d['07_undefined'] = field[7]
            d['08_type_of_cartographic_material'] = field[8]
            d['09_undefined'] = field[9..10]
            d['11_government_publication'] = field[11]
            d['12_form_of_item'] = field[12]
            d['13_undefined'] = field[13]
            d['15_index'] = field[14]
            d['15_undefined'] = field[15]
            d['16_special_format_characteristics'] = field[16..17]

        def f_006(self, field):
            print "in 006 for visual materials"
            d = {}
            d['00_form_of_material'] = field[0]
            d['01_relief'] = field[1..4]
            d['05_projection'] = field[5..6]
            d['07_undefined'] = field[7]
            d['08_type_of_cartographic_material'] = field[8]
            d['09_undefined'] = field[9..10]
            d['11_government_publication'] = field[11]
            d['12_form_of_item'] = field[12]
            d['13_undefined'] = field[13]
            d['15_index'] = field[14]
            d['15_undefined'] = field[15]
            d['16_special_format_characteristics'] = field[16..17]

        def f_006(self, field):
            print "in 006 for mixed materials"
            d = {}
            d['00_form_of_material'] = field[0]
            d['01_undefined'] = field[1..5]
            d['06_form_of_item'] = field[6]
            d['07_undefined'] = field[7..17]
