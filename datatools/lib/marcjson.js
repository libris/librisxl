// namespace object (works in both browser and node)
var marcjson = typeof exports !== 'undefined'? exports : {};

// build namespace
(function (exports) {

  exports.parseLeader = function (map, struct, reversible) {
    var leader = struct.leader;
    var columns = map['000'].fixmaps[0].columns;
    return buildFixedFieldObject(leader, columns, map.fixprops, reversible);
  };

  exports.fixedFieldParsers = {

    '006': parseFixedField,

    '007': parseFixedField,

    '008': function (row, dfn, leader, fixprops, reversible) {
      var recTypeBibLevelKey = leader.typeOfRecord.code + leader.bibLevel.code;
      var columns = null;
      // TODO: prepare table to lookup fixmap by matchRecTypeBibLevel
      for (var fixmap=null, i=0; fixmap=dfn.fixmaps[i++];) {
        if (fixmap.matchRecTypeBibLevel.indexOf(recTypeBibLevelKey) > -1) {
          //var type = fixmap.term; // TODO: use computed resource type key
          columns = fixmap.columns;
          break;
        }
      }
      return buildFixedFieldObject(row, columns, fixprops, reversible);
    }

  };

  function parseFixedField(row, dfn, leader, fixprops, reversible) {
    var matchKey = row[0];
    var columns = null;
    for (var fixmap=null, i=0; fixmap=dfn.fixmaps[i++];) {
      if (fixmap.matchKeys.indexOf(matchKey) > -1) {
        columns = fixmap.columns;
        break;
      }
    }
    if (columns === null)
      return row;
    else
      return buildFixedFieldObject(row, columns, fixprops, reversible);
  }

  function buildFixedFieldObject(repr, columns, fixprops, reversible) {
    var result = reversible? makeFixedFieldResult(columns) : {};
    columns.forEach(function (colDfn) {
      processFixedCol(repr, colDfn, result, fixprops);
    });
    return result;
  }

  function makeFixedFieldResult(columns) {
    // TODO: save raw value in case columns don't cover the full range
    var ctor = function () {};
    ctor.prototype.toJSON = function () {
      var self = this;
      // TODO: use saved raw as base (to cover missing column defs)
      var s = "";
      columns.forEach(function (colDfn) {
        var prop = getColumnName(colDfn);
        var o = self[prop];
        if (o === undefined) {
          s += new Array(colDfn.length).join(" ");
        } else {
          var v = o.code;
          if (v === '_') v = ' ';
          s += v;
          if (v.length !== colDfn.length) {
            s += new Array(colDfn.length - v.length).join(" ");
          }
        }
      });
      return s;
    };
    return new ctor();
  }

  function processFixedCol (repr, colDfn, result/*, fixprops*/) {
    var off = colDfn.offset;
    var key = repr.substring(off, off + colDfn.length) || colDfn['default'];
    var prop = getColumnName(colDfn);
    if (prop) {
      key = key == ' '? '_' : key;
      result[prop] = {code: key, getDfn: function () { return colDfn; }};
      //var valueId = fixprops[prop][key].id;
      //if (valueId) { result[prop].id = valueId; }
    }
  }

  function getColumnName(colDfn) {
    if (colDfn.propRef) {
      return colDfn.propRef;
    } else if (colDfn.placeholder[0] != '<') {
      return colDfn.placeholder;
    } else {
      return "_col_" + colDfn.offset + "_" + colDfn.length;
    }
  }


  /**
  * Expands fixed marc fields into objects, in-place. Uses a marc-map containing
  * parsing instructions. If the reversible parameter is true, the resulting
  * objects have toJSON methods responsible for turning them back into fixed
  * field values upon serialization.
  */
  exports.expandFixedFields = function (map, struct, reversible) {
    if (typeof struct.leader === 'object')
      return; // assumes expand has already been run
    var leader = exports.parseLeader(map, struct, reversible);
    struct.leader = leader;
    for (var tag in exports.fixedFieldParsers) {
      struct.fields.forEach(function (field) {
        var row = field[tag];
        if (row) {
          var parse = exports.fixedFieldParsers[tag];
          var dfn = map[tag];
          field[tag] = parse(row, dfn, leader, map.fixprops, reversible);
        }
      });
    }
  };

  /**
  * Get one key from an object expected to contain only one key.
  */
  exports.getMapEntryKey = function (o) {
    for (var key in o) return key;
  };

  exports.getIndicatorType = function (tag, indKey, indEnum) {
    var i = 0;
    for (var k in indEnum) if (i++) break;
    if (i === 1 &&
        (indEnum['_'].id === 'undefined' ||
          indEnum['_'].label_sv === 'odefinierad')) {
      return 'hidden';
    } else if (indEnum) {
      return 'select';
    } else {
      return 'plain';
    }
  };

  exports.getWidgetType = function (tag, row) {
    if (tag === 'leader' || exports.fixedFieldParsers[tag]) {
      return 'fixedfield';
    } else if (typeof row === 'string') {
      return 'raw';
    } else {
      return 'field';
    }
  };


  exports.addField = function (struct, tagToAdd, dfn) {
    var fields = struct.fields;
    if (!tagToAdd)
      return;
    for (var i=0, ln=fields.length; i < ln; i++) {
      var field = fields[i];
      var tag = exports.getMapEntryKey(field);
      if (tag > tagToAdd) break;
    }
    // TODO: get more defaults from dfn (overlay)
    var subfields = [];
    var row = {ind1: " ", ind2: " ", subfields: subfields};
    var defaultCodes = (dfn && dfn.defaultCodes)? dfn.defaultCodes : ['a'];
    defaultCodes.forEach(function (code) {
      var subfield = {};
      subfield[code] = "";
      subfields.push(subfield);
    });
    var o = {};
    o[tagToAdd] = row;
    fields.splice(i, 0, o);
    return o;
  };

  exports.removeField = function (struct, index) {
    struct.fields.splice(index, 1);
  };

  exports.addSubField = function (row, subCode, index) {
    if (!subCode)
      return;
    var o = {};
    o[subCode] = "";
    if (index === -1)
      row.subfields.push(o);
    else
      row.subfields.splice(index + 1, 0, o);
    return o;
  };

  exports.removeSubField = function (row, index) {
    row.subfields.splice(index, 1);
  };


  exports.createEntityGroups = function (map, overlay, struct) {
    var entitySpecs = overlay.entities;
    var out = {};
    var structFields = {};
    exports.expandFixedFields(map, struct);
    map.leader = map['000'];
    var leaderField = {leader: struct.leader};
    decorateMarcField(map, overlay, 'leader', leaderField);
    structFields['leader'] = [leaderField];
    struct.fields.forEach(function (field) {
      var tag = exports.getMapEntryKey(field);
      decorateMarcField(map, overlay, tag, field);
      var tagged = structFields[tag];
      if (tagged === undefined) tagged = structFields[tag] = [];
      tagged.push(field);
    });

    for (entity in entitySpecs) {
      var entitySpec = entitySpecs[entity];
      var group = out[entity] = {};
      for (groupKey in entitySpec) {
        groupSpec = entitySpec[groupKey];
        var targetGroup = group[groupKey] =
          createTargetGroup(map, overlay, struct, groupSpec);
        addFieldsBySpec(structFields, groupSpec, targetGroup);
      }
    }
    return out;
  };

  function createTargetGroup(map, overlay, struct, groupSpec) {
    var targetGroup = [];
    var defs = targetGroup.fieldDefs = [];
    groupSpec.forEach(function (tag) {
      if (typeof tag === 'string') {
        decorateMarcFieldDefinition(map, overlay, tag);
        defs.push(map[tag]);
      } else {
        decorateMarcFieldDefinition(map, overlay, exports.getMapEntryKey(tag));
      }
    });
    targetGroup.addField = function (tag) {
      var newField = exports.addField(struct, tag, map[tag]);
      decorateMarcField(map, overlay, tag, newField);
      var i = groupSpec.indexOf(tag);
      var nextTag = groupSpec[i + 1];
      for (ln=this.length; i < ln; i++) {
        var field = this[i];
        if (field.getTagDfn().tag === nextTag) {
          break;
        }
      }
      this.splice(i, 0, newField);
    };
    return targetGroup;
  }

  function decorateMarcFieldDefinition(map, overlay, tag) {
    var dfn = map[tag];
    dfn.tag = tag;
    var tagExt = overlay.extend[tag];
    for (var extKey in tagExt) {
      var extVal = tagExt[extKey];
      if (extKey === 'defaultCodes') {
        dfn[extKey] = extVal;
      }
      if (dfn.subfield) {
        var subfield = dfn.subfield[extKey];
        if (subfield) {
          for (var subKey in extVal) {
            subfield[subKey] = extVal[subKey];
          }
        }
      }
    }
    dfn.ind1Type = indicatorType(tagExt, dfn, 'ind1');
    dfn.ind2Type = indicatorType(tagExt, dfn, 'ind2');
    // TODO: only add if not hidden
    dfn.indicators = {};
    var subCode = (tagExt && tagExt.indicatorsFor)?
      tagExt.indicatorsFor : 'a';
    if (this.ind1Type == 'hidden' && this.ind2Type == 'hidden')
      dfn.indicators[subCode] = [];
    else
      dfn.indicators[subCode]  = [
        {key: 'ind1', type: dfn.ind1Type, enum: dfn.ind1},
        {key: 'ind2', type: dfn.ind2Type, enum: dfn.ind2}
      ];
    // TODO: complete (add subCode to subDfn, preproc...)
    //dfn.getSubDfn = function (subfield) {
    //  subCode = exports.getMapEntryKey(subfield);
    //  return this.subfield[subCode];
    //};
  }

  function decorateMarcField(map, overlay, tag, field) {
    var dfn = map[tag];
    field.getRow = function () { return this[tag]; };
    field.getTagDfn = function () { return dfn; };
    field.getWidgetType = function () {
      return exports.getWidgetType(tag, this.getRow());
    };
    field.addSubField = function (subCode, index) {
      exports.addSubField(this[tag], subCode, index);
    };
  }

  function indicatorType(tagExt, dfn, indKey) {
    var tag = dfn.tag;
    var indEnum = dfn[indKey];
    var i = 0;
    for (var k in indEnum) if (i++) break;
    // TODO: extract 'hidden' logic to creation of dfn.indicators list
    if (i === 1 &&
        (indEnum['_'].id === 'undefined' ||
          indEnum['_'].label_sv === 'odefinierad')) {
      return 'hidden';
    } else if (tagExt && tagExt[indKey]) {
      return tagExt[indKey].type;
    } else if (indEnum) {
      return 'select';
    } else {
      return 'plain';
    }
  }

  function addFieldsBySpec(structFields, groupSpec, targetGroup) {
    groupSpec.forEach(function (path) {
      if (typeof path === 'string') {
        var fieldSet = structFields[path];
        if (fieldSet) {
          // call apply to use fields as varargs
          Array.prototype.push.apply(targetGroup, fieldSet);
        }
      } else {
        for (var key in path) {
          var holder = structFields[key];
          if (!holder)
            continue;
          // TODO: preproc split fixed fields like in decorateMarcField
          var field = {}, sub = field[key] = {};
          targetGroup.push(field);
          var subkeys = path[key];
          subkeys.forEach(function (subkey) {
            var sourceField = holder[0];
            var target = sourceField[key][subkey];
            if (target) {
              // copy decorated sourceField methods manually..
              field.getTagDfn = sourceField.getTagDfn;
              field.getRow = sourceField.getRow;
              field.getWidgetType = sourceField.getWidgetType;
              //decorateField(field);
              sub[subkey] = target;
            }
          });
        }
      }
    });
  }

  exports.rawToNamed = function (map, struct) {
    var out = {};
    out.leader = exports.parseLeader(map, struct);
    (struct.fields).forEach(function(field) {
      for (fieldTag in field) {
        var sourceRow = field[fieldTag];
        var dfn = map[fieldTag];
        var parse = exports.fixedFieldParsers[fieldTag];
        if (parse) {
          out[dfn.id] = parse(sourceRow, dfn, out.leader/*, map.fixprops*/);
        } else {
          var key = fieldTag;
          var outObj = sourceRow;
          if (dfn) {
            key = dfn.id;
            outObj = exports.rawRowToNamedRow(dfn, sourceRow);
          }
          if (dfn === undefined || dfn.repeatable !== false) {
            var outList = out[key];
            if (outList === undefined) {
              outList = out[key] = [];
            }
            outList.push(outObj);
            outObj = outList;
          }
          out[key] = outObj;
        }
      }
    });
    return out;
  };

  exports.rawRowToNamedRow = function(fieldDfn, row) {
    if (typeof row === 'string')
      return row;
    if (fieldDfn.type === 'fixedLength' || fieldDfn.subfield === undefined)
      return row;
    var outField = {};
    var ind1 = row.ind1,
        ind2 = row.ind2;
    if (ind1 && ind1 !== " ") {
      var ind1Repr = ind1.toString();
      var ind1Val = fieldDfn.ind1[ind1Repr];
      outField[fieldDfn.ind1.id || 'ind1'] = ind1Val? ind1Val.id : ind1Repr;
    }
    if (ind2 && ind2 !== " ") {
      var ind2Repr = ind2.toString();
      var ind2Val = fieldDfn.ind2[ind2Repr];
      outField[fieldDfn.ind2.id || 'ind2'] = ind2Val? ind2Val.id : ind2Repr;
    }
    row.subfields.forEach(function (subfield) {
      for (subCode in subfield) {
        var key = subCode;
        var subDfn = fieldDfn.subfield[subCode];
        var outObj = subfield[subCode];
        if (subDfn) {
          key = dfnKey(subCode, subDfn);
        }
        if (subDfn === undefined || subDfn.repeatable !== false) {
          var outList = outField[key];
          if (outList === undefined) {
            outList = outField[key] = [];
          }
          outList.push(outObj);
          outObj = outList;
        }
        outField[key] = outObj;
      }
    });
    return outField;
  };

  exports.namedToRaw = function () {
    // TODO
  };

  function dfnKey(key, dfn) {
    return dfn.id || "[" + key + "] " + dfn.label_sv;
  }

})(marcjson);
