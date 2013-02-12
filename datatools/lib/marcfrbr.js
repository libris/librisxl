// TODO: This module is a *fork* of marcjson.js and a work in progress. They
// should be merged once the results are in.

var marcjson = typeof exports !== 'undefined'? exports : {};

(function (exports) {

  exports.toFrbrStruct = function (map, struct) {

    var rec = {type: 'Record'};
    var manifestation = {
      type: 'Manifestation',
      expressionManifested: {
        type: 'Expression',
        expressionOfWork: {type: 'Work'}
      },
      exemplarOfManifestation: {type: 'Item'}
    };
    // TODO: And remove unfilled nodes.
    // TODO: idTemplates = {
    //  "Record": "http://libris.kb.se/bib/{controlNumber}",
    //  "Expression": "http://libris.kb.se/expression/{titleProper}-{statementOfResponsibilityRelatingToTitleProper}",
    //  "Manifestation": "http://libris.kb.se/bib/{controlNumber}#manifestation",
    //}

    var result = {
      data: [rec],
      recmap: {
        Record: rec,
        Manifestation: manifestation,
        Expression: manifestation.expressionManifested,
        Work: manifestation.expressionManifested.expressionOfWork,
        Item: manifestation.exemplarOfManifestation
      },
      tempmap: {},
      getEntity: function (type, addNew, rel) {
        type = type || "Record";
        var entity = this.recmap[type];
        if (entity) {
          addNew = false;
        } else {
          entity = this.tempmap[type];
        }
        if (addNew === true || !entity) {
          entity = this.tempmap[type] = {type: type};
          // TODO: fix multiple entity conflation (below is not enough)
          //entity = {type: type};
          //getSet(this.tempmap, type).push(entity);
        }
        if (rel && entity.type !== 'Manifestation' && entity.type !== 'Record') {
          getSet(this.recmap['Manifestation'], rel).push(entity);
        } else if (addNew) {
          this.data.push(entity);
        }
        return entity;
      }
    };

    var leader = exports.parseLeader(map, struct);
    exports.processFixedField('leader', leader, result);
    struct.fields.forEach(function(field) {
      for (fieldTag in field) {
        var sourceRow = field[fieldTag];
        var dfn = map[fieldTag];
        var handler = exports.fixedFieldParsers[fieldTag];
        if (handler) {
          var converted = handler(sourceRow, dfn, leader, map.fixprops);
          exports.processFixedField(dfn.id, converted, result);
        } else {
          exports.processRawRow(sourceRow, dfn, result);
        }
      }
    });
    rec.describes = manifestation;
    return result.data;
  };

  exports.parseLeader = function (map, struct) {
    var leaderStr = struct.leader;
    var converted = {};
    map['000'].fixmaps[0].columns.forEach(function (colDfn) {
      exports.processFixedCol(leaderStr, colDfn, converted, map.fixprops);
    });
    return converted;
  };

  exports.fixedFieldParsers = {

    '006': parseFixedField,

    '007': parseFixedField,

    '008': function (row, dfn, leader, fixprops) {
      var converted = {};
      var recTypeBibLevelKey = leader.typeOfRecord.marcKey + leader.bibLevel.marcKey;
      // TODO: prepare table to lookup fixmap by matchRecTypeBibLevel
      dfn.fixmaps.forEach(function (fixmap) {
        if (fixmap.matchRecTypeBibLevel.indexOf(recTypeBibLevelKey) > -1) {
          //var type = fixmap.term; // TODO: use computed resource type key
          fixmap.columns.forEach(function (colDfn) {
            exports.processFixedCol(row, colDfn, converted, fixprops);
          });
        }
      });
      return converted;
    }
  };

  function parseFixedField(row, dfn, leader) {
    var matched = false;
    var converted = {};
    //var matchKey = leader.typeOfRecord.marcKey;
    var matchKey = row[0];
    dfn.fixmaps.forEach(function (fixmap) {
      if (fixmap.matchKeys.indexOf(matchKey) > -1) {
        matched = true;
        fixmap.columns.forEach(function (colDfn) {
          exports.processFixedCol(row, colDfn, converted);
        });
      }
    });
    return matched? converted : row;
  }

  exports.processFixedCol = function (repr, colDfn, converted, fixprops) {
    var off = colDfn.offset;
    var key = repr.substring(off, off + colDfn.length) || colDfn['default'];
    var prop = colDfn.propRef;
    if (!prop) {
      if (colDfn.placeholder[0] != '<') {
        prop = colDfn.placeholder;
      } else {
        // TODO: add tag name prefix
        var tagname = "";
        prop = "_" +  tagname + "_col_" + off + "_" + colDfn.length;
      }
    }
    if (key === ' ') return;
    key = key == ' '? '_' : key;
    converted[prop] = {marcKey: key, getDfn: function () { return colDfn; }};
    var propDfn = fixprops[prop];
    if (propDfn && propDfn[key]) {
      converted[prop].id = propDfn[key].id;
    }
  };

  exports.processFixedField = function (fieldName, field, result) {
    for (attr in field) {
      var value = field[attr];
      var obj = value.id? {id: value.id} : value.marcKey;
      result.getEntity(value.getDfn().entity, true)[attr] = obj;
    }
  };

  exports.processRawRow = function(row, fieldDfn, result) {
    var outObj = {};
    if (!fieldDfn) {
      console.log("Missing field definition for", row);
      return;
    }
    var key = fieldDfn.id;

    if (typeof row === 'string' || fieldDfn.type === 'fixedLength' || fieldDfn.subfield === undefined) {
      outObj[key] = row;
    } else {

      var ind1 = row.ind1,
          ind2 = row.ind2;
      if (ind1 && ind1 !== " ") {
        var ind1Repr = ind1.toString();
        var ind1Val = fieldDfn.ind1[ind1Repr];
        outObj[fieldDfn.ind1.id || 'ind1'] = ind1Val? ind1Val.id : ind1Repr;
      }
      if (ind2 && ind2 !== " ") {
        var ind2Repr = ind2.toString();
        var ind2Val = fieldDfn.ind2[ind2Repr];
        outObj[fieldDfn.ind2.id || 'ind2'] = ind2Val? ind2Val.id : ind2Repr;
      }
      var addNew = true;
      var rel = fieldDfn.id;
      row.subfields.forEach(function (subfield) {
        for (subCode in subfield) {
          var key = subCode;
          var subDfn = fieldDfn.subfield[subCode];
          var value = subfield[subCode];
          if (subDfn) {
            key = dfnKey(subCode, subDfn);
          }
          var entity = subDfn? result.getEntity(subDfn.entity, addNew, rel) : outObj;
          if (subDfn === undefined || subDfn.repeatable !== false) {
            var outList = getSet(entity, key);
            outList.push(value);
            value = outList;
          }
          entity[key] = value;
          addNew = false;
        }
      });

    }
  };

  function dfnKey(key, dfn) {
    return dfn.id || "[" + key + "] " + dfn.label_sv;
  }

  function getSet(entity, key) {
    var set = entity[key];
    if (set === undefined) {
      set = entity[key] = [];
    }
    return set;
  }

})(marcjson);
