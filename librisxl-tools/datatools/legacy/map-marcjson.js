var fs = require('fs');
var marcjson = require('./lib/marcjson');

var marcMapPath = "../whelk-extensions/src/main/resources/marcmap.json"
var marcmap = require(marcMapPath)

if (!process.argv[2]) {
  console.log("USAGE: map-marcjson.js REC_TYPE MARC_JSON_FILE [OVERLAY_FILE | -n]")
  process.exit();
}

function loadJson(path) {
  var data = fs.readFileSync(path, 'utf-8');
  return JSON.parse(data);
}

var recordType = process.argv[2];
var marcStructPath = process.argv[3];
var op = process.argv[4];

var submap = marcmap;
if (recordType)
  submap = marcmap[recordType];
var struct = loadJson(marcStructPath);

var out;
if (op === '-n') {
  out = marcjson.rawToNamed(submap, struct);
} else if (op !== undefined) {
  var overlay = loadJson(op);
  out = marcjson.createEntityGroups(submap, overlay, struct);
} else {
  marcjson.expandFixedFields(submap, struct);
  out = struct;
}

console.log(JSON.stringify(out, null, 2));
