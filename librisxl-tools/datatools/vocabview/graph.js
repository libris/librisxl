function GraphView(scale) {

  scale = (typeof scale !== 'undefined')? scale : 1

  this.nodeWidth = 32 * scale
  this.nodeHeight = 16 * scale

  var force = this.force = d3.layout.force()
      .charge(function (d) {
        if (d.link)
          return -20 * scale
        if (d.isEnum)
          return -100 * scale
        return -(200 * scale + 100 * scale * d.children.length + 20 * (d.siblingCount || 0))
      })
      .linkDistance(20 * scale)
      .linkStrength(0.1)
      .gravity(0.1)

  this.animated = true

  this.svg = d3.select("body").append("svg")

  this.svg.append("defs").selectAll("marker")
      .data(["arrow"])
    .enter().append("marker")
      .attr("id", String)
      .attr("viewBox", "0 -5 10 10")
      .attr("refX", 30).attr("refY", 0)
      .attr("markerWidth", 10).attr("markerHeight", 5)
      .attr("orient", "auto")
      .append("path")
      .attr("d", "M0,-5L10,0L0,5");

  var el = this.svgElement = this.svg[0][0]
  this.updateSize = function () {
    var style = window.getComputedStyle(el)
    var width = parseFloat(style.width.replace(/px$/, '')),
        height = parseFloat(style.height.replace(/px$/, ''))
    force.size([width, height])
  }.bind(this)
  this.updateSize()
  window.addEventListener("onresize", this.updateSize)

}
GraphView.prototype = {

  color: d3.scale.category20(),

  viewData: function (source, clickHandler, enums) {

    this.updateSize()

    var that = this

    //enums = enums || ['content', 'carrier']

    var view = this.svg.append("g")
        .call(d3.behavior.zoom().on("zoom", zoomRedraw))
        .append("g")

    function zoomRedraw() {
      if (that.animated)
        return
      var evt = d3.event
      view.attr("transform", "translate("+ evt.translate +") scale("+ evt.scale +")")
    }

    var nodeIndex = this.indexNodes(source.nodes)

    var nodes = source.nodes.slice(),
        links = [],
        bilinks = []

    source.nodes.forEach(function (node) {
      if (!node.children)
        return
      node.children.forEach(function (id) {
        var child = nodeIndex[id]
        var im = {link: true}
        nodes.push(im)
        links.push({source: child, target: im}, {source: im, target: node})
        bilinks.push([child, im, node])
      })
    })

    that.force
        .nodes(nodes)
        .links(links)

    var link = view.selectAll(".link")
        .data(bilinks)
        .enter().append("path")
        .attr("class", function (d) { return d[0].isEnum? "link enum" : "link"})

    var node = view.selectAll(".node")
        .data(source.nodes)
        .enter().append("g")
        .attr("class", function (d) {
          var cl = ['node']
          if (d.isBase) cl.push('base')
          if (d.isEnum) cl.push('enum')
          return cl.join(' ')
        })
        .on("dblclick", function () {
          d3.event.stopPropagation()
          if (!that.animated) {
            node.call(that.force.drag)
            that.force.start()
          } else {
            that.force.stop()
            node.on("mousedown.drag", null)
          }
          that.animated = !that.animated
        }, true)
    if (that.animated) {
        node.call(that.force.drag)
    }

    if (clickHandler) {
      node.on("click", function (d) {
        clickHandler.call(this, d)
      })
    }

    node.append("title")
        .text(function(d) {
          var s = d.name
          if (!d.isBase) s += " < " + d.groupName
          if (d.termgroups && d.termgroups.length)
              s += " ("+ d.termgroups.join(", ") + ")"
          return s
        })

    node.append("ellipse")
        .each(function (d) {
          var mod = (d.isEnum)? 0.5 : 1
          d3.select(this).attr("rx", that.nodeWidth * mod).attr("ry", that.nodeHeight * mod)
        })
        .style("fill", function(d) { return that.color(d.groupName); })

    node.append("text")
        .attr("dy", ".31em")
        .text(function(d) { return d.name; })

    that.force.on("tick", function() {
      link.attr("d", function(d) {
            return "M" + d[0].x + "," + d[0].y
            + "S" + d[1].x + "," + d[1].y
            + " " + d[2].x + "," + d[2].y
          })
          .attr("marker-end", "url(#arrow)")

      node.attr('transform', function (d) { return "translate("+ d.x +","+ d.y +")" })
    })

    that.force.start()
    if (!that.animated) {
      var n = 80; // depends on graph complexity..
      for (var i = n * n; i > 0; --i) that.force.tick()
      that.force.stop()
    }

  },


  indexNodes: function (nodes) {
    var nodeIndex = {}
    nodes.forEach(function (node) {
      nodeIndex[node.name] = node
    })
    nodes.forEach(function (node) {
      node.children.forEach(function (id) {
        var child = nodeIndex[id]
        // TODO: something's wrong in the vocab-view output (time to use json-ld..)
        if (!child) {
          console.log("unknown child node", id)
          child = nodeIndex[id] = {name: id, children: []}
          nodes.push(child)
        }
        child.parent = node
        child.siblingCount = (child.siblingCount || 0) + node.children.length
      })
    })

    var roots = nodes.filter(function (node) { return !node.parent })
    var rootBase = roots.length === 1? roots[0] : null

    function setGroupName(groupName, node) {
      node = (typeof node === 'object')? node : nodeIndex[node]
      if (node.termgroups && node.termgroups.length && !node.termgroups.filter(
        function (v) { return enums.indexOf(v) === -1 }).length) {
        node.isEnum = true
      }
      node.isBase = rootBase? node.parent === rootBase : !node.parent
      groupName = groupName || (node.isBase? node.name : null)
      if (!groupName) {
        return
      }
      node.groupName = groupName
      if (node.children) {
        node.children.forEach(setGroupName.bind(this, groupName))
      }
    }
    nodes.forEach(setGroupName.bind(this, null))
    return nodeIndex
  }

}
