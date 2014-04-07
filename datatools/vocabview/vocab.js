window.onload = function () {

  var nodes = Array.prototype.map.call(document.querySelectorAll(".class"), function (el) {
    return {
      name: el.id,
      children: Array.prototype.map.call(el.querySelectorAll(".subclasses a"),
                                         function (el) {
                                           var ref = el.getAttribute('href')
                                           return (ref[0] == '#')? ref.substring(1) : null
                                         }).filter(function (it) { return it })
    }
  })

  viewData({nodes: nodes}, function (d) {
    console.log(this, d)
    if (d.name) {
      document.location = '#' + d.name
    }
  })

  var svg = document.getElementsByTagName('svg')[0]
  svg.classList.add('hide')

  var classNav = document.querySelector('body > nav > section > b')
  classNav.addEventListener('click', function () {
    svg.classList.toggle('hide')
  })

}
