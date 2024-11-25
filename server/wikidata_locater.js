
const routes = require('./routes.json');

function getPage(request, markerExpeted) {
  var linksByCats = {};
  const markerLon = markerExpeted ? request.query.markerLon : null;
  const markerLat = markerExpeted ? request.query.markerLat : null;

  Object.keys(routes).forEach((rPrefix, _) => {
    const rInfo = routes[rPrefix];
    var name = rInfo['name'];
    var cats = rInfo['category'];
    const type = ('type' in rInfo) ? rInfo['type'] : 'vector';
    if (type == 'raster') {
      return;
    }
    if (!Array.isArray(cats)) {
      cats = [cats];
    }
    for (const cat of cats) {
      if (!(cat in linksByCats)) {
        linksByCats[cat] = [];
      }
      var query = '';
      if (markerExpeted) {
        query = `?markerLat=${markerLat}&markerLon=${markerLon}#14/${markerLat}/${markerLon}`;
      }
      linksByCats[cat].push(`<a href="${rPrefix}view${query}" target="_blank">${name}</a>`);
    }
  });
  
  var catLis = [];
  Object.keys(linksByCats).forEach((cat,_) => {
    const linkLis = linksByCats[cat].map((l) => `<li>${l}</li>`);
    const catLi = `<li>${cat}<ul>${linkLis.join("")}</ul></li>`
    catLis.push(catLi);
  });
  var desc = 'Main';
  if (markerExpeted) {
    desc = `${markerLat}, ${markerLon}`;
  }
  return `
  <html>
    <head>
      <link rel="stylesheet" type="text/css" href="main-dark.css">
    </head>
    <body>
      <h1>Locater Links ( ${desc} )</h1>
        <div>
          <ul>${catLis.join("")}</ul>
        </div>
    </body>
  </html>`
}
module.exports = getPage;
