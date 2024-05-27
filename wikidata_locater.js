
const routes = require('./routes.json');

function getPage(request) {
  var linksByCats = {};
  const markerLon = request.query.markerLon;
  const markerLat = request.query.markerLat;

  Object.keys(routes).forEach((rPrefix, _) => {
    const rInfo = routes[rPrefix];
    var name = rInfo['name'];
    var cats = rInfo['category'];
    if (!Array.isArray(cats)) {
      cats = [cats];
    }
    for (const cat of cats) {
      if (!(cat in linksByCats)) {
        linksByCats[cat] = [];
      }
      linksByCats[cat].push(`<a href="${rPrefix}view?markerLat=${markerLat}&markerLon=${markerLon}#14/${markerLat}/${markerLon}" target="_blank">${name}</a>`);
    }
  });
  
  var catLis = [];
  Object.keys(linksByCats).forEach((cat,_) => {
    const linkLis = linksByCats[cat].map((l) => `<li>${l}</li>`);
    const catLi = `<li>${cat}<ul>${linkLis.join("")}</ul></li>`
    catLis.push(catLi);
  });
  return `
  <html>
    <head>
      <link rel="stylesheet" type="text/css" href="main-dark.css">
    </head>
    <body>
      <h1>Locater Links ( ${markerLat}, ${markerLon} )</h1>
        <div>
          <ul>${catLis.join("")}</ul>
        </div>
    </body>
  </html>`
}
module.exports = getPage;
