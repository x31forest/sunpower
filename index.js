var elasticsearch = require('elasticsearch');
var _ = require('lodash');
var fetch = require('node-fetch')
var moment = require('moment')


var client = new elasticsearch.Client({
  hosts: ['http://jupiter:32768']
});

function getRecord(t1, p1, u1, unit) {
  var id = (t1+unit).replace(":-","")
  var arr = []
  console.log(`Processing unit ${unit}: ${t1}`)
  arr.push({ index: { _index: 'sunpower3', _type: 'electrical', _id: id} })
  arr.push({
    id: id, "@timestamp": t1+"-04:00",
    "produced": p1,
    "consumed": u1,
    "type": `unit_${unit}`,
	"message": t1
  })
  return arr
}

function record(arr) {
  //console.log(`Record elastic:${t1}, ${p1}, ${u1}, ${unit}`)
  return new Promise((resolve, reject) => {
    client.bulk({
      body: arr
    }).then(() => {
      resolve()
    }
    ).catch(() => {
      reject()
    })
  })

}

function _authenticate(username, password) {
  if (password) {
    var url =
      "https://elhapi.edp.sunpower.com/v1/elh/authenticate";
    var data = JSON.stringify({
      username: username,
      password: password,
      isPersistent: false
    });
    return fetch(url, {
      body: data, // must match 'Content-Type' header
      cache: "no-cache", // *default, no-cache, reload, force-cache, only-if-cached
      headers: {
        "user-agent": "Fiddle",
        "content-type": "application/json",
        Accept: "application/json"
      },
      method: "POST", // *GET, POST, PUT, DELETE, etc.
      referrer: "no-referrer" // *client, no-referrer
    })
      .then(response => response.json()) // parses response to JSON
      .then(r => { console.log("Response: " ,r);
				return [r.tokenID,r.addressId]
				});
  }
}

function getData(lastMinutes, [tokenId,addressId]) {
  console.log("Token:", tokenId, "Address:",addressId);
  var formatType = "YYYY-MM-DD[T]HH:mm:ss"
  var now = moment()
  var endTime = now.unix()
  //var endTime = now.subtract(110,'days').unix()
  var startTime = now.subtract(60, 'minutes').unix()
  if (lastMinutes)
    startTime = now.subtract(lastMinutes, 'minutes').unix()
  console.log(`From: ${startTime} to ${endTime}`)
  var url = `https://elhapi.edp.sunpower.com/v1/elh/address/${addressId}/energy/minute?endepm=${endTime}000&startepm=${startTime}000`;
  console.log(url)
  return fetch(url, {
    cache: "no-cache", // *default, no-cache, reload, force-cache, only-if-cached
    headers: {
      "user-agent": "Fiddle",
      "content-type": "application/json",
      Accept: "application/json",
	  Authorization: "SP-CUSTOM " + tokenId
    },
    method: "GET", // *GET, POST, PUT, DELETE, etc.
    referrer: "no-referrer" // *client, no-referrer)
  })
	.then(response => response.json())
    .then(response => {
	  console.log("Energy response",response)
      return response;
    });
}

var password = ''
var minutes = process.argv[2]
console.log(`Retrieving data for ${minutes} minutes`)
_.range(2).forEach(
  iteration => {
    var bulkData = []
    _authenticate("pkalex07.solar" + (iteration + 1) + "@gmail.com", password)
      .then(getData.bind(null, minutes))
      .then(data => {
	    console.log(data)
        _.map(data, element => {
		  let [bdt,ep,eu]=element.split(',')
          if(eu==null)
            eu=0.0001
          if(ep==null)
            ep=0.0001
		  console.log(`Processing unit ${iteration}: ${bdt}`)
          bulkData = bulkData.concat(getRecord(bdt, ep*12*1000, eu*12*1000, iteration+1))
        })
      })
      .then(() => {
        if(bulkData.length>0)
          record(bulkData)
      })
      .catch(console.error)
  });
