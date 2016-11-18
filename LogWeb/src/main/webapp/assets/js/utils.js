var alreadyFetched = {};

function getUrl(){
    var today = new Date();
    today.setSeconds(0);
    today.setMilliseconds(0);
    var timestamp = today.valueOf();
    var dataurl = "http://localhost:8080/services/LogCount/TotalsForMinute/" + timestamp + "/";
    return dataurl;
}

function fetch() {
    // find the URL in the link right next to us
    var dataurl = getUrl();

    // then fetch the data with jQuery
    function onDataReceived(series) {
        // append to the existing data
        for(var i = 0; i < series.length; i++){
            if(alreadyFetched[series[i].FileName] == null){
                alreadyFetched[series[i].FileName] = {
                    FileName: series[i].FileName,
                    values: [{
                        Minute: series[i].Minute,
                        Total: series[i].Total
                    }]
                };
            } else {
                alreadyFetched[series[i].FileName].values.push({
                    Minute: series[i].Minute,
                    Total: series[i].Total
                });
                if(alreadyFetched[series[i].FileName].values.length > 30){
                    alreadyFetched[series[i].FileName].values.pop();
                }
            }
        }

        //update the graph
        d3.select('#chart svg')
            .datum(getdata())
            .transition().duration(500)
            .call(chart);
    }

    function onError(request, status, error){
        console.log("Received Error from AJAX: " + request.responseText);
    }

    $.ajax({
        url:dataurl,
        type:'GET',
        dataType:'json',
        crossDomain: true,
        xhrFields: {
            withCredentials: true
        },
        success:onDataReceived,
        error:onError
    });
}

function getdata(){
    var series = [];
    var keys = [];
    for (key in alreadyFetched) {
        keys.push(key);
    }
    for(var i = 0; i < keys.length; i++){
        var newValues = [];
        for(var j = 0; j < alreadyFetched[keys[i]].values.length;j++){
            newValues.push([alreadyFetched[keys[i]].values[j].Minute, alreadyFetched[keys[i]].values[j].Total]);
        }
        series.push({
            key:alreadyFetched[keys[i]].FileName,
            values:newValues
        });
    }

    return series;
}