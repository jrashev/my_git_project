//start
const WebSocket = require('ws');

function WebSocketClient() {
    this.number = 0;	// Message number
    this.autoReconnectInterval = 5 * 1000;	// ms
}
WebSocketClient.prototype.open = function (url) {
    this.url = url;
    this.instance = new WebSocket(this.url);
    this.instance.on('open', () => {
        this.onopen();
    });
    this.instance.on('message', (data, flags) => {
        this.number++;
        this.onmessage(data, flags, this.number);
    });
    this.instance.on('close', (e) => {
        switch (e) {
            case 1000:	// CLOSE_NORMAL
                console.log("WebSocket: closed");
                break;
            default:	// Abnormal closure
                this.reconnect(e);
                break;
        }
        this.onclose(e);
    });
    this.instance.on('error', (e) => {
        switch (e.code) {
            case 'ECONNREFUSED':
                this.reconnect(e);
                break;
            default:
                this.onerror(e);
                break;
        }
    });
}
WebSocketClient.prototype.send = function (data, option) {
    try {
        this.instance.send(data, option);
    } catch (e) {
        this.instance.emit('error', e);
    }
}
WebSocketClient.prototype.reconnect = function (e) {
    console.log(`WebSocketClient: retry in ${this.autoReconnectInterval}ms`, e);
    this.instance.removeAllListeners();
    var that = this;
    setTimeout(function () {
        console.log("WebSocketClient: reconnecting...");
        that.open(that.url);
    }, this.autoReconnectInterval);
}
WebSocketClient.prototype.onopen = function (e) { console.log("WebSocketClient: open", arguments); }
WebSocketClient.prototype.onmessage = function (data, flags, number) { console.log("WebSocketClient: message", arguments); }
WebSocketClient.prototype.onerror = function (e) { console.log("WebSocketClient: error", arguments); }
WebSocketClient.prototype.onclose = function (e) { console.log("WebSocketClient: closed", arguments); }



var moment = require('moment');

var knex = require('knex')({
    client: 'mysql',
    connection: {
        host: '79.124.64.76',
        user: 'lora_6_db',
        password: 'xfHpbWFS66Kk3yql',
        database: 'new_lora_6'
    }
});

var bookshelf = require('bookshelf')(knex);

var dt = bookshelf.Model.extend({
    tableName: 'dt_water_meter_6'
});

//****** new table ;
var summ = bookshelf.Model.extend({
    tableName: 'summary_meter_6'
});
//************** end new table ;

var ports = bookshelf.Model.extend({
    tableName: 'ports'
});

var input_count = 0;
var input_index = 1;
var mv = 0;
var scale = 0;
function isInt(n) {
    return n % 1 === 0;
}

function GetLastData(dev, rx_time, period, batlevel, batvolt, mv, scale) {
    return dt.where('dev_eui', dev)
        .query(function (qb) {
            qb.limit(1);
        }).orderBy('ts', 'DESC')
        .fetch().then(function (item) {

            var ts_delta_prev = ts_delta;
	          var mv_delta_prev = mv_delta;
            var mv_prev = mv;
	          var ts_delta = 0;
            var mv_delta = 0;

           if (item !== null) {
                var prev = item.attributes;
                var moment_unixtime = moment.unix(rx_time);
                var moment_db = moment(prev.ts);
                var unixtime = Date.parse(prev.ts) / 1000;
                ts_delta = moment_unixtime.diff(moment_db, 'minutes', true);
                ts_delta = Math.round(ts_delta);
                mv_delta = mv - prev.mv;
                if (ts_delta == 0 && mv_delta == 0 && prev.ts != null)
                    return false;
            };

            var timestamp = moment.unix(rx_time).format('YYYY-MM-DD HH:mm:ss');
            var timestamp_utc = moment.unix(rx_time).utc().format('YYYY-MM-DD HH:mm:ss');

            var newRow = {
                dev_eui: dev,
                ts: timestamp,
                ts_utc: timestamp_utc,
                ts_delta: ts_delta,
                period: period,
                mv: mv,
                mv_delta: mv_delta,
                scale: scale,
                batlevel: batlevel,
                batvolt: batvolt
            };


//************ new add ;
 	          return new dt(newRow).save().then(function (model) {
                if(model!==null){
                    new summ().where({
                        dev_eui: model.attributes.dev_eui
                    })
                        .save({
                          ts: timestamp,
                          ts_utc: timestamp_utc,
                          ts_delta: ts_delta,
                          period: period,
                          mv: mv,
                          mv_delta: mv_delta,
                          scale: scale,
                          batlevel: batlevel,
                          batvolt: batvolt,
                          ts_delta_prev: ts_delta_prev,
          		            mv_prev: mv_prev,
          		            mv_delta_prev: mv_delta_prev
                        }, {
                                patch: true
                            })

                    return true;
                }
            });

//********************* end new add ;





        }).catch(function (err) {
            console.error(err);
        });

}

const ws = new WebSocketClient();
ws.open('wss://ns.eu.everynet.io/api/v1.0/data?access_token=68d777cb43f54932bcbd82483f096c29&filter=5d2abf06cc613a0001492f1c');
ws.onopen = function (e) {
    console.log("WebSocketClient connected");
};
ws.onmessage = function (message, flags, number) {
    // console.log(`WebSocketClient message #${number}: `, message);

    let data = JSON.parse(message);
    if (data.type == "uplink") {
        var dev_eui = data.meta.device;
        var rx_time = data.params.rx_time;
        var payload_base64 = data.params.payload;
        var payload = Buffer.from(payload_base64, 'base64');
        var length = payload.length;
        console.log("device " + dev_eui);
        console.log("rx_time " + rx_time);
        console.log("payload: ");
        console.log(payload);
        console.log("payload length: " + length);
        input_count = (length - 5) / 6;
        console.log("input count: " + input_count);


                ports.where('dev_eui', dev_eui).fetch().then(function (portArray) {
                    if (portArray) {
                        var active_ports = portArray.attributes.ports.split(",").map(Number);
                        if (isInt(input_count) && input_count > 0) {
                            var period = payload.readUIntBE(0, 2);
                            var batlevel = payload.readUIntBE(length - 3, 1);
                            var batvolt = payload.readUIntBE(length - 2, 2);

                            console.log("transmission period: " + period);
                            console.log("batlevel: " + batlevel);
                            console.log("batvolt: " + batvolt);

                            for (let i = 0; i < input_count; i++) {
                                mv = payload.readUIntBE(2 + 6 * i, 4);
                                scale = payload.readUIntBE(6 + 6 * i, 2);
                                input_index = i + 1;
                                console.log("mv" + input_index + ": " + mv);
                                console.log("scale" + input_index + ": " + scale);
                                var isActive = active_ports.indexOf(input_index);
                                if (isActive != -1) {
                                    GetLastData(dev_eui + input_index.toString(), rx_time, period, batlevel, batvolt, mv, scale);
                                } else {
                                    console.log("Port " + input_index + " on device " + dev_eui + " is inactive");
                                }
                            }
                        } else {
                            console.log("Cant find input count!");
                        }
                    } else {


                        if (isInt(input_count) && input_count > 0) {
                            var period = payload.readUIntBE(0, 2);
                            var batlevel = payload.readUIntBE(length - 3, 1);
                            var batvolt = payload.readUIntBE(length - 2, 2);

                            console.log("transmission period: " + period);
                            console.log("batlevel: " + batlevel);
                            console.log("batvolt: " + batvolt);

                            for (let i = 0; i < input_count; i++) {
                                mv = payload.readUIntBE(2 + 6 * i, 4);
                                scale = payload.readUIntBE(6 + 6 * i, 2);
                                input_index = i + 1;
                                console.log("mv" + input_index + ": " + mv);
                                console.log("scale" + input_index + ": " + scale);
                                GetLastData(dev_eui + input_index.toString(), rx_time, period, batlevel, batvolt, mv, scale);
                            }
                        } else {
                            console.log("Cant find input count!");
                        }
                    }



                })
    }
}


