// nodeunit тест rabbit
var storage = require('./lib');
var async = require('async');

var configInit =  require('./config.js');

storage.init(configInit, function(err, res){
    if(err) console.log(err);
    else console.log('Ok!');
});
