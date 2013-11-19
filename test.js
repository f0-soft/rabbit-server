// nodeunit тест rabbit
var storage = require('./lib');
var async = require('async');

var configInit =  require('./config.js');

exports.testInitDefaultPort = function(test){
    test.expect(2);
    process.on( 'uncaughtException', test.done );
    storage.init(configInit, function(err, res){
        test.ifError(err);
        test.ok(res);
        test.done();
    });
};

exports.testClose = function(test){
    test.expect(1);
    storage.close( function(err){
        test.ifError(err);
        test.done();
    });
};

exports.testInitPort6666 = function(test){
    test.expect(1);
    storage.open(6666, null, function(err){
        test.ifError(err);
        test.done();
    });
};

