var mongodb = require('mongodb');
var redis = require('redis');
var async = require('async');
var _ = require('underscore');
var net = require('net');

// объект хранения подключённых колекций
var gCollection = {};

// справочник связи названия поля и пути
// gFieldDepPath {collectionName: {field: [namePath, linkColl]}}
// namePath - название пути
// linkColl - название коллекции связанной
var gFieldDepPath = {};

// справочник связи названия поля и пути
var gAllFieldDepPath = {};

// справочник колекций и полей участвующих в путях
var gFieldCollStart = {};

// спарвочник префикса схем
// gPrefixCol {collectionName: prefix}
var gPrefixCol = {};

//  обратный справочник спарвочник префикса схем
// grePrefixCol {prefix: collectionName}
var grePrefixCol = {};

// справочник таблиц path
// gPath {namePath: [path]}
var gPath = {};

// спаравочник для хранения предвычесляемых связей
// gPresum {название схемы: {поле: [[название схемы с предвычеслениями, полеселектор, field]...]}}
var gPresum = {};

// справочник связыннх докмуентов для проверки перед удалением
var gBackRef = {};

// справочник колекций с весами предсортировки
var gScore = {};

// справочник колекций вытягивающих веса
var gScoreJoin = {}

// обратный справочник изменённых путей
var gDepScoreJoin = {}

// справочник колекций и полей массивов
var gArrayFields = {};

// временное хранение измнений весов
var gBuffScore = {};

// временный буфер изменённых значений пути
var gModifyPath = {};

// временный буфер старого пути
var gModifyOldPath = {};

// временный буфер нового пути
var gModifyNewPath = {};

// справочник полей разбиваемых на слова для поиска
var gWordFields = {};

// буффер недавних весов
var buffScore = {};
var buffReScore = {};

var DB;
var clientRedis;

var dnode = require('dnode');

var server = net.createServer(function (socket) {

    var d = dnode({
        find: find,
        init: init,
        status: status,
        insert: insert,
        modify: modify,
        delete: remove,
        createSortIndex: createSortIndex,
        updateChildIndex: updateChildIndex,
        aggregate: aggregate
    });

    socket.pipe(d).pipe(socket);

});

var maxInt = 2147483648;


function myErr( text ) {
    return ( new Error( 'f0.rabbit-server: ' + text ));
}


// Инициализация storage
var init = function ( config , callback){
    if(!config) config = {};

    // конфигурация Mongo
    var configMongo = {};
    if(config.mongo) configMongo = config.mongo;
    var host = configMongo.host || '127.0.0.1';
    var port = configMongo.port || 27017;
    var options =  configMongo.options ||{auto_reconnect: true};
    var dbname = configMongo.dbname || 'test';

    // конфигурация Redis
    var configRedis = {};
    if(config.mongo) configRedis = config.redis;
    var hostRedis = configRedis.host || '127.0.0.1';
    var portRedis = configRedis.port || 6379;
    var optionsRedis = configRedis.options || {};

    // справочные данные
    if(config.gFieldDepPath)  gFieldDepPath = config.gFieldDepPath;
    if(config.gPath)  {
        gPath = config.gPath;
        for(key in gPath){
            if(!gFieldCollStart[gPath[key][1][0]]) gFieldCollStart[gPath[key][1][0]] = {};
            gFieldCollStart[gPath[key][1][0]][gPath[key][1][1]] = key;
            for(var i = 1; i < gPath[key].length; i++){
                if(!gAllFieldDepPath[gPath[key][i][0]]) gAllFieldDepPath[gPath[key][i][0]] = {};
                gAllFieldDepPath[gPath[key][i][0]][gPath[key][i][1]] = key;
            }
        }
    }
    if(config.gDepScoreJoin) gDepScoreJoin = config.gDepScoreJoin;
    if(config.gPresum) gPresum = config.gPresum;
    if(config.gBackRef) gBackRef = config.gBackRef;
    if(config.gPrefixCol) {
        gPrefixCol = config.gPrefixCol.c2p;
        grePrefixCol = config.gPrefixCol.p2c;
    }
    if(config.gScore) gScore = config.gScore;
    if(config.gArrayFields) gArrayFields = config.gArrayFields;
    if(config.gScoreJoin) gScoreJoin = config.gScoreJoin;
    if(config.gWordFields) gWordFields = config.gWordFields;

    DB = new mongodb.Db(dbname,new mongodb.Server(host, port, options),{w: 1, wtimeout: 1000});


    var rabbitPort = 7777;
    var rabbitHost = 'localhost';
    if(config.port) rabbitPort= config.port;
    if(config.host) rabbitHost = config.host;

    async.parallel({
            mongo: function(callback){
                DB.open(function(error){
                    if(error) callback (err);
                    else {
                        if(callback) callback(null, 'Ok');
                    }
                })
            },
            redisStart: function(callback){
                clientRedis = redis.createClient( portRedis, hostRedis, optionsRedis);

                clientRedis.on('ready', function(res){
                    callback(null, 'Ok');
                });

                clientRedis.on('error', function(err){
                    callback (err);
                });
            },
            rabbitStart: function(callback){
                open(rabbitPort, rabbitHost, callback);
            }
        },
        function(err, res) {
            if(err) callback(err);
            else  callback(null, res);
        });

    return this;
};

function open (port, host, callback){
    server.listen(port, host, function(){
        console.log('\nRabbit-server запущен. Port: '+port+'\n');
        callback();
    });
}

function close (callback){
    server.close(function(){
        console.log('\nRabbit-server закрыт.\n');
        callback();
    });
}

// функция показывающая статус заказа
var status = function (){
    if(DB._state === 'connected') return 'connected';
    else return 'disconnected';
};

// Подключение колекции к глобальному объекту
// collectionName - имя колекции
function addCollection ( collectionName ){
    if(DB._state !== 'connected') {
        throw new Error('Error disconnected MongoDb. collectionName:'+collectionName);
        return this;
    }

    var buffObj = {};
    buffObj.coll =  new mongodb.Collection(DB, collectionName);
    buffObj.collname = collectionName;

    gCollection[collectionName] = buffObj;
};

// Поиск данных в БД
// query - { collname, selector, field, [options]}
// collname - название колекции,
// selector - объект запроса,
// fields - массив возвращаемых полей,
// options - объект {sort, limit, skip, count, sortExt} опциональное поле
// sortExt - {collnameExt, filedExt, field}
// sortExt.collnameExt - название внешней колекции (полное)
// sortExt.filedExt - внешнее поле сортируемое
// sortExt.field - поле root документа где находится связывающий эелемнт
// callback(err, res)
function find( query, callback ){
    var vCollname = validCollname(query.collname);
    if(vCollname) {
        callback(vCollname);
        return;
    }

    // массив функци набирается
    var aWaterfall = startFind (query);
    // функция водопад
    var paramEnd = {collname: query.collname, callback: callback};
    if(this.updatePath) paramEnd.noUpdatePath = true;
    if(this.flag_a) paramEnd.flag_a = true;
    if(this.old_path) paramEnd.old_path = true;
    async.waterfall(aWaterfall, endFind.bind(paramEnd));
}

// функция формирующая массив функций для async.waterfall
function startFind (query){
    var fields = {};
    var options = {};

    var collname = query.collname;
    if(query.options) options = query.options;

    var selector = updateSelector(collname, query.selector);

//    console.log('selector', selector);

    if(!gCollection[collname]) addCollection(collname);

    var self = {};
    _.extend (self, gCollection[collname]);

    if(query.fields){
        for(var i = 0; i < query.fields.length ; i++){
            if(gArrayFields[collname] && gArrayFields[collname][query.fields[i]] && query.fields[i] != '_a'){
                fields['_a.'+query.fields[i]] = 1;
            }
            else fields[query.fields[i]] = 1;
        }
    } else {
        callback (myErr('fields - обязательное поле'));
        return this;
    }

    var arrayRes = [];
    if(!options.skip) options.skip = 0;

    if(options.sortExt){
        var fes = '_a.'+options.sortExt.field+'_'+gPrefixCol[options.sortExt.collnameExt]+'_'+options.sortExt.fieldExt;
        options.sort = {};
        options.sort[fes] = options.sortExt.sortExt;
        selector[fes] = {$ne:null};
    }

    var queryFind = {
        selector: selector,
        fields: fields,
        options: options
    }


    if(options.sortExt && gBuffScore[options.sortExt.collnameExt]){
        arrayRes.push(function (cb){
            findSortExt.call(self, queryFind, cb);
        });
    }
    else {
        arrayRes.push(function (cb){
            mongoFind.call(self, queryFind, cb);
        });
    }

    return arrayRes;
}

// подмена модифицированного пути в селекторе поиска
function updateSelector (collname, selector){
    var pathFields;
    if(gAllFieldDepPath[collname]){
        pathFields =  _.keys(gAllFieldDepPath[collname]);
    }

    if(pathFields){
        for(var i = 0; i < pathFields.length; i++){
            if(selector[pathFields[i]]){
                var index = returnIndexCollPath(gAllFieldDepPath[collname][pathFields[i]], collname);
                var bField  = selector[pathFields[i]];

                if(!selector['$and']) selector['$and'] = [];
                var bObj = {'_a':{'$elemMatch':{}}};
                bObj['_a']['$elemMatch'][pathFields[i]] = {$in:[]};

                if(bField['$in']){

                    for(var z = 0; z < bField['$in'].length; z++){
                        var res = findModifyPath(index, gAllFieldDepPath[collname][pathFields[i]], bField['$in'][z]);
                        bObj['_a']['$elemMatch'][pathFields[i]]['$in'].push(res.val);

                        if(res.add && res.add.length > 0){
                            bObj['_a']['$elemMatch'][pathFields[i]]['$in'] = bObj['_a']['$elemMatch'][pathFields[i]]['$in'].concat(res.add);
                        }

                        if(res.del && res.del.length > 0){
                            if(!bObj['_a']['$elemMatch'][pathFields[i]]['$nin']) bObj['_a']['$elemMatch'][pathFields[i]]['$nin'] = [];
                            bObj['_a']['$elemMatch'][pathFields[i]]['$nin'] =  bObj['_a']['$elemMatch'][pathFields[i]]['$nin'].concat(res.del);
                        }
                    }
                }
                else {

                    var res = findModifyPath(index, gAllFieldDepPath[collname][pathFields[i]], bField);
                    bObj['_a']['$elemMatch'][pathFields[i]]['$in'].push(res.val);

                    if(res.add && res.add.length > 0){
                        bObj['_a']['$elemMatch'][pathFields[i]]['$in'] = bObj['_a']['$elemMatch'][pathFields[i]]['$in'].concat(res.add);
                    }

                    if(res.del && res.del.length > 0){
                        if(!bObj['_a']['$elemMatch'][pathFields[i]]['$nin']) bObj['_a']['$elemMatch'][pathFields[i]]['$nin'] = [];
                        bObj['_a']['$elemMatch'][pathFields[i]]['$nin'] =  bObj['_a']['$elemMatch'][pathFields[i]]['$nin'].concat(res.del);
                    }
                }

                delete selector[pathFields[i]];
                selector['$and'].push(bObj);

            }
        }
    }

    return add_a(collname, selector);
}


// поиск модифицированного пути
function findModifyPath (index, nampath, id){
    if(gModifyPath[nampath] && gModifyPath[nampath][id]){
        var mData = gModifyPath[nampath][id];
        var resObj = {add:[], del:[], val: new RegExp('^'+id+'_')};
        for(var i = 0; i < mData.add.length; i ++){
            resObj.add.push(new RegExp('^'+mData.add[i]));
        }
        for(var i = 0; i < mData.del.length; i ++){
            if(mData.del[i].i < index) resObj.del.push(new RegExp('^'+mData.del[i].d));
        }
        return resObj;
    }
    else return {val: new RegExp('^'+id+'_')}
}

//функция отработки поиска при сортировки по внешнему полю
function findSortExt (query, callback){

    var self = this;
    var rCount;
    var options = query.options;
    var feidL = options.sortExt.field+'_'+gPrefixCol[options.sortExt.collnameExt]+'_id';
    var fescL = options.sortExt.field+'_'+gPrefixCol[options.sortExt.collnameExt]+'_'+options.sortExt.fieldExt;
    var fesc = '_a.'+fescL;
    var feid = '_a.'+feidL;
    var optionsExt = query.options.sortExt;

    var queryFind = {
        selector: query.selector,
        fields: {},
        options: {sort:  options.sort, limit: options.limit, skip: options.skip}
    }

    queryFind.fields[fesc] = 1;
    queryFind.fields[feid] = 1;

    mongoFind.call(self, queryFind, function(err, res, count){
        rCount = count;
        var alpath;
        if(options.sort[fesc] === 1) alpath = maxScroe({collname: optionsExt.collnameExt, field: fescL, _a: res[res.length-1]['_a']});
        else alpath = minScroe({collname: optionsExt.collnameExt, field: fescL, _a: res[res.length-1]['_a']});

        if (!alpath) mongoFind.call(self, query, callback);
        else {
            findSortExtUpdate.call(self, query, function (err, res, count){
                query.selector._id = {$in:res};
                delete query.options;
                mongoFind.call(self, query, function(error, result){
                    if(error) callabck (error);
                    else {
                        var sortRes = [];
                        for(var i = 0; i < res.length; i++){
                            for(var z = 0; z < result.length; z++){
                                if(res[i] === result[z]._id){
                                    sortRes.push(result[z]);
                                    break;
                                }
                            }
                        }
                        var countRes = null;
                        if(options.count) countRes = count;
                        callback(null, sortRes, countRes);
                    }
                });
            });
        }
    });
}

function findSortExtUpdate(query, callback){
    var self = this;
    var options = query.options;
    var feidL = options.sortExt.field+'_'+gPrefixCol[options.sortExt.collnameExt]+'_id';
    var fescL = options.sortExt.field+'_'+gPrefixCol[options.sortExt.collnameExt]+'_'+options.sortExt.fieldExt;
    var fesc = '_a.'+fescL;
    var feid = '_a.'+feidL;
//    var optionsExt = query.options.sortExt;
//    delete query.options.sortExt;

    var queryFind = {
        selector: query.selector,
        fields: {},
        options: {skip: options.skip, sort:  options.sort}
    }

    queryFind.fields[fesc] = 1;
    queryFind.fields[feid] = 1;

    if(options.limit) {
        queryFind.options.limit = options.limit + options.skip;
        queryFind.options.skip = 0;
    }

    var queryWhilst = queryFind;
    queryWhilst.fesc = fescL;
    queryWhilst.feid = feidL;
    queryWhilst.resArray = [];
    queryWhilst.buffArray = [];
    queryWhilst.lengthResArray = queryFind.options.limit;
    queryWhilst.count = 0;
    queryWhilst.start = 0;
    queryWhilst.end = maxInt;
    queryWhilst.sort = options.sort[fesc];
    queryWhilst.optionsExt = query.options.sortExt;

    if(query.options.count === true) queryWhilst.countRes = '_';

    async.whilst(
        function () { return queryWhilst.count < queryWhilst.lengthResArray; },
        function (callback) {
            findSortJoin.call(self, queryWhilst, callback);
        },
        function (err) {
            if(err) callabck(err);
            else {
                var result = _.sortBy(queryWhilst.resArray, function(data){ return data.score});
                if(queryWhilst.sort === -1) result = result.reverse();
                var resultId = result.splice(query.options.skip, query.options.limit);
                var res = [];
                for(var i = 0; i < resultId.length; i++){
                    res.push(resultId[i].id);
                }
                callback(null, res, queryWhilst.countRes);
            }
        }
    );
}

//функция набора результирующего массива
function findSortJoin (query, callback){
    var self = this;
    if(query.countRes === '_') query.options.count = true;
    mongoFind.call(self, query, function(err, res, count){
        if(err) callback (err);
        else {
            if(!_.isNull(count)) {
                delete  query.options.count;
                query.countRes = count;
            }
            parseScore.call(self, query, res, callback);
        }
    });
}

//функция возврата массива объектов для пути
function returnArrayReg(aID, field){
    var res = [];

    for(var i = 0; i < aID.length; i++){
        res.push(new RegExp('^'+aID[i]+'_'));
    }
    var obj  = {};
    obj['_a.'+field] = {$in:res};
    return obj;
}

// поиск наличия изменённых полей
// doc - {collname, field, _a}
function maxScroe (doc){
    var max = 0;

    for(var i = 0;  i < doc._a.length; i++){
        if(doc._a[i][doc.field] && doc._a[i][doc.field] >= max) max = doc._a[i][doc.field];
    }

    var bscore = gBuffScore[doc.collname].id;
    var keys = _.keys(bscore);

    var newScore = [];
    var oldScore = [];
    var delScore = [];

    for(var i = 0; i < keys.length; i++){
        if(bscore[keys[i]].old <= max){
            if(bscore[keys[i]].new > max) oldScore.push(keys[i]);
            else delScore.push(keys[i]);
        } else if(bscore[keys[i]].new < max) newScore.push(keys[i]);
    }

    var resObj = {
        newScore: newScore,
        oldScore: oldScore,
        delScore: delScore,
        max: max
    }
    if(newScore.length > 0 || oldScore.length > 0 || delScore.length) resObj.aflag = 1;

    return resObj;
}

function minScroe (doc){
    var min = maxInt;

    for(var i = 0;  i < doc._a.length; i++){
        if(doc._a[i][doc.field] && doc._a[i][doc.field] <= maxInt) min = doc._a[i][doc.field];
    }

    var bscore = gBuffScore[doc.collname].id;
    var keys = _.keys(bscore);

    var newScore = [];
    var oldScore = [];
    var delScore = [];

    for(var i = 0; i < keys.length; i++){
        if(bscore[keys[i]].old >= min){
            if(bscore[keys[i]].new < min) oldScore.push(keys[i]);
            else delScore.push(keys[i]);
        } else if(bscore[keys[i]].new > min) newScore.push(keys[i]);
    }

    var resObj = {
        newScore: newScore,
        oldScore: oldScore,
        delScore: delScore,
        min: min
    }
    if(newScore.length > 0 || oldScore.length > 0 || delScore.length) resObj.aflag = 1;

    return resObj;
}

function returnCompactDocScore(query, res){
    var bScore = [];
    var fid = query.feid;
    var fscore = query.fesc;

    for (var i = 0; i < res.length; i++){
        var rObj = {_id: res[i]._id, score: []};
        var _a = res[i]._a;
        for(var z = 0; z < _a.length; z++){
            if(_a[z][fid]){
                rObj.score.push({
                    _id: _a[z][fid],
                    score: _a[z][fscore]
                });
            }
        }
        bScore.push(rObj);
    }
    return bScore;
}

// функция возварт веса
// collname
function parseScore (query, res, callback){
    var self = this;
//    var fid = query.feid;
//    var fscore = query.fesc;

    var newScore = [];
    var bScore = returnCompactDocScore (query, res);

    if(bScore.length !== 0){
        var lastDoc = bScore[bScore.length-1].score;

        if(query.sort === 1){
            var max = 0;
            for(var i = 0;  i < lastDoc.length; i++){
                if(lastDoc[i].score >= max) max = lastDoc[i].score;
            }
            query.end = max;
        }
        else {
            var min = maxInt;
            for(var i = 0;  i < lastDoc.length; i++){
                if(lastDoc[i].score <= min) min = lastDoc[i].score;
            }
            query.start = min;
        }


        var newScore = [];
        var oldScore = [];
        var bscore = gBuffScore[query.optionsExt.collnameExt].id;
        var keys = _.keys(bscore);

        if(query.sort === 1){
            for(var i = 0; i < keys.length; i++){
                if(bscore[keys[i]].old <= max) oldScore.push(keys[i]);
                else if(bscore[keys[i]].new < max) newScore.push(keys[i]);
            }
        }
        else {
            for(var i = 0; i < keys.length; i++){
                if(bscore[keys[i]].old >= min) oldScore.push(keys[i]);
                else if(bscore[keys[i]].new > min) newScore.push(keys[i]);
            }
        }
    }

    if(newScore.length > 0) {
        var sel = _.extend({}, query.selector);
        if(sel['$and']) sel['$and'].push(returnArrayReg(newScore, query.optionsExt.field));
        else sel['$and'] = [returnArrayReg(newScore, query.optionsExt.field)];
        var findQuery = {
            selector: sel,
            fields: query.fields,
            options: {limit: query.options.limit}
        };

        mongoFind.call(self, findQuery, function(err, result, count){
            if(err) callabck (err);
            else {
                result = returnCompactDocScore (query, result);
                // вставка в резульат толкьо новых значений
                var dublflag;
                for(var i = 0; i < result.length; i++){
                    dublflag = 0;
                    for(var z = 0; z < bScore.length; z++){
                        if(result[i]._id === bScore[z]._id){
                            dublflag = 1 ;
                            break;
                        }
                    }
                    if(dublflag === 0) bScore.push(result[i]);
                }
                pushResScore(query, bScore);
                if(res.length < query.options.limit) query.count = query.lengthResArray;
                else  query.count = query.resArray.length;
                query.options.skip = query.options.limit + query.options.skip ;
                query.options.limit = query.options.skip + query.options.limit - query.resArray.length;
                callback(null, null);
            }
        });
    }
    else {
        pushResScore(query, bScore);
        if(res.length < query.options.limit) query.count = query.lengthResArray;
        else  query.count = query.resArray.length;
        query.options.skip = query.options.limit + query.options.skip ;
        query.options.limit = query.options.skip + query.options.limit - query.resArray.length;
        callback(null, null);
    }
}

function pushResScore (query, data){

    var start = query.start;
    var end = query.end;

    // провекра на наличие документов в резульатте или буфере
    var resData = [];
    var dublflag;
    for(var z = 0; z < data.length; z++){
        dublflag = 0;
        for(var i =0; i < query.buffArray.length; i++){
            if(query.buffArray[i].id === data[z]._id) {
                dublflag = 1;
                break;
            }
        }
        if(dublflag === 0){
            for(var i =0; i < query.resArray.length; i++){
                if(query.resArray[i].id === data[z]._id) {
                    dublflag = 1;
                    break;
                }
            }
        }
        if(dublflag === 0) resData.push(data[z]);
    }
    data = resData;

    var bscore = gBuffScore[query.optionsExt.collnameExt].id;

    for(var i = 0; i < data.length; i++){
        var max = 0;
        var min = maxInt;
        for(var z = 0; z < data[i].score.length; z++){
            if(bscore[data[i].score[z]._id]){
                data[i].score[z].score = bscore[data[i].score[z]._id].new;
            }
            if(data[i].score[z].score > max) max = data[i].score[z].score;
            if(data[i].score[z].score < min) min = data[i].score[z].score;
        }
        data[i].max = max;
        data[i].min = min;
    }

    if(query.sort === 1){
        var buffArray = [];
        for(var i = 0; i < query.buffArray.length; i++){
            if(query.buffArray[i].score <= end) {
                query.resArray.push({id:  query.buffArray[i].id, score:  query.buffArray[i].score});
            } else buffArray.push(query.buffArray[i]);
        }

        query.buffArray = buffArray;

        for(var i = 0; i < data.length; i++){
            if(data[i].min <= end) {
                query.resArray.push({id: data[i]._id, score: data[i].min});
            }
            else {
                query.buffArray.push({id: data[i]._id, score: data[i].min});
            }
        }
    } else {
        var buffArray = [];
        for(var i = 0; i < query.buffArray.length; i++){
            if(query.buffArray[i].score >= start) {
                query.resArray.push({id:  query.buffArray[i].id, score:  query.buffArray[i].score});
            } else buffArray.push(query.buffArray[i]);
        }

        query.buffArray = buffArray;

        for(var i = 0; i < data.length; i++){
            if(data[i].max >= start) {
                query.resArray.push({id: data[i]._id, score: data[i].max});
            }
            else {
                query.buffArray.push({id: data[i]._id, score: data[i].max});
            }
        }

    }

    if(data.length <  query.options.limit){
        query.resArray = query.resArray.concat(query.buffArray);
    }

    if(query.sort === 1) query.start = query.end;
    else query.end = query.start;

}

// функция find к mongodb
function mongoFind (query, callback){
    var cursor = this.coll.find(query.selector, query.fields, query.options);

    if(query.options && query.options.count ==  1){
        async.parallel({
                toArray: function(callback){
                    cursor.toArray(function(err, res){
                        if(err) callback (err);
                        else {
                            callback(null, res);
                        }
                    });
                },
                count: function(callback){
                    cursor.count(function(err, count) {
                        callback(null, count)
                    });
                }
            },
            function(err, results) {
                if(err) callback (err);
                else callback(null, results.toArray, results.count)
            });
    } else {
        cursor.toArray(function(err, res){
            if(err) {
                callback (err);
            }
            else {
                callback(null, res, null);
            }
        });
    }


//    cursor.explain(function(err, doc) {
//        console.log("------ Explanation");
//        console.dir(doc);
////        console.dir(doc.millis);
//    });

}

// подмена результата поиска
function exchangeResPath (collname, doc){
//    console.log('doc', doc);
//    console.log('gModifyNewPath', gModifyNewPath);
    if(gFieldDepPath[collname]){
        var fields = _.keys(gFieldDepPath[collname]);
        for(var i = 0; i < fields.length ; i++){
            if(doc[fields[i]] && gModifyNewPath[gFieldDepPath[collname][fields[i]][0]]){
                var namePath = gFieldDepPath[collname][fields[i]][0];
//                var index = returnIndexCollPath (namePath, collname);

                var resPath = [];
                var delId = [];
                for(var z = 0; z < doc[fields[i]].length; z++){
                    var point = doc[fields[i]][z].split('_');
                    if(gModifyNewPath[namePath][point[0]]){
                        delId.push(point[0]+'_');
                        for(var x = 0; x < gModifyNewPath[namePath][point[0]].length; x++){
                            resPath.push(gModifyNewPath[namePath][point[0]][x]+'_'+doc[fields[i]][z]);
                        }
                    }
                }

                if(delId.length > 0){
                    for(var x =0; x < delId.length; x++){
                        var newPath = []
                        for(var z = 0; z < doc[fields[i]].length; z++){
                            if((doc[fields[i]][z]+'_').indexOf(delId[x]) < 1) newPath.push(doc[fields[i]][z]);
                        }
                        doc[fields[i]] = newPath;
                    }
                }

                var uniqPath = _.uniq(resPath);
                doc[fields[i]] = doc[fields[i]].concat(uniqPath);
            }
        }
    }
//    console.log('doc2', doc);
}


// функция постобработки запрсоа find
function endFind (err, results, count){
    if(err) this.callback(err);
    else {
        var res = {};
        res.result = updateResult(this.collname, results, this.flag_a);
        if(!this.noUpdatePath) {
            for(var i = 0; i < res.result.length ; i++){
                exchangeResPath(this.collname, res.result[i]);
            }
        }
        if(this.old_path) {
            resOldPath(res.result);
        }
        if(!_.isNull(count)) res.count = count;
        this.callback (null, res);
    }
}

function resOldPath(data){
       for(var i = 0; i < data.length; i++){
           if(gModifyOldPath[data[i]._id]){
               fields = _.keys(gModifyOldPath[data[i]._id]);
               for(var z = 0; z < fields.length; z++){
                    if(data[i][fields[z]]){
                        data[i][fields[z]] = gModifyOldPath[data[i]._id][fields[z]];
                    }
               }
           }
       }
}

// функция подмены запроса к массиву на обращение _a
function add_a (collname, obj){
    if(gArrayFields[collname]){
        var keys = _.keys(gArrayFields[collname]);

        for(var i = 0; i < keys.length; i++){
            if(obj[keys[i]]){
                obj['_a.'+keys[i]] = obj[keys[i]];
                delete obj[keys[i]];
            }
        }
    }

    if(gWordFields[collname]){
        var keys = _.keys(gWordFields[collname]);

        for(var i = 0; i < keys.length; i++){
            if(obj[keys[i]] && obj[keys[i]]['$in']){
                if(!obj.$and) obj.$and = []
                for(var z = 0; z < obj[keys[i]]['$in'].length; z++){
                    var bObj = {};
                    bObj['_a._w_'+keys[i]] = obj[keys[i]]['$in'][z];
                    obj.$and.push(bObj);
                }
                delete obj[keys[i]];
            }
        }
    }
    return obj;
}

// функция обрнаботки result приведение к совместимости rabbit2
function updateResult (collname, data, flag){
    var aField = gArrayFields[collname];
    var delElem = [];
    if(data[0] && data[0]['_a']){
        for(var z = 0; z < data.length; z++){
            for(var i = 0; i < data[z]['_a'].length; i++){
                var key = _.keys(data[z]['_a'][i]);
                if(key.length > 0 && aField && aField[key[0]]){
                    if(!data[z][key[0]]) data[z][key[0]] = [];
                    data[z][key[0]].push(data[z]['_a'][i][key[0]]);
                    delElem.push(i);
                }
            }

            resUnderscore(collname, data);

            if(flag){
                delElem = delElem.reverse();
                for(var i = 0; i < delElem.length; i++){
                    data[z]['_a'].splice(delElem[i],1);
                }

                data[z].sub_a = data[z]['_a'];
            }
            delete data[z]['_a'];
        }
    }

    return data;
}

// уберает _ в конце пути при возврате find
function resUnderscore (collname, data){
    var keys = []
    if(gFieldCollStart[collname]) keys = keys.concat(_.keys(gFieldCollStart[collname]));
    if(gFieldDepPath[collname]) keys = keys.concat(_.keys(gFieldDepPath[collname]));

    if(keys.length > 0){
        for(var i = 0; i < keys.length; i++){
            for(var z = 0; z < data.length; z++){
                if(data[z][keys[i]]){
                    for(var x = 0 ; x < data[z][keys[i]].length; x++){
                        if(data[z][keys[i]][x][data[z][keys[i]][x].length-1] === '_')
                        data[z][keys[i]][x] = data[z][keys[i]][x].substr(0, data[z][keys[i]][x].length-1);
                    }
                }
            }
        }
    }

}

// функция проверки колекции
function validCollname (collname){
    if(!collname){
        return (myErr('collname - обязательное поле.'));
    }

    if(!gPrefixCol[collname]){
        return(myErr('Collection '+collname+' отсутствует в справочнике префиксов.'));
    }

}

// Сохраняет документы как новые
// query - {collname, doc}
function insert (query, callback ){

    var vCollname = validCollname(query.collname);
    if(vCollname) {
        callback(vCollname);
        return;
    }

    if(!query.doc) {
        callback(myErr('query.doc - отсутвует.'));
        return;
    }

    if(_.isArray(query.doc)) {
        callback(myErr('query.doc - массив недопустим.'));
        return;
    }

    // массив функций для водопада
    var aWaterfall = startInsert (query.collname, query.doc);
    // функция водопад
    async.waterfall(aWaterfall, endInsert.bind({collname: query.collname, callback: callback}));

}

// функция возврата массива последовательных функции для вставки
function startInsert (collname, query){
    if(!gCollection[collname]) addCollection(collname);

    var self = {};
    _.extend (self, gCollection[collname]);

    var aWaterfall = [];

    if(gFieldDepPath[collname]) {
        aWaterfall.push(function (cb){
            self.old_path = true;
            createPath.call(self, query, cb);
        });

        aWaterfall.push(function (res, cb){
            incrDoc.call(self, res, cb);
        });

    } else {
        aWaterfall.push(function (cb){
            incrDoc.call(self, query, cb);
        });
    }

    if(gScore[self.collname]) {
        aWaterfall.push(function (res, cb){
            addScore.call(self, res, cb);
        });
    }

    if(gWordFields[self.collname]) {
        aWaterfall.push(function (res, cb){
            addWord(collname, res);
            cb(null, res);
        });
    }

    if(gScoreJoin[self.collname]){
        aWaterfall.push(function (res, cb){
            addJoinScore.call(self, res, cb);
        });
    }

    aWaterfall.push(function (res, cb){
        mongoInsert.call(self, res, cb);
    });

    return aWaterfall;
}

// функция добавления слов
function addWord (collname, doc){
    var keys = _.keys(gWordFields[collname]);
    var nonWordRegexp = /[^a-zA-Zа-яА-Я0-9]/gi;


    for(var i = 0; i < keys.length; i++){
        if(doc[keys[i]]){
            if(!doc._a) doc._a = [];
            var str = doc[keys[i]].toLowerCase();
            str = str.replace( nonWordRegexp, ' ' );
            str = str.trim();
            var result = str.split(' ');
            for(var z = 0; z < result.length; z++){
                var obj = {};
                obj['_w_'+keys[i]] = result[z];
                doc._a.push(obj);
            }
        }
    }

}

// функция получение и присвоение объекту _id
function incrDoc (data, callback){
    var self = this;

    clientRedis.INCR(this.collname+':count:', function (err, res){
        if(err ) callback (err);
        else {
            if(!data['_id']) data['_id'] = gPrefixCol[self.collname]+res.toString(36);
            callback (null, data);
        }
    });
}

// вставка в Mongo
function mongoInsert (doc, callback){
    var self = this;

    if(gArrayFields[self.collname]) addArray (self.collname, doc);

    self.coll.insert(doc, {w:1}, callback);
}

//функция переноса массивов в одно поле _a
function addArray(collname, doc, id){
    var keys = _.keys(gArrayFields[collname]);
    for(var i = 0; i < keys.length; i++){
        if(doc[keys[i]]){
            if(!doc['_a']) doc['_a'] = [];
            if(_.isArray(doc[keys[i]])){
                for(var z = 0 ; z < doc[keys[i]].length ; z++){
                    var obj = {}
                    if(gFieldCollStart[collname] && gFieldCollStart[collname][keys[i]]) {
                        if(id && gModifyNewPath[gFieldCollStart[collname][keys[i]]]){
                            var aDepDoc = findDepDocModify(gFieldCollStart[collname][keys[i]], id);
                        }
                        obj[keys[i]] =  doc[keys[i]][z]+'_';
                    }
                    else obj[keys[i]] =  doc[keys[i]][z];
                    doc['_a'].push(obj);
                }
            } else {
                var obj = {}
                obj[keys[i]] =  doc[keys[i]];
                doc['_a'].push(obj);
            }
            delete doc[keys[i]];
        }
    }
}

// функция создания пути
function createPath (doc, callback){
    var key = _.keys(gFieldDepPath[this.collname]);
    var self = this;
    var aQuery = [];
    for (var i = 0; i < key.length; i++){
        if(doc[key[i]]) aQuery.push([doc[key[i]], gFieldDepPath[this.collname][key[i]][0],  gFieldDepPath[this.collname][key[i]][1]]);
    }

    async.mapSeries(aQuery, addPath.bind(self), function(err, res){
        if(err) callback(err);
        else {
            for(var z = 0; z < key.length; z++){
                if(doc[key[z]]) doc[key[z]] = res[z];
            }
            callback(null, doc);
        }
    }.bind(self));
}

// функция построения пути
// data - массив [id, namePath, inColl]
function addPath (data, callback){
    var fWatefall = [];
    var oIdRes = {};
    var self = this;
    if(gPath[data[1]][0][0] !== data[2]){
        for(var i = 1; i < gPath[data[1]].length ; i++){
            if(gPath[data[1]][i][0] === data[2]){
                if(!_.isArray(data[0])) data[0] = [data[0]];
                var query = {
                    collname: data[2],
                    selector: {_id: {$in: data[0]}},
                    fields: [gPath[data[1]][i][1]]
                };

                fWatefall.unshift(function (cb){
                    find.call({updatePath: 'no', old_path: self.old_path}, this.query, cb);
                }.bind({query:query}));
                break;
            }
            else {
                var query = {
                    collname: gPath[data[1]][i][0],
                    selector: {_id: {$in: []}},
                    fields: [gPath[data[1]][i][1]]
                };

                var field = gPath[data[1]][i+1][1];
                var coll = gPath[data[1]][i][0];

                fWatefall.unshift(function (res, cb){
                    var aId = [];
                    for(var z = 0; z < res.result.length; z++){
                        var resId = selectID(res.result[z][this.field], this.coll);
                        oIdRes[res.result[z]._id] = resId;
                        aId = aId.concat(resId);
                    }
                    this.query.selector['_id']['$in'] = aId;
                    find.call({updatePath: 'no', old_path: self.old_path}, this.query, cb);
                }.bind({query:query, field:field, coll: coll}))
            }
        }
        async.waterfall(fWatefall, function(err, res){
            if(err) {
                callback (err);
            }
            else {
                for(var z = 0; z < res.result.length; z++){
                    var resId = selectID(res.result[z][gPath[data[1]][1][1]], gPath[data[1]][0][0]);
                    oIdRes[res.result[z]._id] = resId;
                }
                var result = strPath(oIdRes, data[0]);
                callback (null, result);
            }
        })

    }
}

// функция вычлениния id нужных колекций
// array - массив данных;
// collname - колекция вычленения
function selectID (data, collname){
    var prefix = gPrefixCol[collname];
    var result = [];

    for(var i = 0; i < data.length; i++){
        var aPoint = data[i].split('_');
        if(prefix === aPoint[0].substr(0,2)) result.push(aPoint[0]);
    }

    return result;
}

// функция достраивания пути
// data - {_id:[depId]}
// startId - массив стартовых id root
function strPath (data, startId){
    var result = [];
    var aPoint = [];

    for(var i = 0; i < startId.length; i++){
        aPoint.push([data[startId[i]], [startId[i]]]);
    }

    while(aPoint.length > 0){
        var point = aPoint[0];
        aPoint.shift();
        result.push(point[1]);
        if(point[0]){
            if(_.isArray(point[0])){
                for(var z = 0; z < point[0].length; z++){
                    aPoint.push([data[point[0][z]], point[1].concat(point[0][z])]);
                }
            }
        }
    }

    var aStrId = [];

    for(var i = 0; i < result.length; i++){
        result[i] = result[i].reverse();
        var strRes  = result[i][0];
        for(var k = 1; k < result[i].length; k++){
            strRes = strRes+'_'+result[i][k];
        }
        aStrId.push(strRes+'_');
    }
    return aStrId;
}


// функция возврата массива последовательных функции для вставки
function endInsert (err, res){
    if(err) this.callback (err);
    else {
        var result = updateResult(this.collname, res);
        this.callback (null, result[0]);
    }
}

// функция переноса массива на один уровень
function flatten(data){
    var result = [];
    for(var i = 0; i < data.length; i++){
        for(var z = 0; z < data[i].length; z++){
            result.push(data[i][z]);
        }
    }
    if(this.limit)result = result.splice(this.skip, this.limit);
    return result;
}

// Изменяет документы соответствующие запросу
// query - {collname, doc}
function modify (query, callback ){

    var vCollname = validCollname(query.collname);
    if(vCollname) {
        callback(vCollname);
        return;
    }

    if(!query.doc) {
        callback(myErr('query.doc - отсутвует.'));
        return;
    }

    if(_.isArray(query.doc)) {
        callback(myErr('query - массив недопустим.'));
        return;
    }

    // массив функций для водопада
    var aWaterfall = startModify (query.collname, query.doc);
    // функция водопад
    async.waterfall(aWaterfall, endModify.bind({callback: callback}));

};


// функция создание водопада для модификации
function startModify (collname, query){
    if(!gCollection[collname]) addCollection(collname);

    var self = {};
    _.extend (self, gCollection[collname]);

    var aWaterfall = [];


    aWaterfall.push(function (cb){
        process.nextTick(cb.bind(null, null, null));
    });

    if(gFieldDepPath[collname]) {
        aWaterfall.push(function (res, cb){
            self.old_path = true;
            createPath.call(self, query.properties, cb);
        });
    }

    if(gAllFieldDepPath[collname]) {
        aWaterfall.push(function (res, cb){
            checkUpdatePath.call(self, query, function(err, result){
                if(err) cb (err);
                else {
                    cb(null, res);
                }
            });
        });
    }

    if(gScore[collname]) {
        aWaterfall.push(function (res, cb){
            self._id = query.selector._id;
            addScore.call(self, query.properties, cb);
        });
    }

    if(gScoreJoin[collname]){
//        aWaterfall.push(function (res, cb){
//            var keys = _.keys(gScoreJoin[collname]);
//            var fields = [];
//            for(var i = 0; i < keys.length; i++){
//                var subkeys = _.keys(gScoreJoin[collname][keys[i]]);
//                for(var z = 0; z < subkeys.length; z++){
//                    var sfiled = _.keys(gScore[subkeys[z]]);
//                    for (var x = 0; x < sfiled.length; x++){
//                        fields.push([keys[i]+'_'+gPrefixCol[subkeys[z]]+'_'+sfiled[x], keys[i]+'_'+gPrefixCol[subkeys[z]]+'_id']);
//                    }
//                }
//            }
//            find(null, fields);
//        });

        aWaterfall.push(function (res, cb){
            addJoinScore.call(self, query.properties, cb);
        });
    }

    if(gWordFields[collname]){
        aWaterfall.push(function (res, cb){
            addWord(collname, query.properties);
            cb(null, null);
        });
    }

    aWaterfall.push(function (res, cb){
        findAndModify.call(self, query, cb);
    });

    return aWaterfall;
}

function returnIndexCollPath (namepath, collname){
    for(var z = 0; z < gPath[namepath].length; z++){
        if(gPath[namepath][z][0] === collname)  return z;
    }
}

// функция проверки изменения пути
function checkUpdatePath (query, callback){

    var collname = this.collname;
    var keys = _.keys(gAllFieldDepPath[collname]);
    var fields = [];
    var collnamePath = [];
    var namePath = [];
    var indexCollPath = [];

    for(var i = 0; i < keys.length; i++){
        if(query.properties[keys[i]]) {
            fields.push(keys[i]);
            if(gFieldDepPath[collname] && gFieldDepPath[collname][keys[i]]) collnamePath.push(gFieldDepPath[collname][keys[i]][1]);
            else  collnamePath.push(gPath[gAllFieldDepPath[collname][keys[i]]][0][0]);
            namePath.push(gAllFieldDepPath[collname][keys[i]]);
            indexCollPath.push(returnIndexCollPath(gAllFieldDepPath[collname][keys[i]], collname))
        }
    }

    var qFind = {
        collname: collname,
        selector: {_id: query.selector._id},
        fields: fields
    }


    if(fields.length > 0){
        find(qFind, function (err, res){
            if(err) callback (err);
            else {
                for(var i = 0; i < fields.length; i++){
                    var aId;
                    var falgUOld;
                    if(gModifyOldPath[query.selector._id] && gModifyOldPath[query.selector._id][fields[i]]){
                        falgUOld = 1;
                        aId =  selectID(gModifyOldPath[query.selector._id][fields[i]], collnamePath[i]);
                    } else {
                        aId =  selectID(res.result[0][fields[i]], collnamePath[i]);
                    }
                    var aNewId =  selectID(query.properties[fields[i]], collnamePath[i]);

                    if(gModifyPath[namePath[i]]) deleteDataPath(namePath[i], query.selector._id);

                    var uId = _.union(aId, aNewId);
                    var repeatID = _.intersection(aId, aNewId);

                    if(uId.length != repeatID.length){
                        var result;
                        if(!falgUOld){
                            if(!gModifyOldPath[query.selector._id]) gModifyOldPath[query.selector._id] = {};
                            gModifyOldPath[query.selector._id][fields[i]] = res.result[0][fields[i]];
                            result = res.result[0][fields[i]];
                        } else {
                            result = gModifyOldPath[query.selector._id][fields[i]];
                        }

                        if(!gModifyNewPath[namePath[i]]) gModifyNewPath[namePath[i]] = {};
                        gModifyNewPath[namePath[i]][query.selector._id] = query.properties[fields[i]];

                        var queryUpdatePath = {
                            _id: query.selector._id,
                            old_path:  returnPath(result),
                            new_path:  returnPath(query.properties[fields[i]]),
                            oldId: aId,
                            newId: aNewId,
                            union: uId,
                            namePath: namePath[i],
                            indexCollPath: indexCollPath[i]
                        }
                        saveUpdatePath(queryUpdatePath);
                        var aDepDoc = findDepDocModify(namePath[i], query.selector._id);
//                        updatePathDep(namePath[i], aDepDoc);
                    } else {
                        if(gModifyOldPath[query.selector._id]) delete gModifyOldPath[query.selector._id];
                        if(gModifyNewPath[namePath[i]] && gModifyNewPath[namePath[i]][query.selector._id]) delete gModifyNewPath[namePath[i]][query.selector._id];
                    }
                }
                callback (null, null);
            }
        });
    } else callback (null, null);
}

//поиск в справочнике изменённых докуметов связанных с измен
function findDepDocModify (path, id){
    var resArray = [];
    if(gModifyNewPath[path]){
        var aId = _.keys(gModifyNewPath[path]);
        for(var i = 0; i < aId.length; i++){
            for(var z = 0; z < gModifyNewPath[path][aId[i]].length; z++){
                if(gModifyNewPath[path][aId[i]][z].indexOf(id+'_') !== -1) {
                    resArray.push(aId[i]);
                    break;
                }
            }
        }
    }

    return resArray;
}


//перестроение пути документов у которых изменилось связь в пути
function updatePathDep (path, aDepDoc){

    var query = [];
    if(gModifyNewPath[path]){
        for(var i = 0; i < aDepDoc.length; i++){
            var obj = [];
            obj.collname =  grePrefixCol[aDepDoc[i].substr(0,2)];
            obj.field = findRootPath(gModifyNewPath[path][aDepDoc[i]]);
            query.push(obj);
        }
    }

}

// поиск корневых элементов пути
function findRootPath (path){
    var res = [];
    for(var  i =0 ; i < path.length; i++){
        var buffArray = path[i].split('_');
        if(buffArray.length === 2) res.push(buffArray[0]);
    }
    return res;
}

// удаление старых данных из буфера пути
function deleteDataPath (namepath, id){
    var keys = _.keys(gModifyPath[namepath]);
    for(var i = 0; i < keys.length; i++){
        var newArrayAdd = [];
        for(var z = 0; z < gModifyPath[namepath][keys[i]].add.length ; z++){
            if(gModifyPath[namepath][keys[i]].add[z] !== id+'_'){
                newArrayAdd.push(gModifyPath[namepath][keys[i]].add[z]);
            }
        }
        gModifyPath[namepath][keys[i]].add = newArrayAdd;

        var newArrayDel = [];
        for(var z = 0; z < gModifyPath[namepath][keys[i]].del.length ; z++){
            if(gModifyPath[namepath][keys[i]].del[z]._id !== id){
                newArrayDel.push(gModifyPath[namepath][keys[i]].del[z]);
            }
        }
        gModifyPath[namepath][keys[i]].del = newArrayDel;

        if(gModifyPath[namepath][keys[i]].del.length === 0 && gModifyPath[namepath][keys[i]].add.length === 0){
            delete gModifyPath[namepath][keys[i]];
        }
    }

}

//функция разбора и переворачивания пути
function returnPath (path){
    var res = [];
    var length = 0;
    for(var i = 0; i < path.length; i++){
        var bArray = path[i].split('_');
        if(bArray[bArray.length-1] === '') bArray.splice(bArray.length-1,1);
        if(bArray.length > length) length = bArray.length;
        res.push(bArray.reverse());
    }

    var aPath = [];
    for(var i = 0; i < res.length; i++){
        if(res[i].length === length) aPath.push(res[i]);
    }

    return aPath;
}

//функция формирование записи о модифицированном пути
function saveUpdatePath(query){
    for(var i = 0; i < query.union.length; i++){
        var index = _.indexOf(query.oldId, query.union[i]);
        if(index === -1){
            for(var z = 0; z < query.new_path.length; z++){
                if(query.new_path[z][0] ===  query.union[i]){
                    var addPoint = pointDifference(query.old_path,  query.new_path[z]);
                    if(addPoint.length > 0) addGModifyPath ( query.indexCollPath, query.namePath, query._id, addPoint, query.old_path[z], 'add');
                }
            }
        } else {
            for(var z = 0; z < query.old_path.length; z++){
                if(query.old_path[z][0] ===  query.union[i]){
                    var addPoint = pointDifference(query.new_path,  query.old_path[z]);
                    if(addPoint.length > 0) addGModifyPath(query.indexCollPath ,query.namePath, query._id, addPoint, query.old_path[z]);
                }
            }
        }
    }
}

// функция добавления модифицированного пути в справочник
function addGModifyPath (index, namepath, id, path, fullPath, flag){
    if(flag === 'add'){
        for(var i = 0; i < path.length ; i++){
            if(!gModifyPath[namepath]) gModifyPath[namepath] = {};
            if(!gModifyPath[namepath][path[i]]) gModifyPath[namepath][path[i]] = {add:[id+'_'], del:[]};
            else gModifyPath[namepath][path[i]].add.push(id);
        }
    } else {
        for(var i = 0; i < path.length ; i++){
            var indexL = _.indexOf(fullPath, path[i]);
            var str = '';
            for(var z = indexL; z >= 0; z--){
                str = str+fullPath[z]+'_';
            }
            str = str+id+'_';
            if(!gModifyPath[namepath]) gModifyPath[namepath] = {};
            if(!gModifyPath[namepath][path[i]]) gModifyPath[namepath][path[i]] = {add:[], del:[{d: str, i: index, _id: id}]};
            else {
                gModifyPath[namepath][path[i]].del.push({d: str, i: index, _id: id});
            }
        }
    }
}

//функция нахождения уровней расхождения
function pointDifference (aPath, onePath){
    var newPoint = [];
    var flag = 0;
    for(var i = 0; i < onePath.length; i++){
        flag = 0;
        for(var z = 0; z < aPath.length; z++){
            if(aPath[z][i] === onePath[i]){
                flag = 1;
                break;
            }
        }
        if(flag === 0) newPoint.push(onePath[i]);
    }
    return newPoint;
}

// функция создание водопада для модификации
function endModify (err, res){
    if(err) this.callback (err);
    else this.callback(null, res);
}

// функция атомарного обновления Mongodb
function findAndModify (query, callback){

    var chek = false;

    if(gArrayFields[this.collname]) chek = checkProperties (this.collname, query.properties);

    if(chek) modifyArray.call (this, query, callback);
    else {
        this.coll.findAndModify(query.selector, {_id:1}, {$set: query.properties}, {w:1, new:true}, callback);
    }
}

// функция проверки существоания поля с массивом
function checkProperties(collname, data){

    var keys = _.keys(data);

    for(var i = 0; i < keys.length; i++){
         if(gArrayFields[collname][keys[i]]) return true;
    }
}

// функция модификации докмуента с массивом
function modifyArray (query, callback){
    var self = this;
    var collname = this.collname;
    var selector = query.selector;
    var doc = query.properties;

    var aWaterfal = [];

    aWaterfal.push(function(cb){
        var query = {
            collname: collname,
            selector: selector,
            fields: ['_a']
        }
        find.call({flag_a: true},query, cb);
    });

    aWaterfal.push(function(res, cb){
        delete res.result[0]['_id'];
        if(res.result[0].sub_a) doc = extend_a(res.result[0], doc);
        else doc = _.extend(res.result[0], doc);
        addArray(collname, doc, query.selector._id);
        cb(null, doc);
    });

    async.waterfall(aWaterfal, function (err, res){
        if(err) callback(err);
        else  self.coll.findAndModify(query.selector, {_id:1}, {$set: res}, {w:1, new:true}, callback);
    });
}

// объединение элементов _a и sub_a
function extend_a(resFind, doc){

    if(resFind.sub_a){
        var delArray = [];

        for(var i = 0; i < resFind.sub_a.length; i++){
            var keys = _.keys(resFind.sub_a[i]);
            for(var z = 0; z < doc._a.length; z++){
                if(doc._a[z][keys[0]]) delArray.push(i);
            }
        }

        delArray = delArray.reverse();
        for(var i = 0; i < delArray.length; i++){
            resFind.sub_a.splice(delArray[i], 1);
        }

        if(doc['_a']) doc['_a'] = doc['_a'].concat(resFind.sub_a);
        delete resFind.sub_a;
    }
    doc = _.extend(resFind, doc);
    return doc;
}

// Удаляет все документы соответствующие запросу
// query - {collname, doc}
var remove = function(query, callback ){
    var collName = query.collname;

    if(!gCollection[collName]) addCollection(collName);

    var self = {};
    _.extend (self, gCollection[collName]);

    findDepDoc(collName, query.doc, function(err){
        if(err) callback(err);
        else findAndRemove.call(self, query.doc, callback)
    });

};

// функция атомарного удаления Mongodb
function findAndRemove (query, callback){

    this.coll.findAndRemove(query, {_id:1},  {w:1}, function(err, res){
        if(res){
            var buff = {_id: query._id};
            callback(null, buff);
        } else {
            if(!err) callback();
            else callback(err);
        }
    });
}

// функция создания индекса для внешней сортировки
// query - {collname, field}
function createSortIndex (query, callback){

    var vCollname = validCollname(query.collname);

    if(vCollname) {
        callback(vCollname);
        return;
    }

    var collName = query.collname;
    if(!gCollection[collName]) addCollection(collName);

    var self = {};
    _.extend (self, gCollection[collName]);

    // массив функций для водопада
    var aWaterfall = startCreateSortIndex (query);
    // функция водопад
    async.waterfall(aWaterfall, endCreateSortIndex.bind({self: self, callback: callback}));
}

// функция создания водопада для перестроения индекса
function startCreateSortIndex (query){
    var aWatrfall = [];
    var field = query.field;

    aWatrfall.push(function (cb){
        var queryFind = {
            collname: query.collname,
            selector: {},
            fields: [field],
            options: {sort:{}}
        };

        queryFind.options.sort[query.field] = 1;

        find(queryFind, cb) ;
    });

    aWatrfall.push(function(res, cb){
        var rLength = res.result.length;

        var aVal = [];
        for(var i = 0 ; i < rLength; i++){
            aVal.push(res.result[i][field]);
        }

        var unVal = _.union(aVal);
        var length = unVal.length+1;

        var aScore = [];
        var step = parseInt(maxInt/length);

        for (var i = 0; i < rLength; i++){
            var obj = {selector:{_id: res.result[i]._id}, properties:{}};
            var index = _.indexOf(unVal,  res.result[i][field]);
            obj.properties['_s_'+field] = step*(index+1);
            aScore.push(obj);
        }
        delete gBuffScore[query.collname];
        delete buffScore[query.collname];
        delete buffReScore[query.collname];
        cb (null, aScore);
    });

    return aWatrfall;
}

// функция добавления веса
function addScore (doc, callback){
    var keys = _.keys(gScore[this.collname]);
    var aQuery = [];
    for(var i = 0; i < keys.length; i++){
        if(doc[keys[i]]){
            var obj = {
                collname: this.collname,
                field: keys[i],
                val: doc[keys[i]]
            };

            if(this._id) obj.id = this._id;
            aQuery.push(obj);
        }
    }

    async.map (aQuery, findScore, function(err, res){
        if(err) callback(err);
        else {
            for(var i = 0; i < keys.length; i++){
                doc['_s_'+keys[i]] = res[i];
            }
            callback (null, doc);
        }
    });
}


// функция добавления веса
function addJoinScore (doc, callback){

    var keys = _.keys(gScoreJoin[this.collname]);
    var aQuery = [];
    for(var i = 0; i < keys.length; i++){
        if(doc[keys[i]]) {
            var subkeys = _.keys(gScoreJoin[this.collname][keys[i]]);
            for(var z = 0; z < subkeys.length; z++){
                aQuery.push({
                    collname: subkeys[z],
                    field: keys[i],
                    val: doc[keys[i]]
                });
            }
        }
    }

    async.map (aQuery, findJoinScore, function(err, res){
        if(err) callback(err);
        else {
            res = flatten(res);
            addArrayScore(doc, res);
            callback (null, doc);
        }
    });
}

// функция вставки в массив веса
function addArrayScore (doc, aScore){
    for(var i = 0; i < aScore.length; i++){
        if(!doc['_a']) doc['_a'] = [];
        doc['_a'].push(aScore[i]);
    }
}

// функция поиска приджойненных весов
// query - {collname, field, val]
function findJoinScore (query, callback){
    var collname =  query.collname;
    var id = selectID(query.val, collname);

    var fields = _.keys(gScore[collname]);

    var prefixFields = [];
    for(var i = 0; i < fields.length; i++){
        prefixFields[i] = '_s_'+fields[i];
    }

    var queryFind = {
        collname: collname,
        selector: {'_id':{'$in':id}},
        fields: prefixFields
    }

    find(queryFind, function (err, res){
        if(err) callback (err);
        else {
            var result = [];
            for(var i = 0; i < res.result.length; i++){
                for(var z = 0; z < fields.length; z++){
                    var obj = {};
                    obj[query.field+'_'+gPrefixCol[collname]+'_id'] = res.result[i]._id;
                    var score =  res.result[i][prefixFields[z]];
                    if(gBuffScore[collname] && gBuffScore[collname].id && gBuffScore[collname].id[res.result[i]._id]) score = gBuffScore[collname].id[res.result[i]._id].old;
                    obj[query.field+'_'+gPrefixCol[collname]+'_'+fields[z]] = score;
                    result.push(obj);
                }
            }



            callback(null, result);
        }
    })

}

// функция поиска соседей
// query - {collname, field, val, id}
function findScore (query, callback){

    var collName = query.collname;
    if(!gCollection[collName]) addCollection(collName);
    var field = query.field;
    var val = query.val;

    var aParallel = [];
    var queryMax = {
        collname: collName,
        selector: {},
        options:{limit:1, sort: {}},
        fields: ['_s_'+field, field]
    };

//    if(query.id) queryMax.selector._id = {$ne: query.id};
    queryMax.selector[field] = {'$gte': val};
    queryMax.options.sort[field] = 1;

    aParallel.push(function(cb){
        find(queryMax, cb);
    });


    var queryMin = {
        collname: collName,
        selector: {},
        options:{limit:1, sort: {}},
        fields: ['_s_'+field, field]
    };

//    if(query.id) queryMin.selector._id = {$ne: query.id};
    queryMin.selector[field] = {'$lte': val};
    queryMin.options.sort[field] = -1;

    aParallel.push(function(cb){
        find(queryMin, cb);
    });

    if(query.id){
        var queryOld = {
            collname: collName,
            selector: {_id: query.id},
            options:{},
            fields: ['_s_'+field, field]
        };

        aParallel.push(function(cb){
            find(queryOld, cb);
        });
    }

    async.parallel(aParallel,
        function(err, res){
            if(err) callback (err);
            else {
                var score;
                if(!res[0].result[0] || !res[0].result[0]['_s_'+field]) res[0] = maxInt;
                else {
                    if(res[0].result[0][field] === val) score = res[0].result[0]['_s_'+field];
                    else res[0] = res[0].result[0]['_s_'+field];

                }
                if(!score){
                    if(!res[1].result[0] || !res[1].result[0]['_s_'+field]) res[1] = 0;
                    else res[1] = res[1].result[0]['_s_'+field];
                }


                if(!score) {
                    score = (res[0] + res[1])/2;

                    if(!buffScore[collName]) {
                        buffScore[collName] = {};
                        buffReScore[collName] = {};
                    }

//                    if(query.id && buffScore[collName][res[2].result[0]['_s_'+field]]){
//                        delete  buffScore[collName][res[2].result[0]['_s_'+field]];
//                        delete  buffReScore[collName][res[2].result[0][field]];
//                    }

                    if(buffReScore[collName][val]) score = buffReScore[collName][val];
                    else {
                        if(buffScore[collName][score]){
                            if(val !== buffScore[collName][score]){
                                score = findScoreBuff(collName, score, val, res[0], res[1])
                            }
                        }
                    }

                    buffScore[collName][score] = val;
                    buffReScore[collName][val] = score;

                    if(query.id) addBuffScore(collName, query.id, res[2].result[0]['_s_'+field], score);
                }
                callback (null, score);
            }
        });
}

//функция добавления в буффер веса
function addBuffScore (collname, id, oldScore, newScore){

    if(!gBuffScore[collname]) gBuffScore[collname] = {id:{}};
    var gBuff = gBuffScore[collname];

    gBuff.id[id] = {old: oldScore, new: newScore};




}

//функция поиска веса если начались "гонки"
function findScoreBuff (collname, score, val, max, min){
    var bScore = buffScore[collname];
    var aScore = _.keys(bScore);
    for(var i = 0; i < aScore.length; i++) aScore[i] = parseInt(aScore[i]);
    var index = _.indexOf(aScore, score);
    if(val > bScore[score]){
        var bIndex;
        for(var i = index; i < aScore.length; i++){
            if(val > bScore[aScore[i]] && aScore[i] <= max){
                bIndex = i;
            } else break;
        }
        if(bIndex >= index && bIndex !== aScore.length-1) {
            var res = (aScore[bIndex+1] + aScore[bIndex])/2
            return res;
        } else if(bIndex === aScore.length-1) {
            var res = (aScore[bIndex] + max)/2
            return res;
        }
    } else {
        var bIndex;
        for(var i = index; i >= 0; i--){
            if(val < bScore[aScore[i]] && aScore[i] >= min){
                bIndex = i;
            }  else break;
        }
        if(bIndex <= index && bIndex !== 0) {
            var res = (aScore[bIndex-1] + aScore[bIndex])/2
            return res;
        } else {
            var res = (aScore[bIndex] + min)/2
            return res;
        }
    }
}

// обновления индекса в дочерних колекциях
// query - {collname, field, childColl, childField}
function updateChildIndex (query, callback){

    var vCollname = validCollname(query.collname);

    if(vCollname) {
        callback(vCollname);
        return;
    }

    vCollname = validCollname(query.childColl);

    if(vCollname) {
        callback(vCollname);
        return;
    }

    // массив функций для водопада
    var aWaterfall = startUpdateChildIndex (query);
    // функция водопад
    async.waterfall(aWaterfall, endUpdateChildIndex.bind({callback: callback}));
}

// функция создания водопада для перестроения индекса joina
function startUpdateChildIndex (query){

    var aWatrfall = [];
    var field = query.childField;
    var collname = query.collname;

    var pId = query.field+'_'+gPrefixCol[query.childColl]+'_id';
    var pField = query.field+'_'+gPrefixCol[query.childColl]+'_'+field;

    aWatrfall.push(function (cb){
        var queryFind = {
            collname: query.childColl,
            selector: {},
            fields: ['_s_'+field]
        };

        find(queryFind, cb) ;
    });

    aWatrfall.push(function (res, cb){
        var aQueryUpdate = [];
        var data = res.result;

        for(var i = 0; i < data.length; i++){

            var selPull = {};
            selPull['_a.'+pId] = data[i]._id;
            var upPull = {};
            upPull[pId] = data[i]._id;
            var pull = {
                collname: collname,
                selector: selPull,
                update: {'$pull':{'_a': upPull}},
                options: {multi:true, w:1}
            };

            var selPush = {};
            selPush['_a.'+query.field] = data[i]._id+'_';
            var upPush = {};
            upPush[pId] = data[i]._id;
            upPush[pField] =  data[i]['_s_'+field];
            var push = {
                collname: collname,
                selector: selPush,
                update: {'$push': {'_a': upPush}},
                options: {multi:true, w:1}
            };
            aQueryUpdate.push({pull: pull, push: push});
        }

        async.mapSeries(aQueryUpdate, updateScore, cb);
    });

    return aWatrfall;
}

//db.testContracts.update({'_a.c_id_mn_id':'mn14s'}, {$pull: { '_a': {'c_id_mn_id': 'mn14s'}}});

// функция модификации веса приджойненного
// query - {pull, push]
function updateScore (query, callback){
    async.waterfall([
        function (cb){
            update(query.pull, cb);
        },
        function (res, cb){
            update(query.push, cb);
        }
    ], callback);
}


// функция конечная для перестроения индекса joina
function endUpdateChildIndex (err, data){
    if(err) this.callback(err);
    else this.callback(null, 'Ok');
}

// Обновление всех документов
// query - {collname, selector, document, options}
function update (query, callback ){
    var collName = query.collname;
    var selector = query.selector;
    var update = query.update;
    var options = query.options;

    if(!gCollection[collName]) addCollection(collName);

    var self = gCollection[collName];

    if(gBuffScore[collName]) delete gBuffScore[collName];

    self.coll.update(selector, update, options, function(err, res) {
        if (err) callback(err);
        else callback (null, res);
    });
};

// функция конечная для перестроения индекса
function endCreateSortIndex (err, data){
    if(err) this.callback(err);
    else async.map(data, findAndModify.bind(this.self), this.callback);
}

// провека на сущестование связанных докмуентов
function findDepDoc (collname, doc, callback){
    var fParalel = [];
    if(gBackRef[collname]){
        for(var g = 0; g < gBackRef[collname].length; g++){
            fParalel.push(function(cb){
                var query = {
                    collname: gBackRef[collname][this.g][0],
                    selector: {},
                    fields: ['_id'],
                    options: {limit: 1}
                }
                query.selector[gBackRef[collname][this.g][1]] = doc._id;
                find(query, cb);
            }.bind({g: g}))
        }


        async.parallel(fParalel, function(err, res){
            if(err) callback (err);
            else {
                for(var i = 0; i < res.length; i++){
                    if(res[i].result.length > 0) {
                        callback (new Error('F0.Rabbit: E01'));
                        return;
                    }
                }
                callback (null, 'ok');
            }
        })

    } else process.nextTick(function(){callback(null, 'ok')});
}

// фнукция агрегирования
// query - {collname, pipeline}
var  aggregate = function (query, callback){
    var collectionName = query.collname;

    if(!gCollection[collectionName]) addCollection(collectionName);

    var self = {};
    _.extend (self, gCollection[collectionName]);

    self.coll.aggregate(query.pipeline, callback);

}

module.exports = {
    init: init,
    close: close,
    open: open
}