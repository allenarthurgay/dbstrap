"use strict";
var cradle = require("cradle"),
    async = require('async');

/**
 *
 * @constructor
 *
 * @param {String} name
 * @param {Object} options
 * @param {String} options.port
 * @param {String} options.secure
 * @param {String} options.cache
 * @param {String} options.username
 * @param {String} options.password
 * @param {Boolean} options.create creates db if it doesn't exist
 * @type {Function}
 */
var Database = exports.Database = function (name, options, callback) {

    var self = this;
    self.db = new (cradle.Connection)({
        host:   options.uri,
        port:   options.port,
        secure: options.secure,
        cache:  options.cache,
        raw:false,
        auth:{
            username:options.username,
            password:options.password
        }
    }).database(name);


    if(options.create){
        self.db.exists(function(err, exists){
            if(!exists){
                self.db.create();
                callback(null, true);
            }
            else{
                callback(null, false)
            }
        });
    }
    else{
        callback(null, false);
    }
};

Database.prototype.queryViewByKey = function (viewName, keyValue, skip, limit, callback) {
    var query = {key:keyValue};
    if (skip && (~~skip) > 0) {
        query.skip = skip;
    }

    if (limit && (~~limit) > 0) {
        query.limit = limit;
    }
    this.db.view(viewName, query, callback);
};

Database.prototype.queryViewByMultipleKeyValues = function (viewName, keyValues, skip, limit, callback) {
    var query = {keys:keyValues};

    if (skip && (~~skip) > 0) {
        query.skip = skip;
    }

    if (limit && (~~limit) > 0) {
        query.limit = limit;
    }
    this.db.view(viewName, query, callback);
};

Database.prototype.defineViews = function (name, options) {
    this.db.save(name, options);
};

Database.prototype.select = function (query, callback) {
    callback = callback || function () {
    };

    async.waterfall([
        function (cb) {
            this.db.get(query, cb);
        }
    ], function (err, res) {
        callback(err, res);
    });
};

Database.prototype.put = function (query, doc, callback) {
    callback = callback || function () {
    };


    async.waterfall([
        function (cb) {
            if (query) {
                this.db.save(query, doc, cb);
            }
            else {
                this.db.save(doc, cb);
            }
            ;
        }
    ], function (err, res) {
        callback(err, res);
    });
};

Database.prototype.remove = function (query, callback) {
    callback = callback || function () {
    };


    async.waterfall([
        function (cb) {
            this.db.get(query, cb);
        },
        function (result, cb) {
            this.db.remove(result.id, result._rev, cb);
        }
    ], function (err, res) {
        callback(err, res);
    });
};

