"use strict";

var DBClient = require('./dbclient').DBClient;

var mapDBClient = {};

function getDBClient(dbid) {
    if (mapDBClient.hasOwnProperty(dbid)) {
        return mapDBClient[dbid];
    }

    return undefined;
}

function newDBClient(cfg, callback) {
    let dbclient = new DBClient(cfg);
    mapDBClient[cfg.dbid] = dbclient;

    dbclient.init(callback);
}

exports.getDBClient = getDBClient;
exports.newDBClient = newDBClient;