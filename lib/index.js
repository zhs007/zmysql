"use strict";

var dbmgr = require('./dbmgr');
var dbconfig = require('./dbconfig');
var dbclient = require('./dbclient');

exports.getDBClient = dbmgr.getDBClient;
exports.newDBClient = dbmgr.newDBClient;

exports.newConfig = dbconfig.newConfig;
exports.checkConfig = dbconfig.checkConfig;

exports.noLogger = dbconfig.noLogger;
exports.defaultLogger = dbconfig.defaultLogger;

exports.DBClient = dbclient.DBClient;