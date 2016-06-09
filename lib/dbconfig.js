"use strict";

var noLogger = {
    err: function (str) {

    },
    info: function (str) {

    }
};

var defaultLogger = {
    err: function (str) {
        console.log('err:' + str);
    },
    info: function (str) {
        console.log('info:' + str);
    }
};

// newConfig
function newConfig(dbid, host, user, password, database, reconnecttime, logger, port) {
    if (port == undefined) {
        port = 3306;
    }

    return {
        dbid: dbid,
        host: host,
        user: user,
        password: password,
        database: database,
        reconnecttime: reconnecttime,
        logger: logger,
        port: port
    };
}

// check cfg and set default
function checkConfig(cfg) {
    if (cfg == undefined) {
        return false;
    }

    if (!cfg.hasOwnProperty('dbid') ||
        !cfg.hasOwnProperty('host') ||
        !cfg.hasOwnProperty('user') ||
        !cfg.hasOwnProperty('password') ||
        !cfg.hasOwnProperty('database')) {
        return false;
    }

    if (cfg.hasOwnProperty('reconnecttime')) {
        cfg.reconnecttime = 5000;
    }

    if (cfg.hasOwnProperty('logger')) {
        cfg.logger = noLogger;
    }

    return true;
}

exports.newConfig = newConfig;
exports.checkConfig = checkConfig;

exports.noLogger = noLogger;
exports.defaultLogger = defaultLogger;