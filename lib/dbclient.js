"use strict";

var dbconfig = require('./dbconfig');

var assert = require('assert');
var mysql = require('mysql');
var util = require('util');

const DBSTATE_NOINIT        = 0; // not init
const DBSTATE_INIT          = 1; // init
const DBSTATE_CONNECTING    = 2; // connecting
const DBSTATE_CONNECTED     = 3; // connexted
const DBSTATE_QUERY         = 4; // qeury
const DBSTATE_QUERYLIST     = 5; // qeury list
const DBSTATE_TRANSACTION   = 6; // transaction

class DBClient{

    // constructor
    constructor(cfg) {
        this.cfg = cfg;

        this.dbcon = undefined;

        this.state = DBSTATE_NOINIT;

        this.curCtrl = undefined;
        this.lstCtrlQuery = [];

        if (cfg.hasOwnProperty('logger')) {
            this.logger = cfg.logger;
        }
        else {
            this.logger = dbconfig.noLogger;
        }
    }

    // init
    init(callback) {
        assert.ok(this.state == DBSTATE_NOINIT || this.dbcon == undefined, 'mysql already init.');

        this.logger.info(util.format('mysql(%s) init %s %s@%s password is %s', this.cfg.dbid, this.cfg.database, this.cfg.user, this.cfg.host, this.cfg.password));

        this.state = DBSTATE_INIT;
        this.connect(callback);
    }

    // addCtrl
    __addCtrl(type, param, func) {
        this.cfg.logger.info(util.format('%j', param));

        this.lstCtrlQuery.push({type: type, param: param, func: func});
    }

    // nextCtrl
    __nextCtrl() {
        let dbclient = this;

        dbclient.state = DBSTATE_CONNECTED;

        if (dbclient.curCtrl != undefined) {
            dbclient.curCtrl = undefined;
        }

        if (dbclient.lstCtrlQuery.length > 0) {
            dbclient.curCtrl = dbclient.lstCtrlQuery.shift();

            if (dbclient.curCtrl.type == 'query') {
                dbclient.query(dbclient.curCtrl.param, dbclient.curCtrl.func);
            }
            else if (dbclient.curCtrl.type == 'querylist') {
                dbclient.queryList(dbclient.curCtrl.param, dbclient.curCtrl.func);
            }
        }
    }

    // 连接数据库
    connect(funcOnConnect) {
        assert.ok(this.state != DBSTATE_NOINIT, 'mysql not init.');

        let dbclient = this;

        if (dbclient.state == DBSTATE_INIT) {
            dbclient.state = DBSTATE_CONNECTING;

            this.logger.info(util.format('mysql connect %j', dbclient.cfg));

            dbclient.dbcon = mysql.createConnection(dbclient.cfg);
            dbclient.dbcon.on('error', function (err) {
                dbclient.onDBError(err);
            });

            dbclient.dbcon.connect(function (err) {
                if (err) {
                    dbclient.state = DBSTATE_INIT;

                    this.logger.err(util.format('mysql connect error %j', err));

                    setTimeout(function () {
                        dbclient.connect(funcOnConnect);
                    }, 5000);

                    return ;
                }

                dbclient.state = DBSTATE_CONNECTED;

                if (funcOnConnect != undefined) {
                    funcOnConnect();
                }
            });
        }
        else if (dbclient.state == DBSTATE_CONNECTED) {
            if (funcOnConnect != undefined) {
                funcOnConnect();
            }
        }
    }

    reconnect(funcOnReconnect) {
        assert.ok(this.state != DBSTATE_NOINIT, 'mysql not init.');

        let dbclient = this;
        if (dbclient.state == DBSTATE_CONNECTED) {

            dbclient.dbcon.end(function (err) {
                if (err) {
                    this.logger.err(util.format('mysql connect error %j', err));
                }

                dbclient.state = DBSTATE_INIT;

                dbclient.connect(funcOnReconnect);
            });
        }
        else if (dbclient.state == DBSTATE_INIT) {
            dbclient.connect(funcOnReconnect);
        }
    }

    onDBError(err) {
        let dbclient = this;
        if (err) {
            this.logger.err(util.format('mysql connect error %j', err));

            //if (dbclient.curSQL != undefined) {
            //    log.log('error', util.format('error query sql is %s', dbclient.curSQL));
            //}

            // 如果是连接断开，自动重新连接
            if (err.code === 'PROTOCOL_CONNECTION_LOST') {
                dbclient.state = DBSTATE_INIT;

                dbclient.reconnect();
            }
            else {
                setTimeout(function () {
                    dbclient.reconnect();
                }, 5000);
            }
        }
    }

    __query(sql, funcOnQuery) {
        let dbclient = this;
        dbclient.dbcon.query(sql, function (err, rows, fields) {
            if (dbclient.state != DBSTATE_QUERYLIST) {
                dbclient.__nextCtrl();
            }

            if (err) {
                this.logger.err(util.format('DBClient query(%s) err is %j', sql, err));
            }

            funcOnQuery(err, rows, fields);
        });
    }

    query(sql, funcOnQuery) {
        assert.ok(this.state != DBSTATE_NOINIT, 'mysql not init.');

        let dbclient = this;

        if (dbclient.state == DBSTATE_CONNECTED) {
            dbclient.state = DBSTATE_QUERY;
            dbclient.__query(sql, funcOnQuery);
        }
        else if (dbclient.state == DBSTATE_INIT) {
            dbclient.__addCtrl('query', sql, funcOnQuery);

            dbclient.connect();
        }
        else {
            dbclient.__addCtrl('query', sql, funcOnQuery);
        }
    }

    _queryList(sqlarr, begin, max, result, func) {
        let dbclient = this;
        if(begin >= max) {
            dbclient.__nextCtrl();

            func(result);

            return ;
        }

        let sql = sqlarr[begin];

        dbclient.__query(sql, function (err, rows, fields) {
            result[begin] = {err: err, rows: rows, fields: fields};
            dbclient._queryList(sqlarr, begin + 1, max, result, func);
        });
    }

    // onQueryList(results as [{err, rows, fields},...])
    queryList(sqlarr, funcOnQueryList) {
        assert.ok(this.state != DBSTATE_NOINIT, 'mysql not init.');

        let dbclient = this;
        if (dbclient.state == DBSTATE_CONNECTED) {
            dbclient.state = DBSTATE_QUERYLIST;
            let max = sqlarr.length;
            let i = 0;
            let result = [];

            dbclient._queryList(sqlarr, i, max, result, funcOnQueryList);
        }
        else if (dbclient.state == DBSTATE_INIT) {
            dbclient.__addCtrl('querylist', sqlarr, funcOnQueryList);

            dbclient.connect();
        }
        else {
            dbclient.__addCtrl('querylist', sqlarr, funcOnQueryList);
        }
    }

    isValidResult(rows, name) {
        return typeof (rows) != 'undefined' && rows.length > 0 && rows[0].hasOwnProperty(name) && rows[0][name] !== null;
    }
};

exports.DBClient = DBClient;