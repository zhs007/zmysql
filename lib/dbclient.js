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
        this.logger.info(util.format('%j', param));

        this.lstCtrlQuery.push({type: type, param: param, func: func});
    }

    // removeCtrlWithRollback
    __removeCtrl_Rollback() {
        let dbclient = this;

        for (let ii = 0; ii < dbclient.lstCtrlQuery.length; ++ii) {
            if (dbclient.lstCtrlQuery[ii].type == 'commit') {
                dbclient.lstCtrlQuery.splice(0, ii);

                return ;
            }
        }
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
                dbclient.__query(dbclient.curCtrl.param, dbclient.curCtrl.func);
            }
            else if (dbclient.curCtrl.type == 'querylist') {
                dbclient._queryList(dbclient.curCtrl.param, dbclient.curCtrl.func);
            }
            else if (dbclient.curCtrl.type == 'beginTransaction') {
                dbclient._beginTransaction(dbclient.curCtrl.func);
            }
            else if (dbclient.curCtrl.type == 'commit') {
                dbclient._commit(dbclient.curCtrl.func);
            }
        }
    }

    // 连接数据库
    connect(funcOnConnect) {
        assert.ok(this.state != DBSTATE_NOINIT, 'mysql not init.');

        let dbclient = this;

        if (dbclient.state == DBSTATE_INIT) {
            dbclient.state = DBSTATE_CONNECTING;

            dbclient.logger.info(util.format('mysql connect %j', dbclient.cfg));

            dbclient.dbcon = mysql.createConnection(dbclient.cfg);
            dbclient.dbcon.on('error', function (err) {
                dbclient.onDBError(err);
            });

            dbclient.dbcon.connect(function (err) {
                if (err) {
                    dbclient.state = DBSTATE_INIT;

                    dbclient.logger.err(util.format('mysql connect error %s(%s)', err.message, err.code));

                    setTimeout(function () {
                        dbclient.connect(funcOnConnect);
                    }, 5000);

                    return ;
                }

                dbclient.state = DBSTATE_CONNECTED;

                if (funcOnConnect != undefined) {
                    funcOnConnect();
                }

                dbclient.__nextCtrl();
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
        if (dbclient.state >= DBSTATE_CONNECTED) {

            dbclient.dbcon.end(function (err) {
                if (err) {
                    dbclient.logger.err(util.format('mysql connect error %s(%s)', err.message, err.code));
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
            dbclient.logger.err(util.format('mysql connect error %s(%s)', err.message, err.code));

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

    __reprocctrl() {
        let self = this;

        self.lstCtrlQuery.splice(0, 0, self.curCtrl);
        self.reconnect(function () {
            self.__nextCtrl();
        });
    }

    // 返回true，表示把回调抛给上层逻辑
    __onProcErr(err) {
        if (err.code == 'ECONNREFUSED' || err.code == 'PROTOCOL_CONNECTION_LOST' || err.code == 'PROTOCOL_ENQUEUE_AFTER_FATAL_ERROR' || err.code == 'ETIMEDOUT') {
            let self = this;

            self.__reprocctrl();

            return false;
        }

        return true;

    }

    __query(sql, funcOnQuery) {
        let dbclient = this;
        dbclient.dbcon.query(sql, function (err, rows, fields) {
            if (err) {
                dbclient.logger.err(util.format('DBClient query(%s) err is %s(%s)', sql, err.message, err.code));

                if (!dbclient.__onProcErr(err)) {
                    return ;
                }
            }

            if (dbclient.state != DBSTATE_QUERYLIST) {
                dbclient.__nextCtrl();
            }

            if (funcOnQuery != undefined) {
                funcOnQuery(err, rows, fields);
            }
        });
    }

    query(sql, funcOnQuery) {
        assert.ok(this.state != DBSTATE_NOINIT, 'mysql not init.');

        let dbclient = this;

        if (dbclient.state == DBSTATE_TRANSACTION) {
            dbclient.curCtrl = {type: 'query', param: sql, func: funcOnQuery};
            dbclient.__query(sql, funcOnQuery);
        }
        else if (dbclient.state == DBSTATE_CONNECTED) {
            dbclient.curCtrl = {type: 'query', param: sql, func: funcOnQuery};
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
        if (dbclient.state == DBSTATE_TRANSACTION) {
            let max = sqlarr.length;
            let i = 0;
            let result = [];

            dbclient._queryList(sqlarr, i, max, result, funcOnQueryList);
        }
        else if (dbclient.state == DBSTATE_CONNECTED) {
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

    // beginTransaction
    _beginTransaction(callback) {
        let dbclient = this;

        dbclient.dbcon.beginTransaction(function (err) {
            if (err) {
                dbclient.state = DBSTATE_CONNECTED;
            }
            else {
                dbclient.state = DBSTATE_TRANSACTION;
            }

            callback(err);
        });
    }

    // beginTransaction
    // callback(err)
    beginTransaction(callback) {
        assert.ok(this.state != DBSTATE_NOINIT, 'mysql not init.');

        let dbclient = this;
        if (dbclient.state == DBSTATE_CONNECTED) {
            dbclient.state = DBSTATE_TRANSACTION;

            dbclient._beginTransaction(callback);
        }
        else if (dbclient.state == DBSTATE_INIT) {
            dbclient.__addCtrl('beginTransaction', undefined, callback);

            dbclient.connect();
        }
        else {
            dbclient.__addCtrl('beginTransaction', undefined, callback);
        }
    }

    // rollback
    // callback()
    rollback(callback) {
        let dbclient = this;

        dbclient.__removeCtrl_Rollback();

        dbclient.state = DBSTATE_CONNECTED;
        dbclient.dbcon.rollback(callback);
    }

    _commit(callback) {
        let dbclient = this;

        dbclient.state = DBSTATE_CONNECTED;

        dbclient.dbcon.commit(callback);
    }

    // commit
    // callback(err)
    commit(callback) {
        let dbclient = this;

        //if (dbclient.state == DBSTATE_TRANSACTION) {
        //dbclient.__addCtrl('commit', undefined, callback);
        //}

        dbclient.state = DBSTATE_CONNECTED;
        dbclient.dbcon.commit(callback);
    }


    isValidResult(rows, name) {
        return typeof (rows) != 'undefined' && rows.length > 0 && rows[0].hasOwnProperty(name) && rows[0][name] !== null;
    }

    getInsertID(result) {
        if (result != undefined && result.hasOwnProperty('insertId')) {
            return result.insertId;
        }

        return undefined;
    }

    getAffectedRows(result) {
        if (result != undefined && result.hasOwnProperty('affectedRows')) {
            return result.affectedRows;
        }

        return 0;
    }

    getChangedRows(result) {
        if (result != undefined && result.hasOwnProperty('changedRows')) {
            return result.changedRows;
        }

        return 0;
    }
};

exports.DBClient = DBClient;