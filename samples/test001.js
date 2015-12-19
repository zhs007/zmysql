"use strict";

var zmysql = require('../lib/index');
var util = require('util');

let cfg = zmysql.newConfig('test', '121.41.86.183', 'zhs007', 'zhs007', 'test', 5000, zmysql.defaultLogger);

function queryTest(val, callback) {
    let db = zmysql.getDBClient('test');
    let sql = util.format('insert into test1(val) values(%d)', val);
    db.query(sql, function (err, results, fields) {
        callback();
    });
}

function queryList(begin, end, callback) {
    let db = zmysql.getDBClient('test');
    let arr = [];

    for (let ii = begin; ii < end; ++ii) {
        arr.push(util.format('insert into test1(val) values(%d)', ii));
    }

    db.queryList(arr, function (results) {

    });
}

zmysql.newDBClient(cfg, function () {
    queryTest(1, function () {
        queryTest(2, function () {
            queryTest(3, function () {
                queryTest(4, function () {
                    queryTest(5, function () {
                        queryTest(6, function () {
                            queryTest(7, function () {
                                queryTest(8, function () {
                                    queryTest(9, function () {
                                        queryTest(10, function () {

                                        });
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });
    });

    queryTest(10, function () {
        queryTest(11, function () {
        });
    });

    queryTest(20, function () {
        queryTest(21, function () {
        });
    });

    queryTest(30, function () {
        queryTest(31, function () {
        });
    });

    queryTest(40, function () {
        queryTest(41, function () {
        });
    });

    queryTest(50, function () {
        queryTest(51, function () {
        });
    });

    queryList(100, 200, function () {

    });

    queryTest(60, function () {
        queryTest(61, function () {
        });
    });

    queryTest(70, function () {
        queryTest(71, function () {
        });
    });

    queryTest(80, function () {
        queryTest(81, function () {
        });
    });

    queryTest(90, function () {
        queryTest(91, function () {
        });
    });
});