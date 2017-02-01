'use strict'

var oracledb = null
var mssql = null

function dbConnection(db, connectionString) {
    if (db === 'Oracle') {
        return oracleConnection(connectionString)
    }
    if (db === 'MSSQL') {
        return msSqlConnection(connectionString)
    }
    return null
}

function oracleConnection(connectionString) {
    if (oracledb == null) {
        oracledb = require('oracledb')
        oracledb.fetchAsString = [ oracledb.CLOB, oracledb.DATE ]
    }
    return {
        connectString: connectionString,
        execute: function(query, callback) {
            var executed = function(err, result) {
                var objects = []
                if (err == null) {
                    if (result != null && result.rows != null && result.rows.length > 0) {
                        for(var row of result.rows) {
                            var obj = {}
                            for(var i=0; i<result.metaData.length; ++i) {
                                obj[result.metaData[i].name] = row[i]
                            }
                            objects.push(obj)
                        }
                    }
                }
                return objects
            }
            var connected = function(err, connection) {
                if (err) {
                    callback(err)
                } else {
                    connection.execute(
                        query, [], { maxRows: 10000 },
                        function(err, result) {
                            var objects = executed(err, result)
                            connection.release(function() {
                                callback(err, objects)
                            })
                        })
                }
            }
            oracledb.getConnection(
                {
                    externalAuth: true,
                    connectString: this.connectString
                },
                connected)
        }
    }
}

function msSqlConnection(connectionString) {
    if (mssql == null) {
        mssql = require('edge')
    }
    return {
        connectString: connectionString,
        execute: function(query, callback) {
            var edgeQuery = mssql.func('sql',
                {
                    connectionString: this.connectString,
                    source: query
                })
            edgeQuery(null, callback)
        }
    }
}

module.exports = dbConnection