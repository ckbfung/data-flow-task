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
        oracleConnection: null,
        connectString: connectionString,
        close: function(callback, err) {
            var self = this
            if (err === undefined) {
                err = null
            }
            if (this.oracleConnection != null) {
                this.oracleConnection.commit(function() {
                    self.oracleConnection.release(function(closeError) {
                        self.oracleConnection = null
                        if (err) {
                            callback(err)
                        } else {
                            callback(closeError)
                        } 
                    })
                })
            } else {
                callback(err)
            }
        },
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
            var connected = function(connection) {
                connection.execute(
                    query, [], { autoCommit: false, maxRows: 1000000 },
                    function(err, result) {
                        var objects = executed(err, result)
                        callback(err, objects)
                    })
            }
            if (this.oracleConnection != null) {
                connected(this.oracleConnection)
            } else {
                var self = this
                oracledb.getConnection(
                    {
                        externalAuth: true,
                        connectString: this.connectString
                    },
                    function(err, conn) {
                        if (err) {
                            callback(err)
                        } else {
                            self.oracleConnection = conn
                            connected(conn)
                        }
                    })
            }
        }
    }
}

function msSqlConnection(connectionString) {
    if (mssql == null) {
        mssql = require('edge')
    }
    return {
        connectString: connectionString,
        close: function(callback, err) {
            if (err === undefined) {
                callback(null)
            } else {
                callback(err)
            }
        },
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