'use strict'

var util = require('util')
var fs = require('fs')
var parse = require('csv-parse')
var eventEmitter = require('events').EventEmitter
var dbConnection = require('./dbConnection')

function DataFlowTask(dataSource, dataFlow) {
    if ((this instanceof DataFlowTask) == false) {
        return new DataFlowTask(dataSource, dataFlow)
    }

    this.DataSource = dataSource
    this.DataFlow = dataFlow

    eventEmitter.call(this)
}

util.inherits(DataFlowTask, eventEmitter)

var getCallback = function(callback) {
    if (callback != null) {
        return callback
    }
    return function(err) {
        if (err) {
            throw err
        }
    }
}

function getConnection(dataSouceName, dataSource, callback) {
    if (dataSouceName == null) {
        callback("Empty DataSource")
    } else if (dataSouceName in dataSource) {
        var dsConfig = dataSource[dataSouceName]
        if (dsConfig.DB == null) {
            callback(`${dataSourceName}: Miss DB.`)
        } else if (dsConfig.ConnectionString == null) {
            callback(`${dataSourceName}: Miss ConnectionString.`)
        } else {
            callback(null, dbConnection(dsConfig.DB, dsConfig.ConnectionString))
        }
    } else {
        callback("Invalid DataSource")
    }
}

function runSQL(name, task, config, emitter, callback) {
    emitter.emit('Message', `Start ${task.TaskType} ${name}.`, task)

    if (task.Queries != null && util.isArray(task.Queries)) {
        emitter.emit('Message', task.DbSource, task)
        getConnection(task.DbSource, config.DataSource, function(err, conn) {
            if (err) {
                callback(err)
            } else if (conn == null) {
                callback(`Fail to connection to ${task.DbSource}.`)
            } else {
                var Param = config.Param
                var executeQuery = function(idx) {
                    if (idx >= task.Queries.length) {
                        callback(null)
                    } else {
                        var query = eval("`" + task.Queries[idx] + "`")
                        emitter.emit('Query', `${task.DbSource}: Execute: ${query}`, task)

                        conn.execute(query, function(error, result) {
                            if (task.IgnoreError === false && error) {
                                callback(error)
                            } else {
                                executeQuery(++idx)
                            }
                        })
                    }
                }
                executeQuery(0)
            }
        })
    } else {
        callback(null)
    }
}

function copyTable(srcConn, destConn, tableName, taskName, task, config, emitter, callback) {
    var Param = config.Param

    var query = `Select * from ${tableName}`
    emitter.emit('Query', `${task.DbSource}: Execute: ${query}`, task)

    srcConn.execute(query, function(error, selectedRows) {
        if (error) {
            callback(error)
        } else if (selectedRows.length <= 0) {
            callback(null)
        } else {
            var columns = []
            for (var column in selectedRows[0]) {
                if (selectedRows[0].hasOwnProperty(column)) {
                    columns.push(column);
                }
            }

            var insertColumns = columns.join(',')
            var insertedRows = 0
            var insertRow = function(idx) {
                if (idx >= selectedRows.length) {
                    emitter.emit('Message', `${taskName} Task: ${idx} rows are copied.`, task)
                    callback(null)
                } else {
                    var vals = []
                    for(var column of columns) {
                        var val = selectedRows[idx][column]
                        if (val == null) {
                            vals.push('null')
                        } else if (isNaN(val)) {
                            vals.push(`'${val}'`)
                        } else {
                            vals.push(val)
                        }
                    }
                    var insertVals = vals.join(',')
                    var insert = `insert into ${tableName} (${insertColumns}) values(${insertVals})`
                    emitter.emit('Query', `${task.DbSource}: Execute: ${insert}`, task)

                    destConn.execute(insert, function(error) {
                        if (error) {
                            callback(error)
                        } else {
                            insertRow(++idx)
                        }
                    })
                }
            }
            insertRow(0)
        }
    })
}

function copyDbTable(name, task, config, emitter, callback) {
    emitter.emit('Message', `Start ${task.TaskType} ${name}.`, task)

    if (task.TableNames != null && util.isArray(task.TableNames)) {
        emitter.emit('Message', task.DbSource, task)

        getConnection(task.DbSource, config.DataSource, function(err, srcConn) {
            if (err) {
                callback(err)
            } else if (srcConn == null) {
                callback(`Fail to connection to ${task.DbSource}.`)
            } else {
                emitter.emit('Message', task.DbDestination, task)
                getConnection(task.DbDestination, config.DataSource, function(err, destConn) {
                    if (err) {
                        callback(err)
                    } else if (destConn == null) {
                        callback(`Fail to connection to ${task.DbDestination}.`)
                    } else {
                        var iterateTables = function(idx) {
                            if (idx >= task.TableNames.length) {
                                callback(null)
                            } else {
                                var tableName = task.TableNames[idx]
                                if (task.TruncateFirst === true) {
                                    var query = `delete from ${tableName}`
                                    emitter.emit('Query', `${task.DbSource}: Execute: ${query}`, task)

                                    destConn.execute(query, function(error) {
                                        if (error) {
                                            callback(error)
                                        } else {
                                            copyTable(srcConn, destConn, tableName, name, task, config, emitter, function(err) {
                                                if (err) {
                                                    callback(err)
                                                } else {
                                                    iterateTables(++idx)
                                                }
                                            })
                                        }
                                    })
                                } else {
                                    copyTable(srcConn, destConn, tableName, name, task, config, emitter, function(err) {
                                        if (err) {
                                            callback(err)
                                        } else {
                                            iterateTables(++idx)
                                        }
                                    })
                                }
                            }
                        }
                        iterateTables(0)
                    }
                })
            }
        })
    } else {
        callback(null)
    }
}

function insertData(destConn, rows, name, task, config, emitter, callback) {
    var Param = config.Param

    var executeQuery = function(rowIdx, Row, queryIdx) {
        if (queryIdx >= task.DbDestination.Queries.length) {
            insertRow(++rowIdx)
        } else {
            if (Object.keys(Row).length <= 0) {
                executeQuery(rowIdx, Row, ++queryIdx)
            } else {
                var insert = eval("`" + task.DbDestination.Queries[queryIdx] + "`")
                emitter.emit('Query', `${task.DbDestination.Name}: Execute: ${insert}`, task)

                destConn.execute(insert, function(error) {
                    if (error) {
                        callback(error)
                    } else {
                        executeQuery(rowIdx, Row, ++queryIdx)
                    }
                })
            }
        }
    }

    var context = {}
    context['Name'] = name
    context['Task'] = task

    var insertRow = function(rowIdx) {
        if (rowIdx >= rows.length) {
            emitter.emit('Message', `${name} Task: ${rowIdx} rows are Inserted.`, task)
            callback(null)
        } else {
            var row = rows[rowIdx]
            var runTransform = function(idx) {
                if (idx >= task.Transforms.length) {
                    executeQuery(rowIdx, row, 0)
                } else {
                    var transform = task.Transforms[idx]
                    config.Transforms[transform](
                        row, context,
                        function(err) {
                            if (err) {
                                callback(err)
                                return
                            }
                            runTransform(++idx)
                        })
                }
            }
            runTransform(0)
        }
    }

    insertRow(0)
}

function insertDbData(name, task, config, emitter, callback) {
    emitter.emit('Message', `Start ${task.TaskType} ${name}.`, task)

    if (task.DbSource.Query != null &&
        task.DbDestination.Queries != null && util.isArray(task.DbDestination.Queries)) {
        emitter.emit('Message', task.DbSource.Name, task)

        getConnection(task.DbSource.Name, config.DataSource, function(err, srcConn) {
            if (err) {
                callback(err)
            } else if (srcConn == null) {
                callback(`Fail to connection to ${task.DbSource.Name}.`)
            } else {
                emitter.emit('Message', task.DbDestination.Name, task)
                getConnection(task.DbDestination.Name, config.DataSource, function(err, destConn) {
                    if (err) {
                        callback(err)
                    } else if (destConn == null) {
                        callback(`Fail to connection to ${task.DbDestination}.`)
                    } else {
                        var Param = config.Param
                        var query = eval('`' + task.DbSource.Query + '`')
                        emitter.emit('Query', `${task.DbSource.Name}: Execute: ${query}`, task)

                        srcConn.execute(query, function(error, selectedRows) {
                            if (error) {
                                callback(error)
                            } else if (selectedRows.length <= 0) {
                                callback(null)
                            } else {
                                insertData(destConn, selectedRows, name, task, config, emitter, callback)
                            }
                        })
                    }
                })
            }
        })
    } else {
        callback(null)
    }
}

function insertDbDataFromCsv(name, task, config, emitter, callback) {
    emitter.emit('Message', `Start ${task.TaskType} ${name}.`, task)

    if ( task.CsvSource != null && task.CsvSource.File != null &&
         task.CsvSource.SkipHeader != null && task.CsvSource.Delimiter != null ) {
        emitter.emit('Message', task.DbDestination.Name, task)

        var skipLine = task.CsvSource.SkipHeader
        getConnection(task.DbDestination.Name, config.DataSource, function(err, destConn) {
            if (err) {
                callback(err)
            } else if (destConn == null) {
                callback(`Fail to connection to ${task.DbDestination}.`)
            } else {
                emitter.emit('Message', task.CsvSource.File, task)
                var csvData = []
                fs.createReadStream(task.CsvSource.File)
                    .pipe(parse({delimiter: task.CsvSource.Delimiter}))
                    .on('data', function(csvRow) {
                        if (skipLine === true) {
                            skipLine = false
                        } else {
                            csvData.push(csvRow)
                        }
                    })
                    .on('end',function() {
                        insertData(destConn, csvData, name, task, config, emitter, callback)
                    })
            }
        })
    } else {
        callback(null)
    }
}

function executeTask(name, task, config, emitter, callback) {
    if (task.TaskType == null) {
        callback(null)
    } else if (task.TaskType === 'Run SQL') {
        runSQL(name, task, config, emitter, callback)
    } else if (task.TaskType === 'Copy DB Table') {
        copyDbTable(name, task, config, emitter, callback) 
    } else if (task.TaskType === 'Insert DB data') {
        insertDbData(name, task, config, emitter, callback)
    } else if (task.TaskType === 'Insert CSV data') {
        insertDbDataFromCsv(name, task, config, emitter, callback)
    } else {
        callback(null)
    }
}

DataFlowTask.prototype.Start = function start(param, transforms, callback) {
    callback = getCallback(callback)

    var self = this
    var config = {}
    config.DataSource = JSON.parse(JSON.stringify(self.DataSource))
    config.DataFlow = JSON.parse(JSON.stringify(self.DataFlow))
    config.Param = JSON.parse(JSON.stringify(param))

    config.Transforms = {}
    for(var transform of transforms) {
        config.Transforms[transform.name] = transform
    }

    var keys = []
    for (var key in config.DataFlow) {
        if (config.DataFlow.hasOwnProperty(key)) {
            keys.push(key)
        }
    }

    var iterateTasks = function(idx) {
        if (idx >= keys.length) {
            callback(null)
        } else {
            var name = keys[idx]
            var task = config.DataFlow[name]
            if (util.isArray(task) === true) {
                var completed = 0
                for(var i=0; i<task.length; ++i) {
                    executeTask(
                        name + `:${i}`, task[i], config, self,
                        function(err) {
                            if (err) {
                                callback(err)
                                return
                            }
                            if (completed >= task.length) {
                                iterateTasks(++idx)
                            } else {
                                ++completed
                            }
                        })
                }
            } else {
                executeTask(
                    name, task, config, self,
                    function(err) {
                        if (err) {
                            callback(err)
                            return
                        }
                        iterateTasks(++idx)
                    })
            }
        }
    }
    iterateTasks(0)
}

module.exports = DataFlowTask