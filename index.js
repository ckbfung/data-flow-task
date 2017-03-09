'use strict'

const util = require('util')
const fs = require('fs')
const parse = require('csv-parse')
const eventEmitter = require('events').EventEmitter
const dbConnection = require('./dbConnection')
const yaml = require('js-yaml')

function DataFlowTask(dataSource, dataFlow) {
    if ((this instanceof DataFlowTask) == false) {
        return new DataFlowTask(dataSource, dataFlow)
    }

    if (dataFlow != undefined) {
        this.DataSource = dataSource
        this.DataFlow = dataFlow
    } else {
        this.DataSource = null
        let config = yaml.safeLoad(
            fs.readFileSync(dataSource, 'utf8'))
        this.DataFlow = config.DataFlow
    }

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

function getConnection(dataSourceName, dataSource, callback) {
    if (dataSourceName == null) {
        callback("Empty DataSource")
    } else if (dataSource == null) {
        callback(null, dbConnection(dataSourceName.DB, dataSourceName.ConnectionString))
    } else if (dataSourceName in dataSource) {
        var dsConfig = dataSource[dataSourceName]
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

function compareXY(x, y) {
    if (isNaN(x) && isNaN(y) && typeof x === 'number' && typeof y === 'number') {
        return 1
    }

    if (x === y) {
        return 1
    }

    if ((x instanceof Date && y instanceof Date) ||
        (x instanceof String && y instanceof String) ||
        (x instanceof Number && y instanceof Number)) {
        return (x.toString() === y.toString()) ? 1:0
    }
    return 0
}

function runSQL(name, task, config, emitter, callback) {
    emitter.emit('Message', `Start ${task.TaskType} ${name}.`, task)

    if (task.Queries != null && util.isArray(task.Queries)) {
        emitter.emit('Message', task.DbSource, task)
        getConnection(task.DbSource, config.DataSource, function(err, conn) {
            if (err) {
                callback(err)
            } else if (conn == null) {
                callback(`Fail to connect to ${task.DbSource}.`)
            } else {
                var Param = config.Param
                var executeQuery = function(idx) {
                    if (idx >= task.Queries.length) {
                        conn.close(callback)
                    } else {
                        var query = eval("`" + task.Queries[idx] + "`")
                        emitter.emit('Query', `${task.DbSource}: Execute: ${query}`, task)

                        conn.execute(query, function(error, result) {
                            if (task.IgnoreError === false && error) {
                                conn.close(callback, error)
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
            srcConn.close(callback, error)
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
                    emitter.emit('Message', `${taskName} Task: ${idx} rows are copied to ${tableName}.`, task)
                    srcConn.close(function(err) {
                        destConn.close(callback, err)
                    })
                } else {
                    var vals = []
                    for(var column of columns) {
                        var val = selectedRows[idx][column]
                        if (val == null) {
                            vals.push('null')
                        } else if (isNaN(val)) {
                            vals.push(`'${val.replace(/'/g, "''")}'`)
                        } else {
                            vals.push(val)
                        }
                    }
                    var insertVals = vals.join(',')
                    var insert = `insert into ${tableName} (${insertColumns}) values(${insertVals})`
                    emitter.emit('Query', `${task.DbDestination}: Execute: ${insert}`, task)

                    destConn.execute(insert, function(error) {
                        if (error) {
                            srcConn.close(function() {
                                destConn.close(callback, error)
                            })
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
                callback(`Fail to connect to ${task.DbSource}.`)
            } else {
                emitter.emit('Message', task.DbDestination, task)
                getConnection(task.DbDestination, config.DataSource, function(err, destConn) {
                    if (err) {
                        srcConn.close(function() {
                            callback(err)
                        })
                    } else if (destConn == null) {
                        srcConn.close(function() {
                            callback(`Fail to connect to ${task.DbDestination}.`)
                        })
                    } else {
                        var iterateTables = function(idx) {
                            if (idx >= task.TableNames.length) {
                                srcConn.close(function(error) {
                                    destConn.close(callback, error)
                                })
                            } else {
                                var tableName = task.TableNames[idx]
                                if (task.TruncateFirst === true) {
                                    var query = `delete from ${tableName}`
                                    emitter.emit('Query', `${task.DbSource}: Execute: ${query}`, task)

                                    destConn.execute(query, function(error) {
                                        if (error) {
                                            srcConn.close(function() {
                                                destConn.close(callback, error)
                                            })
                                        } else {
                                            copyTable(srcConn, destConn, tableName, name, task, config, emitter, function(error) {
                                                if (error) {
                                                    srcConn.close(function() {
                                                        destConn.close(callback, error)
                                                    })
                                                } else {
                                                    iterateTables(++idx)
                                                }
                                            })
                                        }
                                    })
                                } else {
                                    copyTable(srcConn, destConn, tableName, name, task, config, emitter, function(error) {
                                        if (error) {
                                            srcConn.close(function() {
                                                destConn.close(callback, error)
                                            })
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

function compareQueryResults(name, task, config, emitter, callback) {
    emitter.emit('Message', `Start ${task.TaskType} ${name}.`, task)

    emitter.emit('Message', task.DbSource.Name, task)
    getConnection(task.DbSource.Name, config.DataSource, function(err, srcConn) {
        if (err) {
            callback(err)
        } else if (srcConn == null) {
            callback(`Fail to connect to ${task.DbSource.Name}.`)
        } else {
            emitter.emit('Message', task.DbDestination.Name, task)
            getConnection(task.DbDestination.Name, config.DataSource, function(err, destConn) {
                if (err) {
                    srcConn.close(function() {
                        callback(err)
                    })
                } else if (destConn == null) {
                    srcConn.close(function() {
                        callback(`Fail to connect to ${task.DbDestination.Name}.`)
                    })
                } else {
                    var Param = config.Param

                    var srcQuery = eval("`" + task.DbSource.Query + "`")
                    emitter.emit('Query', `Execute: ${srcQuery}`, task)

                    var destQuery = eval("`" + task.DbDestination.Query + "`")
                    emitter.emit('Query', `Execute: ${destQuery}`, task)

                    srcConn.execute(srcQuery, function(error, srcRows) {
                        if (error) {
                            srcConn.close(function() {
                                destConn.close(callback, error)
                            })
                        } else {
                            destConn.execute(destQuery, function(error, destRows) {
                                if (error) {
                                    srcConn.close(function() {
                                        destConn.close(callback, error)
                                    })
                                } else if (task.Compare.Keys == null || task.Compare.Keys.length == 0) {
                                    srcConn.close(function() {
                                        destConn.close(callback, 'Compare Key is not specified.')
                                    })
                                } else {
                                    var buildUniqueIdx = function(rows) {
                                        var hash = {}
                                        for(var i=0; i<rows.length; ++i) {
                                            var row = rows[i]
                                            var node = hash
                                            for(var x=0; x<task.Compare.Keys.length; ++x) {
                                                var k = row[task.Compare.Keys[x]]
                                                if (!(k in node) || !node.hasOwnProperty(k)) {
                                                    node[k] = {}
                                                }
                                                node = node[k]
                                            }
                                            node['Data'] = row
                                            node['Checked'] = 0
                                        }
                                        return hash
                                    }

                                    function* getNode(node) {
                                        for (var k in node) {
                                            if (node.hasOwnProperty(k)) {
                                                yield { Key: k, Node: node[k] }
                                            }
                                        }
                                    }

                                    function* iterateNode(srcNode, destNode, idx, keyList) {
                                        if (keyList == null) {
                                            keyList = []
                                            for(var i=0; i<=idx; ++i) {
                                                keyList.push('')
                                            }
                                        }
                                        var nodeIter = getNode(srcNode)
                                        var nodeNext = nodeIter.next()
                                        while (nodeNext.done == false) {
                                            var childNode = null
                                            var key = nodeNext.value.Key
                                            keyList[idx] = key
                                            if (destNode != null && (key in destNode)
                                                    && destNode.hasOwnProperty(key)) {
                                                childNode = destNode[key]
                                            }
                                            if (idx == 0) {
                                                var keys = JSON.parse(JSON.stringify(keyList))
                                                keys.reverse()
                                                if (childNode == null) {
                                                    yield { Source: nodeNext.value.Node, Dest: null, Keys: keys }
                                                } else {
                                                    yield { Source: nodeNext.value.Node, Dest: childNode, Keys: keys }
                                                }
                                            } else {
                                                var iter = iterateNode(nodeNext.value.Node, childNode, idx-1, keyList)
                                                var next = iter.next()
                                                while (next.done == false) {
                                                    yield next.value
                                                    next = iter.next()
                                                }
                                            }
                                            nodeNext = nodeIter.next()
                                        }
                                    }

                                    function* unCheckedDestNode(node, idx) {
                                        var nodeIter = getNode(node)
                                        var nodeNext = nodeIter.next()
                                        while (nodeNext.done == false) {
                                            if (idx == 0) {
                                                yield nodeNext.value.Node
                                            } else {
                                                var iter = unCheckedDestNode(nodeNext.value.Node, idx-1)
                                                var next = iter.next()
                                                while (next.done == false) {
                                                    yield next.value
                                                    next = iter.next()
                                                }
                                            }
                                            nodeNext = nodeIter.next()
                                        }
                                    }

                                    var compareRow = function(srcRow, destRow) {
                                        var diff = {}
                                        for (var k in srcRow) {
                                            if (srcRow.hasOwnProperty(k)) {
                                                diff[k] = compareXY(srcRow[k], destRow[k])
                                            }
                                        }
                                        return { A: srcRow, B: destRow, Diff: diff }
                                    }

                                    var compared = {
                                        InSource: [],
                                        InDestination: [],
                                        Diff: []
                                    }

                                    var srcHash = buildUniqueIdx(srcRows)
                                    var destHash = buildUniqueIdx(destRows)

                                    var iter = iterateNode(srcHash, destHash, task.Compare.Keys.length-1)
                                    var n = iter.next()
                                    while (n.done == false) {
                                        var source = n.value.Source
                                        if (source != null) {
                                            source.Checked = 1
                                        }
                                        var dest = n.value.Dest
                                        if (dest != null) {
                                            dest.Checked = 1
                                            compared.Diff.push(compareRow(source.Data, dest.Data))
                                        } else {
                                            compared.InSource.push(source.Data)
                                        }
                                        n = iter.next()
                                    }

                                    iter = unCheckedDestNode(destHash, task.Compare.Keys.length-1)
                                    n = iter.next()
                                    while (n.done == false) {
                                        if (n.value.Checked == 0) {
                                            compared.InDestination.push(n.value.Data)
                                        }
                                        n = iter.next()
                                    }

                                    var compareTransform = function(idx, transform, callback) {
                                        if (idx >= compared.Diff.length) {
                                            callback(null)
                                        } else {
                                            transform(
                                                compared.Diff[idx], config.Context[name],
                                                function(error) {
                                                    if (error) {
                                                        callback(error)
                                                    } else {
                                                        setTimeout(function() {
                                                            compareTransform(++idx, transform, callback)
                                                        }, 0)
                                                    }
                                                })
                                        }
                                    }

                                    var runTransform = function(idx) {
                                        if (idx >= task.Compare.Transforms.length) {
                                            srcConn.close(function(error) {
                                                destConn.close(callback, error)
                                            })
                                        } else {
                                            var transform = task.Compare.Transforms[idx]
                                            compareTransform(0, config.Transforms[transform], function(err) {
                                                if (error) {
                                                    srcConn.close(function() {
                                                        destConn.close(callback, error)
                                                    })
                                                } else{
                                                    runTransform(++idx)
                                                }
                                            })
                                        }
                                    }

                                    config.Context[name]['Compared'] = compared
                                    if (task.Compare.Transforms != null && task.Compare.Transforms.length > 0) {
                                        runTransform(0)
                                    } else {
                                        srcConn.close(function(error) {
                                            destConn.close(callback, error)
                                        })
                                    }
                                }
                            })
                        }
                    })
                }
            })
        }
    })
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
                        destConn.close(callback, error)
                    } else {
                        executeQuery(rowIdx, Row, ++queryIdx)
                    }
                })
            }
        }
    }

    var insertRow = function(rowIdx) {
        if (rowIdx >= rows.length) {
            emitter.emit('Message', `${name} Task: ${rowIdx} rows are Inserted.`, task)
            destConn.close(callback)
        } else {
            var row = rows[rowIdx]
            var runTransform = function(idx) {
                if (idx >= task.Transforms.length) {
                    executeQuery(rowIdx, row, 0)
                } else {
                    var transform = task.Transforms[idx]
                    config.Transforms[transform](
                        row, config.Context[name],
                        function(error) {
                            if (error) {
                                destConn.close(callback, error)
                            } else {
                                runTransform(++idx)
                            }
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
                callback(`Fail to connect to ${task.DbSource.Name}.`)
            } else {
                emitter.emit('Message', task.DbDestination.Name, task)
                getConnection(task.DbDestination.Name, config.DataSource, function(err, destConn) {
                    if (err) {
                        srcConn.close(function() {
                            callback(err)
                        })
                    } else if (destConn == null) {
                        srcConn.close(function() {
                            callback(`Fail to connect to ${task.DbDestination}.`)
                        })
                    } else {
                        var Param = config.Param
                        var query = eval('`' + task.DbSource.Query + '`')
                        emitter.emit('Query', `${task.DbSource.Name}: Execute: ${query}`, task)

                        srcConn.execute(query, function(error, selectedRows) {
                            if (error) {
                                srcConn.close(callback, error)
                            } else if (selectedRows.length <= 0) {
                                callback(null)
                            } else {
                                insertData(destConn, selectedRows, name, task, config, emitter,
                                    function(error) {
                                        srcConn.close(function(closeError) {
                                            if (error) {
                                                destConn.close(callback, error)
                                            } else {
                                                destConn.close(callback, closeError)
                                            }
                                        })
                                    })
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
                callback(`Fail to connect to ${task.DbDestination}.`)
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
                        insertData(destConn, csvData, name, task, config, emitter,
                            function(error) {
                                destConn.close(callback, error)
                            })
                    })
            }
        })
    } else {
        callback(null)
    }
}

function executeTask(name, task, config, emitter, callback) {
    config.Context[name] = {
        Task: task
    }
    if (task.TaskType == null) {
        callback(null)
    } else if (task.TaskType === 'Run SQL') {
        runSQL(name, task, config, emitter, callback)
    } else if (task.TaskType === 'Compare Query Results') {
        compareQueryResults(name, task, config, emitter, callback) 
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

DataFlowTask.prototype.Start = function start(param, context, transforms, callback) {
    callback = getCallback(callback)

    var self = this
    var config = {}
    if (self.DataSource == null) {
        config.DataSource = null
    } else {
        config.DataSource = JSON.parse(JSON.stringify(self.DataSource))
    }
    config.DataFlow = JSON.parse(JSON.stringify(self.DataFlow))
    config.Param = JSON.parse(JSON.stringify(param))
    config.Context = context
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