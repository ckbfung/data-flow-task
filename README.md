# Data flow task

## Installation

```
$ npm install data-flow-task
```

## Data Sources

Define a name in KEY for Data-Flow-Task to reference.

DB can be 'MSSQL' or 'Oracle'.

ConnectionString is connection string to connect to DB.

eg,
```js
    'MS SQL Source': {
        DB: 'MSSQL',
        ConnectionString: "Data Source=Hostname\\DbInstance;Initial Catalog=DbName;Integrated Security=True"
    }
```

## Tasks

There are four Tasks supported.

* Run SQL
* Copy DB Table
* Compare Query Results
* Insert DB data
* Insert CSV data

### Run SQL

Define a TaskType: 'Run SQL'.

DataSource is defined in 'Data Sources' configuration.

Queries is to define queries to be executed. Queries are sequential execution in order.

eg,
```js
    TaskType: 'Run SQL',
    DbSource: 'MS SQL Source',
    Queries: [
        'DELETE FROM Table',
        "INSERT INTO Table(ColumnA, ColumnB) values(number, 'VarChar')"
    ],
    IgnoreError: false
```

### Copy DB Table

Define a TaskType: 'Copy DB Table'.

DbSource and DbDestination are defined in 'Data Sources' configuration.

TableName is the table to copy from DbSource DB to DbDestination DB.

eg,
```js
    TaskType: 'Copy DB Table',
    DbSource: 'MS SQL Source',
    DbDestination: 'MS SQL Dest',
    TableNames: [ 'Table' ],
    TruncateFirst: true
```

### Compare Query Results

Define a TaskType: 'Compare Query Results'.

DbSource and DbDestination are defined in 'Data Sources' and query configuration.

Compare:Key is to define the Key columns for comparsion.

Compare:Transform is to define the function to tranfrom the compare results.

eg,
```js
    TaskType: 'Compare Query Results',
    DbSource: {
        Name: 'MS SQL Source',
        Query: 'Select COL1, COL2 from Table1 where id = ${Param.ID}'
    },
    DbDestination: {
        Name: 'MS SQL Dest',
        Query: 'Select COL1, COL2 from Table2 where id = ${Param.ID}'
    },
    Compare: {
        Keys: [ 'COL1', 'COL2' ],
        Transforms: ['Transform1', 'Transform2', 'Transform3']
    }
```

### Insert DB data

Define a TaskType: 'Insert DB data'.

DbSource and DbDestination Names are defined in 'Data Sources' configuration.

DbSouce Query is the source query to retrive data from DbSource.

Transforms are function names to inject to Data-Flow-Task. After data is obtained from DbSource Query, Data-Flow-Task executs the transform functions in sequential.

DbDestination Queries is to define queries to be executed. Queries are sequential execution in order.

eg,
```js
    TaskType: 'Insert DB data',
    DbSource: {
        Name: 'MS SQL Source',
        Query: 'Select COL1, COL2 from Table where id = ${Param.ID}'
    },
    Transforms: ['Transform1', 'Transform2', 'Transform3'],
    DbDestination: {
        Name: 'MS SQL Dest',
        Queries: [
            "Insert Into DestTable (DestCol1, DestCol2) Values(${Row.COL1}, '${Row.COL2}')"
        ]
    }
```

### Insert CSV data

Define a TaskType: 'Insert CSV data'.

CsvSource specifies File name, Skip Headers, and Delimiter.

Transforms are function names to inject to Data-Flow-Task. After data is obtained from CSV file, Data-Flow-Task executs the transform functions in sequential.

DbDestination Names is defined in 'Data Sources' configuration.

DbDestination Queries is to define queries to be executed. Queries are sequential execution in order.

eg,
```js
    TaskType: 'Insert CSV data',
    CsvSource: {
        File: './test.csv',
        SkipHeader: true,
        Delimiter: ','
    },
    Transforms: ['TransformCSV'],
    DbDestination: {
        Name: 'MS SQL Dest',
        Queries: [
            "Insert Into DestTableA (DestCol1, DestCol2) Values(${Row.COL1}, '${Row.COL2}')",
            "Update DestTableB Set DestCol1=${Row.COL1}, DestCol2='${Row.COL2}' Where ID = ${Param.ID}"
        ]
    }
```

## Usage

```js
var DataFlowTask = require('data-flow-task')

// Define Data Source
var dataSources = {
    'MS SQL Source': {
        DB: 'MSSQL',
        ConnectionString: "Data Source=Hostname\\DbInstance;Initial Catalog=DbName;Integrated Security=True"
    },
    'MS SQL Dest': {
        DB: 'MSSQL',
        ConnectionString: "Data Source=Hostname\\DbInstance;Initial Catalog=DbName;Integrated Security=True"
    }
}

var dataFlow = {
    Cleanup: {
		TaskType: 'Run SQL',
		DbSource: 'MS SQL Source',
		Queries: [
            'DELETE FROM Table',
            'DELETE FROM ${Param.TableName}'
        ],
        IgnoreError: false
	},
	CopyTable: {
		TaskType: 'Copy DB Table',
		DbSource: 'MS SQL Source',
		DbDestination: 'MS SQL Dest',
        TableNames: ['${Param.TableName}'],
        TruncateFirst: true
	},
    // InsertData is an Array. Each task in Array are executed asynchronously.
	InsertData: [
		{
			TaskType: 'Insert DB data',
			DbSource: {
				Name: 'MS SQL Source',
				Query: 'Select COL1, COL2 from Table Where ID = ${Param.ID}'
			},
			Transforms: ['Transform1', 'Transform2', 'Transform3'],
			DbDestination: {
				Name: 'MS SQL Dest',
				Queries: [
                    "Insert Into DestTableA (DestCol1, DestCol2) Values(${Row.COL1}, '${Row.COL2}')",
                    "Update DestTableB Set DestCol1=${Row.COL1}, DestCol2='${Row.COL2}' Where ID = ${Param.ID}"
                ]
			}
		},
		{
			TaskType: 'Insert CSV data',
			CsvSource: {
				File: './CSVFile.csv',
                SkipHeader: true,
				Delimiter: ','
			},
			Transforms: ['TransformCSV'],
			DbDestination: {
				Name: 'MS SQL Dest',
				Queries: [
                    "Insert Into DestTableA (DestCol1, DestCol2) Values(${Row.COL1}, '${Row.COL2}')",
                    "Update DestTableB Set DestCol1=${Row.COL1}, DestCol2='${Row.COL2}' Where ID = ${Param.ID}"
                ]
            }
		}
	],
    CompareQueries: {
        TaskType: 'Compare Query Results',
        DbSource: {
            Name: 'MS SQL Source',
            Query: 'SELECT COL1, COL2, COL3 FROM SourceTable'
        },
        DbDestination: {
            Name: 'MS SQL Dest',
            Query: 'SELECT COL1, COL2, COL3 FROM DestTable'
        },
        Compare: {
            Keys: [ 'COL1', 'COL2' ],
            Transforms: [ 'TransformCompare' ]
        }
    }
}

function Transform1(row, context, callback) {
    console.log('Call Transform1')
    callback()
}

function Transform2(row, context, callback) {
    console.log('Call Transform2')
    callback()
}

function Transform3(row, context, callback) {
    console.log('Call Transform3')
    callback()
}

function TransformCSV(row, context, callback) {
    console.log('Call TransformCSV')
    callback()
}

function TransformCompare(row, context, callback) {
    console.log('Call TransformCompare')
    callback()
}

var dataFlowTask = DataFlowTask(dataSources, dataFlow)
dataFlowTask.on('Message', function(msg) {
    console.log('Message:', msg)
})

dataFlowTask.on('Query', function(msg) {
    console.log('Query:', msg)
})

dataFlowTask.Start(
    { ID: 10, TableName:'TestTable' },
    {},
    [Transform1, Transform2, Transform3, TransformCSV],
    function(err) {
        if (err) {
            console.log('Error:', err)
        }
})
```

```yaml
SourceDataSources: &srcDB
  DB: MSSQL
  ConnectionString: Data Source=Hostname\\DbInstance;Initial Catalog=DbName;Integrated Security=True

DestDataSources: &destDB
  DB: MSSQL
  ConnectionString: Data Source=Hostname\\DbInstance;Initial Catalog=DbName;Integrated Security=True

DataFlow:
  Cleanup:
    TaskType: Run SQL
    DbSource: *srcDB
    Queryies:
      - DELETE FROM Table
      - DELETE FROM ${Param.TableName}
    IgnoreError: true

  CopyTable:
    TaskType: Copy DB Table
    DbSource: *srcDB
    DbDestination: *destDB
    TableNames:
      - ${Param.TableName}
    TruncateFirst: false

	InsertData:
      - TaskType: Insert DB data
        DbSource:
          Name: *srcDB
          Query: Select COL1, COL2 from Table Where ID = ${Param.ID}
        Transforms:
          - Transform1
          - Transform2
          - Transform3
        DbDestination:
          Name: *destDB
		  Queries:
            - |
                Insert Into DestTableA (DestCol1, DestCol2)
                Values(${Row.COL1}, '${Row.COL2}')
            - |
                Update DestTableB Set DestCol1=${Row.COL1}, DestCol2='${Row.COL2}'
                Where ID = ${Param.ID}
```
