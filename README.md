# rockscolumnfamilies
The idea of this app is to showcase how you can use column families in RocksDB to store data with different schemas.

## Api Methods

Method|Path|Description
---|---|---
POST|/api/rocksdb/createtable|Method to create table.
POST|/api/rocksdb/put|Method to insert data in a given table. 
GET|api/rocksdb/get|Method to get information from give table
DELETE|api/rocksdb/delete|Method to delete keys from given table


## Sample Valid JSON Request Bodys
### Create table

{
    "name": "USERS",
    "columns": [
        {
            "name": "USERNAME",
            "key": true,
            "type": "VARCHAR"
        },
        {
           "name": "EMAIL",
            "key": true,
            "type": "INT"  
        },
          {
            "name": "JOB",
            "type": "VARCHAR"
        },
        {
            "name": "AGE",
            "type": "INT"
        }
    ]
}

### Put

{
    "name": "USERS",
    "columns": [
        {
            "name": "USERNAME",
            "value": "lrpt"
        },
        {
            "name": "EMAIL",
            "value": "LRPT@GIT.com"
        },
        {
            "name": "JOB",
            "value": "Software Engineer"
        },
        {
            "name": "AGE",
            "value": 29
        }
    ]
}

### Get

{
    "name": "USERS",
    "columns": [
        {
            "name": "USERNAME",
            "value": "lrpt"
        },
        {
            "name": "EMAIL",
            "value": "LRPT@GIT.com"
        }
    ]
}
### Delete

{
    "name": "USERS",
    "columns": [
        {
            "name": "USERNAME",
            "value": "lrpt"
        },
        {
            "name": "EMAIL",
            "value": "LRPT@GIT.com"
        }
    ]
}
