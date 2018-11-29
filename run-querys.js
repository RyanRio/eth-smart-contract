let fs = require('fs')
// Imports the Google Cloud client library
const { BigQuery } = require('@google-cloud/bigquery');

// Your Google Cloud Platform project ID
const projectId = 'iconic-medium-222620';

// Creates a client
const bigquery = new BigQuery({
    projectId: projectId,
    keyFilename: "creds.json"
});

// dates are first half of year represented by f
// second half of year represented by s
// year represented by last two digits
// first half of 2015 variable name = ffifteen
// 2015
const ffifteen =
    'SELECT is_erc20,is_erc721,block_number,block_timestamp' +
    ' FROM `bigquery-public-data.ethereum_blockchain.contracts`' +
    ' WHERE block_timestamp' +
    ` BETWEEN '2015-01-01' AND '2015-03-15'`
const ffifteent =
    'SELECT is_erc20,is_erc721,block_number,block_timestamp' +
    ' FROM `bigquery-public-data.ethereum_blockchain.contracts`' +
    ' WHERE block_timestamp' +
    ` BETWEEN '2015-03-16' AND '2015-06-15'`

const sfifteen =
    'SELECT is_erc20,is_erc721,block_number,block_timestamp' +
    ' FROM `bigquery-public-data.ethereum_blockchain.contracts`' +
    ' WHERE block_timestamp' +
    ` BETWEEN '2015-06-16' AND '2015-09-15'`

const sfifteent =
    'SELECT is_erc20,is_erc721,block_number,block_timestamp' +
    ' FROM `bigquery-public-data.ethereum_blockchain.contracts`' +
    ' WHERE block_timestamp' +
    ` BETWEEN '2015-09-16' AND '2016-01-01'`

// 2016
const fsixteen =
    'SELECT is_erc20,is_erc721,block_number,block_timestamp' +
    ' FROM `bigquery-public-data.ethereum_blockchain.contracts`' +
    ' WHERE block_timestamp' +
    ` BETWEEN '2016-01-02' AND '2016-03-01'`
const fsixteent =
    'SELECT is_erc20,is_erc721,block_number,block_timestamp' +
    ' FROM `bigquery-public-data.ethereum_blockchain.contracts`' +
    ' WHERE block_timestamp' +
    ` BETWEEN '2016-03-02' AND '2016-06-01'`
const ssixteen =
    'SELECT is_erc20,is_erc721,block_number,block_timestamp' +
    ' FROM `bigquery-public-data.ethereum_blockchain.contracts`' +
    ' WHERE block_timestamp' +
    ` BETWEEN '2016-06-02' AND '2016-09-01'`
const ssixteent =
    'SELECT is_erc20,is_erc721,block_number,block_timestamp' +
    ' FROM `bigquery-public-data.ethereum_blockchain.contracts`' +
    ' WHERE block_timestamp' +
    ` BETWEEN '2016-09-02' AND '2017-01-01'`

// 2017
const fseventeen =
    'SELECT is_erc20,is_erc721,block_number,block_timestamp' +
    ' FROM `bigquery-public-data.ethereum_blockchain.contracts`' +
    ' WHERE block_timestamp' +
    ` BETWEEN '2017-01-02' AND '2017-03-01'`

const fseventeent =
    'SELECT is_erc20,is_erc721,block_number,block_timestamp' +
    ' FROM `bigquery-public-data.ethereum_blockchain.contracts`' +
    ' WHERE block_timestamp' +
    ` BETWEEN '2017-03-02' AND '2017-06-01'`

const sseventeen =
    'SELECT is_erc20,is_erc721,block_number,block_timestamp' +
    ' FROM `bigquery-public-data.ethereum_blockchain.contracts`' +
    ' WHERE block_timestamp' +
    ` BETWEEN '2017-06-02' AND '2017-09-01'`

const sseventeent =
    'SELECT is_erc20,is_erc721,block_number,block_timestamp' +
    ' FROM `bigquery-public-data.ethereum_blockchain.contracts`' +
    ' WHERE block_timestamp' +
    ` BETWEEN '2017-09-02' AND '2018-01-01'`

// 2018
const feighteen =
    'SELECT is_erc20,is_erc721,block_number,block_timestamp' +
    ' FROM `bigquery-public-data.ethereum_blockchain.contracts`' +
    ' WHERE block_timestamp' +
    ` BETWEEN '2018-01-02' AND '2018-03-01'`
const feighteent =
    'SELECT is_erc20,is_erc721,block_number,block_timestamp' +
    ' FROM `bigquery-public-data.ethereum_blockchain.contracts`' +
    ' WHERE block_timestamp' +
    ` BETWEEN '2018-03-02' AND '2018-06-01'`
const seighteen =
    'SELECT is_erc20,is_erc721,block_number,block_timestamp' +
    ' FROM `bigquery-public-data.ethereum_blockchain.contracts`' +
    ' WHERE block_timestamp' +
    ` BETWEEN '2018-06-02' AND '2018-09-01'`
const seighteent =
    'SELECT is_erc20,is_erc721,block_number,block_timestamp' +
    ' FROM `bigquery-public-data.ethereum_blockchain.contracts`' +
    ' WHERE block_timestamp' +
    ` BETWEEN '2018-09-02' AND '2019-01-01'`
const querys =
    [
        // 2015
        ffifteen,
        ffifteent,
        sfifteen,
        sfifteent,
        // 2016
        fsixteen,
        fsixteent,
        ssixteen,
        ssixteent,
        // 2017
        fseventeen,
        fseventeent,
        sseventeen,
        sseventeent,
        // 2018
        feighteen,
        feighteent,
        seighteen,
        seighteent
    ]

function runQuery(i) {
    let query = querys[i]
    console.log("running query: " + query)
    bigquery.query(query, function (err, rows) {
        if (!err) {
            evaluateQuery(rows)
            if(i+1<querys.length) {
                runQuery(i+1)
            }
        }
        else console.log(err)
    })
}

runQuery(0)

function evaluateQuery(data) {

    sorteddata = []
    let is_erc20 = 0
    let is_erc721 = 0

    // create list of lists of info, each on an individual timestamp
    function filterByDay(list) {
        // the day to create a list of similar days of
        let day = list[0].block_timestamp.value.substring(0, 10)
        let restfilterdata = []
        let filtereddata = list.filter((val) => {
            if (val.block_timestamp.value.substring(0, 10) == day) {
                return true
            } else {
                restfilterdata.push(val)
                return false
            }
        })

        //let restfilter = list.filter((val) => {
        //  return val.block_timestamp.value.substring(0, 10) !== day
        //})

        sorteddata.push(filtereddata)
        if (restfilterdata.length == 0) {
            return []
        }
        else {
            sorteddata.push(filterByDay(restfilterdata))
        }
    }

    // if there is any data to filter, filter
    if (data.length > 0) {
        filterByDay(data)
    }

    // remove undefined values and count erc20 and erc721
    // also organize the data in a more readable manner, with number of contracts that fall
    // on that day after
    const results = sorteddata.map(val => {
        if (val && val[0]) {
            val.forEach(contract=> {
                if(contract.is_erc20) {
                    is_erc20+=1
                }
                if(contract.is_erc721) {
                    is_erc721+=1
                }
            })
            return val[0].block_timestamp.value.substring(0, 10) + ": " + val.length
        }
        else 0
    }).filter(val => {
        return typeof val !== 'undefined'
    })

    console.log("filtered results")
    let numContracts = 0
    // count number of contracts
    sorteddata.forEach(val => {
        if (val && val[0]) {
            numContracts += val.length
        }
    })

    // write to file
    fs.appendFileSync("filteredresults.json",
        JSON.stringify(
            {
                results: results,
                numDates: results.length,
                numContracts: numContracts,
                is_erc20Average: is_erc20/numContracts,
                is_erc721Average: is_erc721/numContracts
            }
        )
    )
    return
}
