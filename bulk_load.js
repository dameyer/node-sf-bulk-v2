'use strict'
/**
 * dmeyer
 * 20Jan2018
 */

let
    config = require('./bulk_config.js'),
    fn = config.FILENAME,
    fileType = config.FILETYPE,
    apiVersion = config.APIVERSION,
    loginUrl = config.LOGINURL + apiVersion,
    sObject = config.SOBJECT,
    op = config.OPERATION,
    baseUrl = '/services/data/v' + apiVersion + '/jobs/ingest/',
    jobId = '',
    sid = '',
    fs = require('fs'),
    request = require("request");

/*
bulk_config.js contains

module.exports = {
    USERNAME: 'your_username',
    PASSWORD: 'your_password',
    FILENAME: 'contacts.csv',
    FILETYPE: 'CSV',
    SOBJECT: 'contact',
    OPERATION: 'insert',
    APIVERSION: '41.0',
    LOGINURL: 'https://login.salesforce.com/services/Soap/u/'
}
*/


const SUCCESSFUL_RESULTS = 'successfulResults',
    FAILED_RESULTS = 'failedResults',
    JOB_COMPLETE = 'JobComplete',
    RESULTS_DIRECTORY = 'results/'

let options = {};


let login = function() {
    let loginXml =
        `<?xml version="1.0" encoding="UTF-8"?>
            <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:urn="urn:partner.soap.sforce.com">
                <soapenv:Body>
                    <urn:login>
                        <urn:username>` + config.USERNAME + `</urn:username>
                        <urn:password>` + config.PASSWORD + `</urn:password>
                    </urn:login>
                </soapenv:Body>
            </soapenv:Envelope>`


    request.post({
            headers: {
                'Content-Type': 'text/xml; charset=utf-8',
                'SOAPAction': "tacos"
            },
            url: loginUrl,
            body: loginXml,
        },
        function(error, response, body) {
            if (error) {
                console.log(error + ': error');
            } else {
                if (response.body.includes('faultcode')) {
                    console.log(response.body)
                } else {
                    sid = response.body.split('sessionId')[1].replace('>', '').replace('</', '')
                    baseUrl = response.body.split('serverUrl')[1].replace('>', '').replace('</', '').split('/services/Soap/u')[0] + baseUrl;

                    console.log(baseUrl)
                    console.log(config.USERNAME)

                    options.headers = {
                        'Authorization': 'Bearer ' + sid
                    }
                    createBatch();
                }
            }
        });

}

let createBatch = function() {
    console.log('creating job...');

    options.url = baseUrl;
    options.headers['Content-Type'] = 'application/json; charset=UTF-8'
    options.headers['Accept'] = 'application/json'

    options.json = {
        "object": sObject,
        "contentType": fileType,
        "operation": op
    };

    request.post(options, function(error, response, body) {
        if (error) {
            console.log(error + ': error');
        } else {
            jobId = response.body.id;
            if (jobId === undefined) {
                console.log(response.body)
            } else {
                console.log(jobId + ' job created');
                loadFile();
            }
        }
    });
};


let loadFile = function() {
    console.log('reading file ... < 115MB.');

    options.json = ''

    options.url = baseUrl + jobId + '/batches/'
    options.headers['Content-Type'] = 'text/csv'
    options.headers['Accept'] = 'application/json'


    //     let f = `FirstName,LastName,Title,Birthdate
    // Marley Paige,Senior Robot,,1940-06-07Z
    // Laurel Meyer,Chief Robot,,
    // Gabe Meyer,Chief Chief,,`

    var f = fs.readFileSync(fn, 'utf8');

    options.body = f;

    request.put(options, function(error, response, body) {
        if (error) {
            console.log(error + ': file loading error');
        } else {
            console.log('file Loaded');
            closeBatch()
        }
    });
};

let closeBatch = function() {
    console.log('closing batch ');

    options.url = baseUrl + jobId + '/'
    options.headers['Content-Type'] = 'application/json; charset=UTF-8'
    options.headers['Accept'] = 'application/json'

    options.json = {
        "state": "UploadComplete"
    };

    request.patch(options, function(error, response, body) {
        if (error) {
            console.log(error + ': error');
        } else {
            console.log(response.body.state)
            getStatus();
        }
    });
};

let getStatus = function() {
    options.url = baseUrl + jobId
    options.headers['Content-Type'] = 'application/json; charset=UTF-8'
    options.headers['Accept'] = 'application/json'

    request.get(options, function(error, response, body) {
        if (error) {
            console.log(error + ': error');
        } else {
            let state = response.body['state']
            console.log(state)
            if (state !== JOB_COMPLETE) {
                setTimeout(function() { getStatus() }, 2000);
            } else {
                getResults(FAILED_RESULTS);
            }
        }
    });

}

let getResults = function(which) {

    options.url = baseUrl + jobId + '/' + which + '/'
    options.headers['Content-Type'] = 'application/json; charset=UTF-8'
    options.headers['Accept'] = 'text/csv'

    request.get(options, function(error, response, body) {
        if (error) {
            console.log(error + ': error');
        } else {
            console.log(response.body)
            fs.writeFileSync(RESULTS_DIRECTORY + which + ' ' + Date() + '.csv', response.body, 'utf-8');
            if (which === FAILED_RESULTS) {
                getResults(SUCCESSFUL_RESULTS)
            }
        }
    });
}

login();