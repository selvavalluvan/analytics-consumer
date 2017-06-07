/***
 Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

 Licensed under the Amazon Software License (the "License").
 You may not use this file except in compliance with the License.
 A copy of the License is located at

 http://aws.amazon.com/asl/

 or in the "license" file accompanying this file. This file is distributed
 on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 express or implied. See the License for the specific language governing
 permissions and limitations under the License.
 ***/

'use strict';

var async = require('async');
var util = require('util');
var kcl = require('aws-kcl');
var logger = require('./logger');
var recordBuffer = require('./record_buffer');
var s3Emitter = require('./s3_emitter');
var config = require('./config');

/**
 * A simple implementation for the record processor (consumer) that simply writes the data to a log file.
 *
 * Be careful not to use the 'stderr'/'stdout'/'console' as log destination since it is used to communicate with the
 * {https://github.com/awslabs/amazon-kinesis-client/blob/master/src/main/java/com/amazonaws/services/kinesis/multilang/package-info.java MultiLangDaemon}.
 */

function recordProcessor(emitter) {
    var log = logger().getLogger('recordProcessor');
    var shardId;
    var buffer = recordBuffer(config.clickStreamProcessor.maxBufferSize);
    var commitQueue = null;

    function _commit(commitInfo, callback) {
        var key = commitInfo.key;
        var sequenceNumber = commitInfo.sequenceNumber;
        var data = commitInfo.data;
        var checkpointer = commitInfo.checkpointer;
        emitter.emit(key, data, function(error) {
            if (error) {
                callback(error);
                return;
            }
            log.info(util.format('Successfully uploaded data to s3 file: %s', key));
            checkpointer.checkpoint(sequenceNumber, function(e, seq) {
                if (!e) {
                    log.info('Successful checkpoint at sequence number: %s', sequenceNumber);
                }
                callback(e);
            });
        });
    }

    function _processRecord(record, checkpointer, callback) {
        var data = new Buffer(record.data, 'base64').toString();
        log.info(util.format('data:::: %s, buffer size:::: %d', data, buffer.getSize()));
        var sequenceNumber = record.sequenceNumber;

        // Add data to buffer until maxBufferSize.
        buffer.putRecord(data, sequenceNumber);

        if (!buffer.shouldFlush()) {
            callback(null);
            return;
        }
        // Buffer is full. Add commit to the queue.
        commitQueue.push({
            key: shardId + '/' + buffer.getFirstSequenceNumber() + '-' + buffer.getLastSequenceNumber(),
            sequenceNumber: buffer.getLastSequenceNumber(),
            data: buffer.readAndClearRecords(),
            checkpointer: checkpointer
        }, callback);
    }

    return {

        initialize: function(initializeInput, completeCallback) {
            shardId = initializeInput.shardId;

            commitQueue = async.queue(_commit, 1);

            emitter.initialize(function (err) {
                if (err) {
                    log.error(util.format('Error initializing emitter: %s', err));
                    process.exit(1);
                }
                else {
                    log.info('Click stream processor successfully initialized.');
                    completeCallback();
                }
            });
        },

        processRecords: function(processRecordsInput, completeCallback) {
            if (!processRecordsInput || !processRecordsInput.records) {
                completeCallback();
                return;
            }
            var records = processRecordsInput.records;

            async.series([
                function(done) {
                    var record;
                    var processedCount = 0;
                    var errorCount = 0;
                    var errors;

                    var callback = function (err) {
                        if (err) {
                            log.error(util.format('Received error while processing record: %s', err));
                            errorCount++;
                            errors = errors + '\n' + err;
                        }

                        processedCount++;
                        if (processedCount === records.length) {
                            done(errors, errorCount);
                        }
                    };

                    for (var i = 0 ; i < records.length ; ++i) {
                        record  = records[i];
                        _processRecord(record, processRecordsInput.checkpointer, callback);
                    }
                }
            ],
            function(err, errCount) {
                if (err) {
                    log.info(util.format('%d records processed with %d errors.', records.length, errCount));
                }
                completeCallback();
            });


            // var record, data, sequenceNumber, partitionKey;
            // for (var i = 0 ; i < records.length ; ++i) {
            //     record = records[i];
            //     data = new Buffer(record.data, 'base64').toString();
            //     sequenceNumber = record.sequenceNumber;
            //
            //     // Add data to buffer until maxBufferSize.
            //     buffer.putRecord(data, sequenceNumber);
            //
            //     if (buffer.shouldFlush()) {
            //         log.info(util.format('Sending data to S3'));
            //         _commit({
            //             key: shardId + '/' + buffer.getFirstSequenceNumber() + '-' + buffer.getLastSequenceNumber(),
            //             sequenceNumber: buffer.getLastSequenceNumber(),
            //             data: buffer.readAndClearRecords(),
            //             checkpointer: processRecordsInput.checkpointer
            //         }, function (error) {
            //             if(error){
            //                 log.info(util.format('Error in _commit : %s', error));
            //             }
            //         });
            //     }
            //
            //     partitionKey = record.partitionKey;
            //     log.info(util.format('ShardID: %s, Record: %s, SeqenceNumber: %s, PartitionKey:%s', shardId, data, sequenceNumber, partitionKey));
            // }
            // if (!sequenceNumber) {
            //     completeCallback();
            //     return;
            // }
        },

        shutdown: function(shutdownInput, completeCallback) {
            if (shutdownInput.reason !== 'TERMINATE') {
                completeCallback();
                return;
            }
            // Make sure to emit all remaining buffered data to S3 before shutting down.
            commitQueue.push({
                key: shardId + '/' + buffer.getFirstSequenceNumber() + '-' + buffer.getLastSequenceNumber(),
                sequenceNumber: buffer.getLastSequenceNumber(),
                data: buffer.readAndClearRecords(),
                checkpointer: shutdownInput.checkpointer
            }, function(error) {
                if (error) {
                    log.error(util.format('Received error while shutting down: %s', error));
                }
                completeCallback();
            });
        }
    };
}

kcl(recordProcessor(s3Emitter(config.s3))).run();
