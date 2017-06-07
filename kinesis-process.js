var async = require('async');
var util = require('util');
var config = require('./config');
var recordBuffer = require('./record_buffer');
var s3Emitter = require('./s3_emitter');
var logger = require('./logger');

var emitter = s3Emitter(config.s3);
var buffer = recordBuffer(config.clickStreamProcessor.maxBufferSize);
var shardId = null;
var commitQueue = null;
var log = logger().getLogger('recordProcessor');

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

function recordProcessor() {

    return {
        initialize: function (initializeInput, completeCallback) {
            shardId = initializeInput.shardId;
            // Your application specific initialization logic.

            // After initialization is done, call completeCallback,
            // to let the KCL know that the initialize operation is
            // complete.
            // completeCallback();

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
        processRecords: function (processRecordsInput, completeCallback) {
            // Sample code for record processing.
            if (!processRecordsInput || !processRecordsInput.records) {
                // Invoke callback to tell the KCL to process next batch
                // of records.
                completeCallback();
                return;
            }
            var records = processRecordsInput.records;

            async.series([
                    function (done) {
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
                            log.info("Processed ", processedCount);
                            if (processedCount === records.length) {
                                done(errors, errorCount);
                            }
                        };

                        for (var i = 0; i < records.length; ++i) {
                            record = records[i];
                            _processRecord(record, processRecordsInput.checkpointer, callback);
                        }
                    }
                ],
                function (err, errCount) {
                    if (err) {
                        log.info(util.format('%d records processed with %d errors.', records.length, errCount));
                    }
                    completeCallback();
                });


            // var record, sequenceNumber, partitionKey, data;
            // for (var i = 0 ; i < records.length ; ++i) {
            //     record = records[i];
            //     sequenceNumber = record.sequenceNumber;
            //     partitionKey = record.partitionKey;
            //     // Data is in base64 format.
            //     data = new Buffer(record.data, 'base64').toString();
            //     // Record processing logic here.
            // }
            // // Checkpoint last sequence number.
            // processRecordsInput.checkpointer.checkpoint(
            //     sequenceNumber, function(err, sn) {
            //         // Error handling logic. In this case, we call
            //         // completeCallback to process more data.
            //         completeCallback();
            //     }
            // );
        },
        shutdown: function (shutdownInput, completeCallback) {
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
            }, function (error) {
                if (error) {
                    log.error(util.format('Received error while shutting down: %s', error));
                }
                completeCallback();
            });
        }
    }
}

var kcl = require('./');
kcl(recordProcessor()).run();