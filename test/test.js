const assert = require('assert');
const Request = require('tedious').Request;
const ConnectionPool = require('../lib/connection-pool');
const Connection = require('tedious').Connection;

const connectionConfig= {
    authentication: {
        type: "default",
        options: {
            userName: 'test',
            password: 'test'
        }
    },
    server: 'localhost',
    options: {
        appName: 'pool-test',
        database: 'test',
        trustServerCertificate: true // Don't do this in production!
    }
};

const timeout = 10000;

/* create a db user with the correct permissions:
CREATE DATABASE test
CREATE LOGIN test WITH PASSWORD=N'test', DEFAULT_DATABASE=test, CHECK_POLICY=OFF
GRANT ALTER ANY CONNECTION TO test

USE test
CREATE USER test FOR LOGIN test WITH DEFAULT_SCHEMA=dbo
ALTER ROLE db_owner ADD MEMBER test

USE msdb
CREATE USER test FOR LOGIN test WITH DEFAULT_SCHEMA=dbo
ALTER ROLE SQLAgentOperatorRole ADD MEMBER test
ALTER ROLE SQLAgentReaderRole ADD MEMBER test
ALTER ROLE SQLAgentUserRole ADD MEMBER test
*/

/* disable the user when not testing:
ALTER LOGIN test DISABLE
*/

describe('Name Collision', function () {

    it('release', function () {
        assert(!Connection.prototype.release);

        const con = new Connection(connectionConfig);
        assert(!con.release);
        con.close();
    });

    it('pool', function () {
        assert(!Connection.prototype.pool);

        const con = new Connection(connectionConfig);
        assert(!con.pool);
        con.close();
    });
});

describe('ConnectionPool', function () {

    it('min', function (done) {
        this.timeout(timeout);

        const poolConfig = {min: 2};
        const pool = new ConnectionPool(poolConfig, connectionConfig);

        setTimeout(function() {
            assert.strictEqual(pool.connections.length, poolConfig.min);
            pool.drain().then(done);
        }, 4);
    });

    it('min=0', function (done) {
        this.timeout(timeout);

        const poolConfig = {min: 0, idleTimeout: 10};
        const pool = new ConnectionPool(poolConfig, connectionConfig);

        setTimeout(function() {
            assert.strictEqual(pool.connections.length, 0);
        }, 4);

        setTimeout(function() {
            pool.acquire(function(err, connection) {
                assert(!err);

                const request = new Request('select 42', function (err, rowCount) {
                    assert.strictEqual(rowCount, 1);
                    connection.release();
                    setTimeout(function () {
                        assert.strictEqual(pool.connections.length, 0);
                        pool.drain().then(done);
                    }, 200);
                });

                request.on('row', function (columns) {
                    assert.strictEqual(columns[0].value, 42);
                });

                connection.execSql(request);
            });
        }, 2000);
    });

    it('max', function (done) {
        this.timeout(timeout);

        const poolConfig = {min: 2, max: 5};
        const pool = new ConnectionPool(poolConfig, connectionConfig);

        const count = 20;
        let run = 0;

        const createRequest = function (err, connection) {
            assert(!err);

            const request = new Request('select 42', function (err, rowCount) {
                assert.strictEqual(rowCount, 1);
                setTimeout(function() {
                    run++;
                    assert(pool.connections.length <= poolConfig.max);
                    if (run === count) {
                        pool.drain().then(done);
                        return;
                    }
                    connection.release();
                }, 200);
            });

            request.on('row', function (columns) {
                assert.strictEqual(columns[0].value, 42);
            });

            connection.execSql(request);
        };

        for (let i = 0; i < count; i++) {
            setTimeout(function() {
                pool.acquire(createRequest);
            }, 1);
        }
    });

    it('min<=max, min specified > max specified', function (done) {
        this.timeout(timeout);

        const poolConfig = { min: 5, max: 1, idleTimeout: 10};
        const pool = new ConnectionPool(poolConfig, connectionConfig);

        setTimeout(function() {
            assert.strictEqual(pool.connections.length, 1);
        }, 4);

        setTimeout(function() {
            pool.acquire(function(err, connection) {
                assert(!err);

                const request = new Request('select 42', function (err, rowCount) {
                    assert.strictEqual(rowCount, 1);
                    connection.release();
                    setTimeout(function () {
                        assert.strictEqual(pool.connections.length, 1);
                        pool.drain().then(done);
                    }, 200);
                });

                request.on('row', function (columns) {
                    assert.strictEqual(columns[0].value, 42);
                });

                connection.execSql(request);
            });
        }, 2000);
    });

    it('min<=max, no min specified', function (done) {
        this.timeout(timeout);

        const poolConfig = {max: 1, idleTimeout: 10};
        const pool = new ConnectionPool(poolConfig, connectionConfig);

        setTimeout(function() {
            assert.strictEqual(pool.connections.length, 1);
        }, 4);

        setTimeout(function() {
            pool.acquire(function(err, connection) {
                assert(!err);

                const request = new Request('select 42', function (err, rowCount) {
                    assert.strictEqual(rowCount, 1);
                    connection.release();
                    setTimeout(function () {
                        assert.strictEqual(pool.connections.length, 1);
                        pool.drain().then(done);
                    }, 200);
                });

                request.on('row', function (columns) {
                    assert.strictEqual(columns[0].value, 42);
                });

                connection.execSql(request);
            });
        }, 2000);
    });

    it('pool error event', function (done) {
        this.timeout(timeout);
        const poolConfig = {min: 0, max: 1};

        // Use a garbage IP address to force lookup failure.
        const pool = new ConnectionPool(poolConfig, {...connectionConfig, server: "1.2544.1231.3" });

        pool.acquire(function() {});

        pool.on('error', function(err) {
            assert(!!err);
            pool.drain().then(done);
        });
    });

    it('connection retry', function (done) {
        this.timeout(timeout);
        const poolConfig = {min: 1, max: 5, retryDelay: 5};
        const pool = new ConnectionPool(poolConfig, connectionConfig);

        pool.on('error', function(err) {
            assert(!!err);
            pool.connectionConfig = connectionConfig;
        });

        function testConnected() {
            for (let i = pool.connections.length - 1; i >= 0; i--) {
                if (pool.connections[i].status === 3/*RETRY*/) {
                    setTimeout(testConnected, 100);
                    return;
                }
            }

            assert.strictEqual(pool.connections.length, poolConfig.min);
            pool.drain().then(done);
        }

        setTimeout(testConnected, 100);
    });

    it('acquire timeout', function (done) {
        this.timeout(timeout);

        const poolConfig = {min: 1, max: 1, acquireTimeout: 2000};
        const pool = new ConnectionPool(poolConfig, connectionConfig);

        pool.acquire(function(err, connection) {
            assert(!err);
            assert(!!connection);
        });

        pool.acquire(function(err, connection) {
            assert(!!err);
            assert(!connection);
            pool.drain().then(done)
        });
    });

    it('idle timeout', function (done) {
        this.timeout(timeout);
        const poolConfig = {min: 1, max: 5, idleTimeout: 100};
        const pool = new ConnectionPool(poolConfig, connectionConfig);

        setTimeout(function() {
            pool.acquire(function (err, connection) {
                assert(!err);

                const request = new Request('select 42', function (err, rowCount) {
                    assert.strictEqual(rowCount, 1);
                    pool.drain().then(done);
                });

                request.on('row', function (columns) {
                    assert.strictEqual(columns[0].value, 42);
                });

                connection.execSql(request);
            });

        }, 300);
    });

    it('connection error handling', function (done) {
        this.timeout(timeout);
        const poolConfig = {min: 1, max: 5};

        const pool = new ConnectionPool(poolConfig, connectionConfig);

        pool.on('error', function(err) {
            assert(err && err.name === 'ConnectionError');
        });

        //This simulates a lost connections by creating a job that kills the current session and then deletes the job.
        pool.acquire(function (err, connection) {
            assert(!err);

            const command = 'DECLARE @jobName constCHAR(68) = \'pool\' + CONVERT(constCHAR(64),NEWID()), @jobId UNIQUEIDENTIFIER;' +
            'EXECUTE msdb..sp_add_job @jobName, @owner_login_name=\'' + connectionConfig.userName + '\', @job_id=@jobId OUTPUT;' +
            'EXECUTE msdb..sp_add_jobserver @job_id=@jobId;' +

            'DECLARE @cmd constCHAR(50);' +
            'SELECT @cmd = \'kill \' + CONVERT(constCHAR(10), @@SPID);' +
            'EXECUTE msdb..sp_add_jobstep @job_id=@jobId, @step_name=\'Step1\', @command = @cmd, @database_name = \'' + connectionConfig.options.database + '\', @on_success_action = 3;' +

            'DECLARE @deleteCommand constCHAR(200);' +
            'SET @deleteCommand = \'execute msdb..sp_delete_job @job_name=\'\'\'+@jobName+\'\'\'\';' +
            'EXECUTE msdb..sp_add_jobstep @job_id=@jobId, @step_name=\'Step2\', @command = @deletecommand;' +

            'EXECUTE msdb..sp_start_job @job_id=@jobId;' +
            'WAITFOR DELAY \'01:00:00\';' +
            'SELECT 42';

            const request = new Request(command, function (err, rowCount) {
                assert(err);
                pool.drain().then(done);
            });

            request.on('row', function (columns) {
                assert(false);
            });

            connection.execSql(request);
        });
    });

    it('release(), reset()', function (done) {
        this.timeout(timeout);

        const poolConfig = {max: 1};
        const pool = new ConnectionPool(poolConfig, connectionConfig);

        const createRequest = function(query, value, callback) {
            const request = new Request(query, function (err, rowCount) {
                assert.strictEqual(rowCount, 1);
                callback();
            });

            request.on('row', function (columns) {
                assert.strictEqual(columns[0].value, value);
            });

            return request;
        };

        pool.acquire(function(err, conn) {
            assert(!err);

            conn.execSql(createRequest('SELECT 42', 42, function () {
                pool.release(conn); //release the connect

                pool.acquire(function (err, conn) { //re-acquire the connection
                    assert(!err);

                    conn.execSql(createRequest('SELECT 42', 42, function () {

                        pool.drain().then(done);
                    }));
                });
            }));
        });
    });

    it('drain', function (done) {
        this.timeout(timeout);

        const poolConfig = {min: 3};
        const pool = new ConnectionPool(poolConfig, connectionConfig);

        pool.acquire(function() { });

        setTimeout(function() {
            assert.strictEqual(pool.connections.length, poolConfig.min);
            pool.drain().then(done);
        }, 4);
    });
});


describe('Load Test', function () {
    const statistics = require('simple-statistics');

    it('Memory Leak Detection - Connection Error', function (done) {
        this.timeout(120000);
        if (!global.gc)
            throw new Error('must run nodejs with --expose-gc');
        let count = 0;
        const mem = [];
        const groupCount = 20;
        const poolSize = 1000;
        const max = poolSize * groupCount;

        const badConnectionConfig = {
            ...connectionConfig,
            authentication: {
                type: "default",
                options: {
                    userName: "baduserthatdoesntexist",
                    password: "passwordforuserthatdoesntexist"
                }
            }
        }

        const pool = new ConnectionPool({max: poolSize, min: poolSize, retryDelay: 1}, badConnectionConfig);

        pool.on('error', function () {
            if ((++count % poolSize) !== 0)
                return;

            global.gc();

            const heapUsedKB = Math.round(process.memoryUsage().heapUsed / 1024);
            mem.push([count, heapUsedKB]);
            console.log(count + ': ' + heapUsedKB + 'KB');

            if (count === max) {
                setTimeout(() => {
                    global.gc();
                    const data = statistics.linearRegression(mem);
                    console.log(data.m);
                    if (data.m >= 0.035)
                        done(new Error('Memory leak detected.'));
                    else
                        done();
                }, 2000)
                pool.drain();
            }
        });
    });

    it('Memory Leak Detection - acquire() and Request', function (done) {
        this.timeout(60000);
        if (!global.gc)
            throw new Error('must run nodejs with --expose-gc');

        const poolConfig = {min: 67, max: 123};
        const pool = new ConnectionPool(poolConfig, connectionConfig);

        const clients = 1000;
        const connections = 100;
        const max = clients * connections;
        const mem = [];

        for (let i = 0; i < clients; i++)
            createClient();

        let count = 0;

        function end(err) {
            done(err);
            pool.drain();
        }

        function createClient() {
            let clientCount = 0;

            function createRequest() {
                pool.acquire(function (err, connection) {
                    if (err)
                        return end(err);

                    const request = new Request('select 42', function (err) {
                        if (err)
                            return end(err);

                        connection.release();

                        if (++clientCount < connections)
                            createRequest();

                        if ((++count % 1000) === 0) {
                            global.gc();

                            if (count === max) {
                                const data = statistics.linearRegression(mem);
                                //console.log(data.m);
                                if (data.m >= 0.025)
                                    end(new Error('Memory leak not detected.'));
                                else
                                    end();
                            } else {
                                const heapUsedKB = Math.round(process.memoryUsage().heapUsed / 1024);
                                mem.push([count, heapUsedKB]);
                                // console.log(count + ': ' + heapUsedKB + 'KB');
                            }
                        }
                    });

                    connection.execSql(request);
                });
            }

            createRequest();
        }
    });
});
