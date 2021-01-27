const connect = require('tedious').connect;
const EventEmitter = require('events').EventEmitter;
const util = require('util');

function release() {
    this.pool.release(this);
}

const PENDING = 0;
const FREE = 1;
const USED = 2;
const RETRY = 3;

function ConnectionPool(poolConfig, connectionConfig) {
    this.connections = []; //open connections of any state
    this.waiting = []; //acquire() callbacks that are waiting for a connection to come available
    this.connectionConfig = connectionConfig;

    this.max = poolConfig.max || 50;

    this.min = Math.min(this.max, poolConfig.min >= 0 ? poolConfig.min : 10);

    this.idleTimeout = !poolConfig.idleTimeout && poolConfig.idleTimeout !== false
        ? 300000 //5 min
        : poolConfig.idleTimeout;

    this.retryDelay = !poolConfig.retryDelay && poolConfig.retryDelay !== false
        ? 5000
        : poolConfig.retryDelay;

    this.acquireTimeout = !poolConfig.acquireTimeout && poolConfig.acquireTimeout !== false
        ? 60000 //1 min
        : poolConfig.acquireTimeout;

    if (poolConfig.log) {
        if (Object.prototype.toString.call(poolConfig.log) === '[object Function]')
            this.log = poolConfig.log;
        else {
            this.log = function(text) {
                console.log('Tedious-Connection-Pool: ' + text);
            };
        }
    } else {
        this.log = function() {};
    }

    this.drained = false;

    setTimeout(fill.bind(this), 4);
}

util.inherits(ConnectionPool, EventEmitter);

let cid = 1;

function createConnection(pooled) {
    if (this.drained) //pool has been drained
        return;

    const endHandler = () => {
        this.log('connection ended: ' + pooled.id);
        if (this.drained) //pool has been drained
            return;

        for (let i = this.connections.length - 1; i >= 0; i--) {
            if (this.connections[i].con === connection) {
                this.connections.splice(i, 1);
                fill.call(this);
                return;
            }
        }
    };

    const handleError = (err) => {
        this.log('connection closing because of error');

        connection.removeListener('end', endHandler);
        pooled.status = RETRY;
        pooled.con = undefined;
        if (pooled.timeout)
            clearTimeout(pooled.timeout);

        pooled.timeout = setTimeout(createConnection.bind(this, pooled), this.retryDelay);
        this.emit('error', err);
    };

    this.log('creating connection: ' + cid);
    const connection = connect(this.connectionConfig, (err) => {
        if(err) handleError(err);
    });

    connection.release = release;
    connection.pool = this;
    if (pooled) {
        pooled.id = cid++;
        pooled.con = connection;
        pooled.status = PENDING;
    } else {
        pooled = {
            id: cid++,
            con: connection,
            status: PENDING
        };

        this.connections.push(pooled);
    }

    connection.on('connect', (err) => {
        this.log('connection connected: ' + pooled.id);
        if (this.drained) { //pool has been drained
            this.log('connection closing because pool is drained');
            connection.close();
            return;
        }

        if (err) {
            handleError(err);
            return;
        }

        const waiter = this.waiting.shift();
        if (waiter !== undefined)
            setUsed.call(this, pooled, waiter);
        else
            setFree.call(this, pooled);
    });

    connection.on('error', handleError);
    connection.on('end', endHandler);
}

function fill() {
    if (this.drained) //pool has been drained
        return;

    let available = 0;
    for (let i = this.connections.length - 1; i >= 0; i--) {
        if (this.connections[i].status !== USED) {
            available++;
        }
    }

    let amount = Math.min(
        this.max - this.connections.length, //max that can be created
        this.waiting.length - available); //how many are needed, minus how many are available

    amount = Math.max(
        this.min - this.connections.length, //amount to create to reach min
        amount);

    if (amount > 0)
        this.log('filling pool with ' + amount);

    for (let i = 0; i < amount; i++)
        createConnection.call(this);
}

ConnectionPool.prototype.acquire = function (callback) {
    if (this.drained) //pool has been drained
        return;

    const self = this;
    let free;

    //look for free connection
    const l = this.connections.length;
    for (let i = 0; i < l; i++) {
        const pooled = this.connections[i];

        if (pooled.status === FREE) {
            free = pooled;
            break;
        }
    }

    const waiter = {
        callback: callback
    };

    if (free === undefined) { //no valid connection found
        if (this.acquireTimeout) {

            waiter.timeout = setTimeout(function () {
                for (let i = self.waiting.length - 1; i >= 0; i--) {
                    const waiter2 = self.waiting[i];

                    if (waiter2.timeout === waiter.timeout) {
                        self.waiting.splice(i, 1);
                        waiter.callback(new Error('Acquire Timeout Exceeded'));
                        return;
                    }
                }
            }, this.acquireTimeout);
        }

        this.waiting.push(waiter);
        fill.call(this);
    } else {
        setUsed.call(this, free, waiter);
    }
};

function setUsed(pooled, waiter) {
    pooled.status = USED;
    if (pooled.timeout) {
        clearTimeout(pooled.timeout);
        pooled.timeout = undefined;
    }
    if (waiter.timeout) {
        clearTimeout(waiter.timeout);
        waiter.timeout = undefined;
    }
    this.log('acquired connection ' + pooled.id);
    waiter.callback(null, pooled.con);
}

function setFree(pooled) {
    pooled.status = FREE;
    pooled.timeout = setTimeout(() => {
        this.log('closing idle connection: ' + pooled.id);
        pooled.con.close();
    }, this.idleTimeout);
}

ConnectionPool.prototype.release = function(connection) {
    if (this.drained) //pool has been drained
        return;

    const self = this;
    let i, pooled;

    for (i = self.connections.length - 1; i >= 0; i--) {
        pooled = self.connections[i];

        if (pooled.con === connection) {
            //reset connection & release it
            connection.reset(function (err) {
                if (!pooled.con) //the connection failed during the reset
                    return;

                if (err) { //there is an error, don't reuse the connection, just close it
                    pooled.con.close();
                    return;
                }
                self.log('connection reset: ' + pooled.id);

                const waiter = self.waiting.shift();

                if (waiter !== undefined) {
                    setUsed.call(self, pooled, waiter);
                    //if (waiter.timeout)
                    //    clearTimeout(waiter.timeout);
                    //waiter.callback(null, connection);
                } else {
                    setFree.call(self, pooled);
                }
            });

            return;
        }
    }
};

ConnectionPool.prototype.drain = async function () {
    this.log('draining pool');
    if (this.drained) {//pool has been drained
        return;
    }

    // Flag as drained as this prevent others from acquiring new connections from the pool.
    this.drained = true;

    for (let i = this.waiting.length - 1; i >= 0; i--) {
        const waiter = this.waiting[i];

        if (waiter.timeout)
            clearTimeout(waiter.timeout);
    }

    this.waiting.length = 0;

    if (this.connections.length === 0) return

    for (let i = this.connections.length - 1; i >= 0; i--) {
        const pooled = this.connections[i];

        if (pooled.timeout)
            clearTimeout(pooled.timeout);

        if (pooled.con) {
            if (pooled.con.request) {
                pooled.con.request.cancel();

                // Make sure no requests are running before closing the connection.
                while (pooled.con.request) {
                    await new Promise(done => setTimeout(done, 100));
                }
            }
            pooled.con.close();
        }
    }

    this.connections = null;
};

module.exports = ConnectionPool;
