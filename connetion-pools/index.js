/*
 * ============================================================================
 *  NOTES: What I learned about DB connections & connection pooling (Postgres)
 * ============================================================================
 *
 * 1. WHAT A CONNECTION IS
 *    - Node and Postgres are separate programs. Before any SQL can run they set
 *      up a connection: TCP handshake -> startup message -> password auth
 *      (SCRAM) -> "ready". Postgres also starts a NEW OS PROCESS for every
 *      connection (a few MB to ~10 MB of RAM each).
 *    - Setting up a connection costs far more than a tiny query like SELECT NOW().
 *    - Postgres limits connections (max_connections, default 100) to protect
 *      memory. Going over it gives: "sorry, too many clients already".
 *
 * 2. NON-POOLED (new Client per query)
 *    - Every query pays connect + auth + close, so most of the time goes to setup.
 *    - A for-loop with no `await` doesn't run queries one after another:
 *      client.connect() returns immediately, so all 1000 connections start at the
 *      SAME moment -> concurrency of 1000 -> crash at 100.
 *    - That's a BURST (1000 at t=0), not "1000 req/sec". A steady 1000 req/s
 *      needs only (rate x time per request) connections at once (Little's Law),
 *      e.g. 1000/s x 5 ms ~ 5 connections.
 *    - With `await` inside the loop it would be sequential: never hits the
 *      limit, but very slow.
 *    - If Promise.all fails, the clients that did connect never call end().
 *      They stay open and hold the slots until the process exits (Ctrl+C).
 *    - `poolSize` on a Client does nothing; a Client is always ONE connection.
 *
 * 3. POOLED (new Pool)
 *    - Keeps a small set of connections OPEN and REUSES them (default max = 10).
 *    - Extra queries wait in a queue INSIDE Node; Postgres never sees more than
 *      `max` connections. 1000 queries -> only 10 connection setups.
 *    - A pool only helps if `max` is SMALLER than the number of queries arriving
 *      at once. `poolSize: 1000` (old name for `max`) = no pooling at all ->
 *      same "too many clients" error.
 *    - Config key is `connectionString`, NOT `url` (pg silently ignores `url`
 *      and falls back to localhost:5432 + no password -> SCRAM
 *      "client password must be a string" error).
 *
 * 4. ONE SHARED CLIENT vs POOL (question 1)
 *    - Reusing one Client for everything saves setup cost, but:
 *      a) One connection runs ONE query at a time; one slow query holds up the
 *         rest (no parallelism).
 *      b) A transaction belongs to the CONNECTION. If request A runs BEGIN,
 *         request B's queries on the same client end up in A's transaction, and
 *         A's ROLLBACK also undoes B's work.
 *      c) If that one connection drops, every request fails.
 *    - Fine for scripts; not for servers. With a pool, use pool.connect() ->
 *      BEGIN/COMMIT -> client.release() so each transaction has its own connection.
 *
 * 5. ARRAY OF CLIENTS AS A POOL (question 2)
 *    - Possible; that's basically what Pool is inside. But Pool already handles
 *      checkout/release, queueing, replacing broken connections, idle timeouts and
 *      releasing on errors. Use Pool.
 *
 * 6. PROCESSES vs CPU CORES
 *    - A process doesn't need its own core. It only uses a core while RUNNING;
 *      most of the time it SLEEPS (waiting for network/disk/input) and uses 0 CPU.
 *    - The OS scheduler time-slices: processes take turns on cores for a few ms.
 *      Concurrency (taking turns) != parallelism (at the same instant, max = cores).
 *    - My laptop: ~530 processes, ~10 cores, only ~3 running at a time.
 *    - 100 connections = 100 mostly-sleeping processes -> works fine on 10 cores.
 *    - The real cost of a connection is memory, setup time and internal contention
 *      inside Postgres, not cores.
 *    - Cores matter when many queries do heavy CPU work at once. A common rule of
 *      thumb: pool size ~ cores x 2 (+ some for disk waits), not 100 or 1000.
 *
 * 7. WHY POSTGRES IS GOOD AT CONCURRENCY
 *    - NOT because of process-per-connection. That is actually a weakness:
 *      expensive per connection, so the limit is low. (MySQL uses threads, which
 *      are cheaper.) Upside: one crashed process doesn't take the others with it.
 *    - That's why production setups use poolers: app-side (pg.Pool) and/or
 *      PgBouncer (thousands of clients -> ~20-50 real connections).
 *    - The real reason is MVCC (Multi-Version Concurrency Control):
 *      UPDATE writes a NEW row version instead of overwriting the old one, so
 *      READERS DON'T BLOCK WRITERS AND WRITERS DON'T BLOCK READERS. Only writes
 *      to the SAME row wait for each other.
 *    - High-concurrency recipe: a FEW real connections (pooled) + MVCC.
 *
 * 8. WHY CONNECTING TAKES MULTIPLE ROUND TRIPS (can't be one request)
 *    - ~4 round trips: TCP handshake -> startup(user, db) -> SCRAM step 1 ->
 *      SCRAM step 2 -> ready. Each step needs something from the other side first.
 *    - TCP: both sides must agree on the starting numbers used to track bytes
 *      before any data can be sent reliably.
 *    - The client doesn't know the login method in advance. Postgres picks it
 *      (trust / scram / cert ...) from pg_hba.conf AFTER seeing user, db and IP.
 *    - SCRAM is challenge-response ON PURPOSE:
 *        client: "I'm X + my random number"
 *        server: "my random number + your salt + iteration count"
 *        client: proof built from password + salt + BOTH random numbers
 *        server: checks it, then sends its own proof (so the client knows it's
 *                the real server too)
 *      -> The password never goes over the network.
 *      -> The server's fresh random number makes each proof single-use, so a
 *         recorded login can't be REPLAYED. A one-shot login could be.
 *      -> The client needs the salt, which only the server has = a wait.
 *    - Password hashing repeats 4096x by default (slows down guessing), so it
 *      costs CPU on every connect.
 *    - Cost = round trips x network delay: localhost ~0.2 ms, same data centre
 *      ~2 ms, remote cloud DB ~200 ms+ per connect. That's why pooling matters
 *      even more for a remote DB.
 *    - Shortcuts: TLS 1.3, Postgres 17 `sslnegotiation=direct` (skips one round
 *      trip). Auth itself can't be skipped safely; pooling avoids paying it on
 *      every query.
 *
 * 9. POOL `max` vs SERVER `max_connections` (two different limits)
 *    - Pool `max` = a ceiling INSIDE my app ("I may open up to N").
 *    - Postgres `max_connections` = what the SERVER accepts (default 100).
 *    - The LOWER one wins. `max: 100000` doesn't give permission on the server.
 *      It just stops the pool from queueing -> it opens a connection per query
 *      -> "too many clients" again. (Like booking 100,000 seats at a 100-seat
 *      theatre.)
 *    - `max` on a Client is ignored (a Client is always 1 connection).
 *    - A pool only helps when max < server limit; then the extra queries wait in
 *      a queue inside Node.
 *    - The only way past 100 is to change the SERVER:
 *      docker run ... postgres:17 -c max_connections=1100
 *
 * 10. WHY NOT AN "INFINITE" POOL / max_connections?
 *    - MEMORY: ~5-10 MB per connection (1,000 ~ 5-10 GB, 10,000 ~ 50-100 GB),
 *      plus work_mem (4 MB) per sort/join per query:
 *      1,000 conns x 3 sorts x 4 MB = 12 GB more. Out of memory -> Linux kills a
 *      process -> Postgres restarts and disconnects EVERYONE.
 *    - THROUGHPUT FALLS past a peak (~ cores x 2-4):
 *        * CPU switching: 1,000 busy processes on 10 cores -> more time spent
 *          switching (and flushing CPU caches) than working.
 *        * Lock contention on shared internals (buffers, transaction list, WAL).
 *        * MVCC snapshots: every query checks the list of running transactions;
 *          more connections = a longer list.
 *      -> 20 busy connections often finish MORE queries/sec than 1,000.
 *    - MULTIPLIES across app instances: 20 servers x pool 100 = 2,000
 *      connections. Classic outage: traffic spike -> autoscale -> every new
 *      instance opens its full pool -> DB falls over.
 *    - OS limits: file descriptors, sockets, process limits (ulimit).
 *    - What production does:
 *        * max_connections 100-500
 *        * app pool max ~10-20 per instance
 *        * total across all instances < max_connections
 *        * PgBouncer in front for many clients/serverless
 *    - Rule of thumb (HikariCP): pool size ~ (cores x 2) + disks.
 *      ~20-25 for my 10-core laptop.
 *    - An "infinite" pool just moves the waiting from a cheap queue in Node to an
 *      expensive pile-up inside Postgres. Small pool + queue = faster and stable.
 * ============================================================================
 */

const { Client } = require('pg');


function benchMarkDBPoolConnection() {
    const { Pool } = require('pg');
    const pool = new Pool({connectionString: 'postgresql://postgres:postgres@localhost:5434/prototype-connection',
        poolSize: 1000

    });

    const startTime = Date.now();
    const promises = [];

    for (let i = 0; i < 1000; i++) {
        promises.push(pool.query('SELECT pg_sleep(0.01)'));
    }

    Promise.all(promises)
        .then(() => {
            const endTime = Date.now();
            console.log(`Pool connection time: ${endTime - startTime} ms`);
            pool.end();
        })
        .catch((err) => {
            console.error('Error executing query', err.stack);
            pool.end();
        });
}

function benchMarkDBClientConnectionNonPooling() {
    const startTime = Date.now();
    const promises = [];

    for (let i = 0; i < 1000; i++) {
        //  question 1 : what if we create this outside the loop and reuse the same client for all queries? Would that be more efficient?
        // question 2: and can we use 'Client' instead of 'Pool' for connection pooling using an array? is that possible? or is it better to use 'Pool' for connection pooling?
        const client = new Client({
            connectionString: 'postgresql://postgres:postgres@localhost:5434/prototype-connection',
            max: 100000,
        });

        promises.push(
            client.connect()
                .then(() => client.query('SELECT pg_sleep(0.01)'))
                .then(() => client.end())
        );
    }

    Promise.all(promises)
        .then(() => {
            const endTime = Date.now();
            console.log(`Client connection time: ${endTime - startTime} ms`);
        })
        .catch((err) => {
            console.error('Error executing query', err.stack);
        });
}

function benchMarkDBConnection() {
    benchMarkDBClientConnectionNonPooling();
    // benchMarkDBPoolConnection();
}

benchMarkDBConnection();