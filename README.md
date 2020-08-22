# Taskmaster

<img src="https://img.cinemablend.com/filter:scale/quill/b/3/6/2/9/9/b36299d3e49f972d430cae647b5bec83ad70eae8.jpg?mw=600" align="right" width=250 >

Postgres based background processing worker system thing.
:warning: Somewhat usable, see `dev-resources/taskmaster/` directory for samples and examples

Recommended environment:

- Linux
- Postgres 11
- JDK 11
- Clojure 1.10.1

Required:



- HikariCP connection pool



> will probably work with Postgres 9.6, Clojure 1.9 and JDK 8.


# Usage


## Database setup

You'll need a Postgres server, and a databsae created. Then you can create the necessary jobs table structure:

> :warning: This has to run before you start any consumers, as they will attempt to fetch any unprocessed jobs while starting!

```clojure
(taskmaster.operation/create-jobs-table! jdbc-connection)

```

## Defining handlers for consuming jobs

Now you can define your consumers, and a handler function which will process each job. If processing is done, return `:taskmaster.operation/ack` qualified symbol. If not, return `:taskmaster.operation/reject`.

Example:


```clojure

(defn handler [{:keys [payload]}]
  (if (do/some-other-work payload)
    :taskmaster.operation/ack
    :taskmaster.operation/reject))
```


Rejecting a job will *keep the failed job data in the jobs table* so that you can:

- requeue it by using `(taskmaster.operation/requeue! conn {:id [faild-job-id-1 failed-job-id-2]})`
- implement a garbage collector and delete them some other time

Failed jobs can be found by querying for `run_count > 0` or via


```clojure
(taskmaster.operation/find-failed-jobs conn {:queue-name queue-name})
```

## Consuming

Let's define a consumer:

```clojure
(def consumer
  (taskmaster.queue/start! db-conn {:queue-name "do_work_yo"
                                    :handler handler
                                    :concurrency 3}))


```

Consumer will spin up 1 listener thread to be notified about new reocrds being inserted matching the queue name and 3 threads to process these jobs, in parallel, one at a time.

## Publishing

Now let's queue up some jobs:


```clojure

(taskmaster.queue/put! db-conn {:queue-name "do_work_yo" :payload {:send-email "test@example.com"}})
```

By default, job payloads are stored as JSON and Taskmaster is setup to serialize/deserialize it using Cheshire.


# Next steps

That's it, you can now add other fun things like:

- use the [middleware pattern](http://clojure-doc.org/articles/cookbooks/middleware.html) to add instrumentation and error reporting
  - see example middlewares here: [test/taskmaster/middleware_test.clj](test/taskmaster/middleware_test.clj)
- spin up multiple consumers to consume from many "queues"
- add some sort of schema validation when pushing/pulling data off the queue


## Components

[Component](https://github.com/stuartsierra/component) approach is recommended, but it's not stricly necessary.
At minimum you'll need

- HikariCP connection pool component
- Consumer component(s)

Publisher is not necessary, as you can ivoke `taskmaster.queue/put!` directly, but it's good to have one anyway, especially in tests.

### Messy example


```clojure
(require '[taskmaster.dev.connection :as c]
         '[taskmaster.component :as com]
         '[clojure.tools.logging :as log]
         '[com.stuartsierra.component :as component])

;; stores all payloads
(def qs (atom []))


;; `component` is the whole consumer component here - so you have access to its' dependencies
(defn handler [{:keys [id queue-name payload component] :as job}]
  (log/infof "got-job t=%s q=%s %s" component queue-name payload)
  (swap! qs conj id)
  (log/info (count (set @qs)))
  ;; fail the job if some-number in the payload is not even
  (let [res   (if (and (:some-number payload) (even? (:some-number payload)))
                :taskmaster.operation/ack
                :taskmaster.operation/reject)]
    (log/info res)
    res))


(def system
  {:db-conn (c/make-one)
   :consumer (component/using
              (com/create-consumer {:queue-name "t3"
                                    :handler handler
                                    :concurrency 2})
              [:db-conn :some-thing])
   :some-thing {:some :thing}
   :publisher (component/using
               (com/create-publisher)
               [:db-conn])})


(def SYS
  (component/start-system (component/map->SystemMap system)))


(com/put! (:publisher SYS) {:queue-name "t3" :payload {:some-number 2}})

;; wait a sec
(component/stop SYS)

```

# How does this work?

There are three core parts:

- Postgres' `LISTEN / NOTIFY` triggers and functions
- an internal `j.u.c ConcurrentLinkedQueue`
- Postgres' `select ... FOR UPDATE SKIP LOCKED`

When the job table is setup, there's a trigger added to send `NOTIFY` whenever a new record is inserted. Then Taskmaster sets up a listener to receive pings
whenever inserts happen. These pings are sent over a `ConcurrentLinkedQueue` to a pool of threads, which
pull all job payloads from the table via a transaction and ensure atomicity via `SELECT ... FOR ... SKIP LOCKED`.

# Dependencies

Taskmaster is built on top of:

- `clojure.tools.logging` for logs (this repo uses logback as the backend)
- `cheshire` serialization for JSONB +
- `next.jdbc`
- `hikari-cp` - connection pool
- `hugsql`  all of the queries

Most of it's wrapped by `nomnom/utility-belt.sql` to smooth out the rough edges




# Roadmap

*Unreleased officialy, check on Clojars*

- [x] :bug: fix a bug where restarting consumers will pick up previously failed jobs
- [ ] non-deleting mode, where ackd jobs stay in the table, this is useful for reprocessing jobs or gathering some extra metrics

- [ ] verify this actually works in production workloads
- [ ] pluggable serialization (avro, simple text, etc) - maybe via middleware?
- [ ] remove HugSQL and `nomnom/utility-belt.sql` to use `next.jdbc` directly


# Acknowledgments

Inspired by:

- https://github.com/QueueClassic/queue_classic
- https://pypi.org/project/pq/
- https://gist.github.com/zarkone/98eb53e4e1f0833b22faa28a1da77ed7


# Copyright and License

Copyright © 2020 Łukasz Korecki All rights reserved. The use and distribution terms for this software are covered by the [Eclipse Public License 1.0](http://opensource.org/licenses/eclipse-1.0.php) which can be found at http://opensource.org/licenses/eclipse-1.0.php  By using this software in any fashion, you are agreeing to be bound by the terms of this license. You must not remove this notice, or any other, from this software.
