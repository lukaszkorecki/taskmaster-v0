# Taskmaster

<img src="https://img.cinemablend.com/filter:scale/quill/b/3/6/2/9/9/b36299d3e49f972d430cae647b5bec83ad70eae8.jpg?mw=600" align="right" width=250 >

Postgres based background processing worker system thing.

Built on top of `next.jdbc`, `hikari-cp`, `hugsql` and `clojure.tools.logging`.

:warning: Definitely not usable yet, see `dev-resources/taskmaster/` directory for samples and examples

Recommended environment:

- Linux
- Postgres 11
- JDK 11
- Clojure 1.10.1

But will probably work with Postgres 9.6, Clojure 1.9 and JDK 8.


# Usage


## Database setup

You'll need a Postgres server, and a databsae created. Then you can create the necessary jobs table structure:

```clojure
(taskmaster.operation/create-jobs-table! jdbc-connection)

```

:warning: This has to run before you start any consumers, as they will attempt to fetch any unprocessed jobs while starting!

Now you can define your consumer, and a handler function which will process each job. If processing is done, return `:taskmaster.operation/ack` qualified symbol. If not, return `:taskmaster.operation/reject`.

That will *keep the failed job data in the jobs table* so that you can:

- requeue it by setting `run_out` column to 0
- implement a garbage collector and delete them some other time



```clojure

(defn handler [{:keys [payload]}]
  (if (do/some-other-work payload)
    :taskmaster.operation/ack
    :taskmaster.operation/reject))
```

Let's define a consumer:

```clojure
(def consumer
  (taskmaster.queue/start! jdbc-conn {:queue-name "do_work_yo"
                                      :handler handler
                                      :concurrency 3}))


```

Consumer will spin up 1 listener thread to be notified about new reocrds being inserted matching the queue name and 3 threads to process these jobs, in parallel, one at a time.

Now let's queue up some jobs:


```clojure

(taskmaster.queue/put! jdbc-conn {:queue-name "do_work_yo" :payload {:send-email "test@example.com"}})
```

By default, job payloads are stored as JSON and Taskmaster is setup to serialize/deserialize it using Cheshire.

## Next steps

That's it, you can now add other fun things like:

- use the [middleware pattern](http://clojure-doc.org/articles/cookbooks/middleware.html) to add instrumentation and error reporting
- spin up multiple consumers to consume from many "queues"
- add some sort of schema validation when pushing/pulling data off the queue


## Component

Recommended way is to use a [Component](https://github.com/stuartsierra/component) approach, but it's not stricly necessary:


```clojure
(require '[taskmaster.dev.connection :as c]
         '[taskmaster.component :as com]
         '[clojure.tools.logging :as log]
         '[com.stuartsierra.component :as component])


(def qs (atom []))


;; `component` is the whole consumer component here - so you have access to its' dependencies
(defn handler [{:keys [id queue-name payload component] :as job}]
  (log/infof "got-job t=%s q=%s %s" component queue-name payload)
  (swap! qs conj id)
  (log/info (count (set @qs)))
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

(component/stop SYS)

```

# How does this work?

There are three core parts:

- Postgres' `LISTEN / NOTIFY` triggers and functions
- an internal `j.u.c ConcurrentLinkedQueue`
- Postgres' `select ... FOR UPDATE SKIP LOCKED`

When the job table is setup, there's a trigger added to send `NOTIFY` whenever a new record is inserted. Then Taskmaster sets up a listener to receive pings whenever inserts happen. These pings are sent over a `ConcurrentLinkedQueue` to a pool of threads, which pull all job payloads from the table via a transaction and ensure atomicity via `SELECT ... FOR ... SKIP LOCKED`.


# Roadmap

- [ ] non-deleting mode, where ackd jobs stay in the table, this is useful for reprocessing jobs or gathering some extra metrics
- [ ] verify this actually works in production workloads
- [ ] shed dependencies - atm it pulls in a lot of stuff from [EnjoyHQ's open source projects](https://github.com/nomnom-insights/) - this will make Component dependency truly optional

# Acknowledgments

Inspired by:

- https://github.com/QueueClassic/queue_classic
- https://pypi.org/project/pq/
- https://gist.github.com/zarkone/98eb53e4e1f0833b22faa28a1da77ed7


# Copyright and License

Copyright © 2020 Łukasz Korecki All rights reserved. The use and distribution terms for this software are covered by the [Eclipse Public License 1.0](http://opensource.org/licenses/eclipse-1.0.php) which can be found at http://opensource.org/licenses/eclipse-1.0.php  By using this software in any fashion, you are agreeing to be bound by the terms of this license. You must not remove this notice, or any other, from this software.
