-- :name create-jobs-table*!  :!
CREATE TABLE if NOT EXISTS :i:table-name  (
  id bigserial PRIMARY key,
  queue_name text NOT NULL CHECK(LENGTH(queue_name) > 0),
  payload jsonb NOT NULL,
  locked_at timestamptz,
  locked_by INTEGER,
  run_count INTEGER DEFAULT 0,
  created_at timestamptz DEFAULT now()
);

CREATE index if NOT EXISTS

--~ (str "idx_" (:table-name params) "_unlocked_jobs")

ON :i:table-name (queue_name, id, run_count)

WHERE locked_at IS NULL;

-- :name drop-jobs-table*! :!
DROP TABLE if EXISTS :i:table-name;
DROP FUNCTION
--~ (str (:table-name params) "_notify()")
;

DROP TRIGGER if EXISTS
--~ (str (:table-name params) "_notify")
ON :i:table-name;

-- :name setup-triggers*! :!
CREATE OR replace FUNCTION

--~ (str (:table-name params) "_notify()")

RETURNS TRIGGER AS $$ BEGIN
  perform pg_notify(NEW.queue_name, '');
  RETURN NULL;
END $$ LANGUAGE plpgsql;

DROP TRIGGER if EXISTS

--~ (str (:table-name params) "_notify")

ON :i:table-name;

CREATE TRIGGER
--~ (str (:table-name params) "_notify")
  AFTER INSERT ON :i:table-name FOR EACH ROW
  EXECUTE PROCEDURE

--~ (str (:table-name params) "_notify()")
;


-- :name ping* :? :1
SELECT 1

-- :name unlock-dead-consumers*! :!

UPDATE :i:table-name
  SET locked_at = NULL, locked_by = NULL

WHERE locked_by NOT IN (SELECT pid FROM pg_stat_activity);

-- :name find-jobs* :?

SELECT * FROM :i:table-name

WHERE
  run_count = :run-count
  AND queue_name = :queue-name

-- :name get-by-ids* :?
SELECT * FROM :i:table-name

WHERE
  run_count >= :min-run-count
  AND id IN (:v*:id)

-- :name put*! :<! :1
INSERT INTO :i:table-name
  (queue_name, payload) VALUES (:queue-name, :payload)
RETURNING id

-- :name put-many*! :<! :*

INSERT INTO :i:table-name
(queue_name, payload) VALUES :t*:payloads


-- :name lock*! :<!
WITH selected_job AS (
  SELECT id
    FROM :i:table-name
  WHERE
    locked_at IS NULL
    AND queue_name = :queue-name
    AND run_count = 0

    FOR NO key UPDATE SKIP LOCKED
)

UPDATE :i:table-name
SET
  locked_at = now(),
  locked_by = pg_backend_pid()
FROM selected_job

WHERE :sql:table-name-id = selected_job.id

RETURNING *

-- :name unlock*! :<!

UPDATE :i:table-name

SET locked_at = NULL, run_count = run_count + 1

WHERE id = :id

-- :name delete-jobs*! :! :n

DELETE FROM :i:table-name

WHERE id IN  (:v*:id)

-- :name delete-all*! :!

DELETE FROM :i:table-name

WHERE queue_name = :queue-name

-- :name queue-size* :? :1

SELECT COUNT(*) FROM :i:table-name WHERE queue_name = :queue-name

-- :name queue-stats* :? :*

SELECT queue_name, COUNT(run_count), run_count > 0 AS is_failed FROM :i:table-name
GROUP BY queue_name, run_count

-- :name listen* :?

LISTEN :i:queue-name
