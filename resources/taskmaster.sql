-- :name create-jobs-table*!  :!
CREATE TABLE IF NOT EXISTS :i:table-name  (
  id bigserial PRIMARY KEY,
  queue_name text NOT NULL CHECK(length(queue_name) > 0),
  payload jsonb NOT NULL,
  locked_at timestamptz,
  locked_by integer,
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_taskmaster_unlocked_jobs on :i:table-name (queue_name, id) WHERE locked_at IS NULL;

-- :name setup-triggers*! :!

CREATE OR REPLACE FUNCTION taskmaster_jobs_notify() RETURNS TRIGGER AS $$ BEGIN
  perform pg_notify(new.queue_name, ''); RETURN NULL;
END $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS taskmaster_jobs_notify ON :i:table-name;

CREATE TRIGGER taskmaster_jobs_notify
  AFTER INSERT ON :i:table-name FOR EACH ROW
  EXECUTE PROCEDURE taskmaster_jobs_notify();


-- :name ping* :? :1

select 1


-- :name unlock-dead-workers*! :!

UPDATE :i:table-name
  SET locked_at = NULL, locked_by = NULL

WHERE locked_by NOT IN (SELECT pid FROM pg_stat_activity);


-- :name put*! :<!
INSERT INTO :i:table-name
  (queue_name, payload) VALUES (:queue-name, :payload) RETURNING id


-- :name lock*! :<!

WITH selected_job AS (
  SELECT id
  FROM :i:table-name

WHERE
locked_at IS NULL AND
queue_name = :queue-name

FOR NO KEY UPDATE SKIP LOCKED
)

UPDATE :i:table-name
SET
  locked_at = now(),
  locked_by = pg_backend_pid()
FROM selected_job

WHERE :sql:table-name-id = selected_job.id

RETURNING *


-- :name unlock*! :<!

update :i:table-name set locked_at = NULL where id = :id

-- :name delete-job*! :!

delete from :i:table-name where id = :id

-- :name delete-all*! :!

delete from :i:table-name where queue_name = :queue-name


-- :name queue-size* :1

select count(*) from :i:table-name where queue_name = :queue-name

-- :name listen* :<!

LISTEN :i:queue-name
