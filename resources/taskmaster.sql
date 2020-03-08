-- :name create-jobs-table*!  :!
create table if not exists :i:table-name  (
  id bigserial primary key,
  queue_name text not null check(length(queue_name) > 0),
  payload jsonb not null,
  locked_at timestamptz,
  locked_by integer,
  run_count integer default 0,
  created_at timestamptz default now()
);

create index if not exists

--~ (str "idx_" (:table-name params) "_unlocked_jobs")

on :i:table-name (queue_name, id, run_count)

where locked_at is null;

-- :name drop-jobs-table*! :!

drop table if exists :i:table-name;
drop function
--~ (str (:table-name params) "_notify()")
;

drop trigger if exists
--~ (str (:table-name params) "_notify")
on :i:table-name;

-- :name setup-triggers*! :!

create or replace function

--~ (str (:table-name params) "_notify()")

returns trigger as $$ begin
  perform pg_notify(new.queue_name, '');
  return null;
end $$ language plpgsql;

drop trigger if exists

--~ (str (:table-name params) "_notify")

on :i:table-name;

create trigger
--~ (str (:table-name params) "_notify")
  after insert on :i:table-name for each row
  execute procedure

--~ (str (:table-name params) "_notify()")
;


-- :name ping* :? :1

select 1

-- :name unlock-dead-consumers*! :!

update :i:table-name
  set locked_at = null, locked_by = null

where locked_by not in (select pid from pg_stat_activity);

-- :name put*! :<! :1
insert into :i:table-name
  (queue_name, payload) values (:queue-name, :payload)
returning id

-- :name lock*! :<!

with selected_job as (
  select id
  from :i:table-name

where
locked_at is null and
queue_name = :queue-name and
run_count < 1

for no key update skip locked
)

update :i:table-name
set
  locked_at = now(),
  locked_by = pg_backend_pid()
from selected_job

where :sql:table-name-id = selected_job.id

returning *

-- :name unlock*! :<!

update :i:table-name

set locked_at = null, run_count = run_count + 1

where id = :id

-- :name delete-job*! :! :1

delete from :i:table-name

where id = :id

-- :name delete-all*! :!

delete from :i:table-name

where queue_name = :queue-name

-- :name queue-size* :? :1

select count(*) from :i:table-name where queue_name = :queue-name

-- :name listen* :?

listen :i:queue-name
