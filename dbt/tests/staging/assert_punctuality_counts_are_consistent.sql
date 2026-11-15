-- Route-months whose train counts contradict each other. Silver rejects these
-- (`cancelled_over_planned`, `late_over_run`), so any row here means a raw row
-- bypassed the Spark job or staging broke the arithmetic. The >15/30/60 min
-- buckets are left out on purpose: AQST counts them on a different basis than
-- trains_late_arrival and silver does not cross-check them.

select
    route_month_id,
    trains_planned,
    trains_cancelled,
    trains_run,
    trains_late_arrival
from {{ ref('stg_sncf__punctuality_monthly') }}
where trains_cancelled > trains_planned
   or trains_run > trains_planned
   or trains_late_arrival > trains_run
