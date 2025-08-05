from prometheus_client import Counter

PUBLISHED = Counter("mpmq_records_published_total", "Total records published")
CONSUMED  = Counter("mpmq_records_consumed_total",  "Total records delivered to consumers")