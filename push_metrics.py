from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
import random

def push_example_metrics():
    registry = CollectorRegistry()

    # Defining a gauge:  our rows ingested per city
    rows_gauge = Gauge(
        'crime_ingested_rows',
        'Number of crime rows ingested per run',
        ['city'],
        registry=registry
    )

    # Fake numbers for now we would plug in df.count()
    dc_rows = random.randint(1000, 2000)
    chi_rows = random.randint(5000, 8000)

    rows_gauge.labels(city='DC').set(dc_rows)
    rows_gauge.labels(city='Chicago').set(chi_rows)

    # Push to our local Pushgateway
    push_to_gateway('localhost:9091', job='crime_pipeline', registry=registry)
    print(f"Pushed metrics: DC={dc_rows}, Chicago={chi_rows}")

if __name__ == "__main__":
    push_example_metrics()
