from src.pipeline import ingest_app_events, ingest_orders, ingest_provider_exports, ingest_refunds
from config.settings import Settings
from bq.bq_integrations import get_bq_client

client = get_bq_client(Settings())

def main():
    ingest_app_events(Settings(), client, "2024-06-01", "12")
    ingest_orders(Settings(), client, "2024-06-01", "12")
    ingest_provider_exports(Settings(), client, "provider_a", "2024-06-01")
    ingest_refunds(Settings(), client, "provider_a", "2024-06-01")


if __name__ == "__main__":
    main()