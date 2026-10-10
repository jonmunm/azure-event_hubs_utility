import time
import json
import random
import asyncio
from datetime import datetime
from azure.identity.aio import AzureCliCredential
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
fake_user_file = f"{BASE_DIR}/users_fake_data.json"

async def send_worker(
        producer_client: EventHubProducerClient, 
        batch_id: int,
        all_records: list
    ):
    """
    Asynchronously sends a single batch of randomly selected records to an Azure Event Hub.

    Args:
        producer_client (EventHubProducerClient): The initialized Event Hub producer client.
        batch_id (int): Identifier number for the current batch.
        all_records (list): The complete list of source records loaded from the mock file.
    """
    start_time = time.perf_counter()
    start_str = datetime.now().strftime('%H:%M:%S')

    # Define a random batch size (e.g., between 5 and 15 records per batch)
    # Ensuring we do not exceed the total records available in the file
    max_limit = min(15, len(all_records))
    min_limit = min(5, max_limit)
    batch_size = random.randint(min_limit, max_limit) if max_limit >= min_limit else len(all_records)
    
    # Select a random subset of records for this batch
    chunk = random.sample(all_records, batch_size)
    row_count = len(chunk)
    
    event_data_batch = await producer_client.create_batch()
    for record in chunk:
        event_data_batch.add(EventData(json.dumps(record)))
        
    await producer_client.send_batch(event_data_batch)
        
    end_time = time.perf_counter()
    duration = end_time - start_time
        
    print(f"✅ [Batch #{batch_id}] Finished in {duration:.2f}s at {start_str} | {row_count:,} rows sent")

async def command(namespace: str, hub: str, batches: int):
    """
    Main command orchestrator to upload multiple batches of mock data in parallel to an Event Hub.

    Args:
        namespace (str): The Azure Service Bus / Event Hubs namespace.
        hub (str): The specific Event Hub name.
        batches (int): Total number of concurrent batches to send.
    """
    credential = AzureCliCredential()
    fully_qualified_namespace = f"{namespace}.servicebus.windows.net"

    producer_client = EventHubProducerClient(
        fully_qualified_namespace=fully_qualified_namespace,
        eventhub_name=hub,
        credential=credential
    )
    
    # Load base records only once
    with open(fake_user_file, "r", encoding="utf-8") as archivo:
        records = json.load(archivo)

    print(f"Starting uploading {batches} batches in parallel to Event Hub '{hub}'...")
    
    async with producer_client:
        # Create concurrent tasks for each requested batch
        tasks = [
            send_worker(producer_client, i + 1, records) 
            for i in range(batches)
        ]
        # Execute all batches in parallel
        await asyncio.gather(*tasks)

    await credential.close()
    
    print(f"All batch uploads to Event Hubs finished successfully.")