import time
import json
from datetime import datetime
from azure.identity.aio import AzureCliCredential
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
dotenv_path = f"{BASE_DIR}/env.env"
load_dotenv(dotenv_path)
fake_user_file = f"{BASE_DIR}/users_fake_data.json"

async def send_worker(
        producer_client: EventHubProducerClient, 
        batch_size: int
    ):
    start_time = time.perf_counter()
    start_str = datetime.now().strftime('%H:%M:%S')

    with open(fake_user_file, "r", encoding="utf-8") as archivo:
        records = json.load(archivo)
    
    chunk = records[:batch_size] 
    row_count = len(chunk)
    
    event_data_batch = await producer_client.create_batch()
    for record in chunk:
        event_data_batch.add(EventData(json.dumps(record)))
        
    await producer_client.send_batch(event_data_batch)
        
    end_time = time.perf_counter()
    duration = end_time - start_time
        
    print(f"✅ Finished in {duration:.2f}s at {start_str} | {row_count:,} rows sent")


async def command(namespace: str, hub: str, samples: int):

    credential = AzureCliCredential()
    fully_qualified_namespace = f"{namespace}.servicebus.windows.net"

    producer_client = EventHubProducerClient(
        fully_qualified_namespace=fully_qualified_namespace,
        eventhub_name=hub,
        credential=credential
    )
    
    print(f"Starting uploading with {samples} mock samples ...")
    
    async with producer_client:
        await send_worker(producer_client, samples)

    await credential.close()
    
    print(f"Batch uploading to Event Hubs finished")