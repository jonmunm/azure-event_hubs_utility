import asyncio
import json
from azure.identity.aio import AzureCliCredential
from azure.eventhub.aio import EventHubConsumerClient

async def command(namespace: str, hub: str, consumer_group: str = "$Default"):
    credential = AzureCliCredential()
    fully_qualified_namespace = f"{namespace}.servicebus.windows.net"

    consumer_client = EventHubConsumerClient(
        fully_qualified_namespace=fully_qualified_namespace,
        eventhub_name=hub,
        consumer_group=consumer_group,
        credential=credential
    )
    
    received_count = [0]
    last_event_time = [asyncio.get_event_loop().time()]

    async def on_event(partition_context, event):
        """
        Callback that executes every time an event is received in the Event Hub.
        Displays the data one by one in the console.
        """
        event_body = event.body_as_str(encoding='utf-8')
        received_count[0] += 1
        last_event_time[0] = asyncio.get_event_loop().time() # Actualiza el tiempo del último mensaje recibido
        
        print(f"📥 Event received (Partition: {partition_context.partition_id}, Sequence: {event.sequence_number}) [Total: {received_count[0]}]:")
        
        try:
            parsed_data = json.loads(event_body)
            print(json.dumps(parsed_data, indent=2, ensure_ascii=False))
        except json.JSONDecodeError:
            print(event_body)
            
        print("-" * 50)
        
        await partition_context.update_checkpoint(event)

    print(f"Starting listening on Event Hub '{hub}' (Consumer Group: {consumer_group}) ...")
    print("Reading all available messages. Will stop automatically when finished.\n")
    
    try:
        async with consumer_client:
            # Ejecutamos la recepción como una tarea en segundo plano
            receive_task = asyncio.create_task(
                consumer_client.receive(
                    on_event=on_event,
                    starting_position="-1",
                )
            )

            # Bucle monitor: si pasan 2 segundos sin recibir nuevos mensajes, asumimos que terminó
            while not receive_task.done():
                await asyncio.sleep(0.5)
                if received_count[0] > 0 and (asyncio.get_event_loop().time() - last_event_time[0] > 2.0):
                    print(f"\n✅ All available messages read ({received_count[0]} total). Stopping automatically...")
                    receive_task.cancel()
                    break
            
            try:
                await receive_task
            except asyncio.CancelledError:
                pass

    except KeyboardInterrupt:
        print("\n🛑 Reception stopped by user.")
    finally:
        await credential.close()
        print("Connection closed.")