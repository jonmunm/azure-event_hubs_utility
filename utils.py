import argparse
import asyncio
from sender import command as send 
from receiver import command as receive

def main():   
    parser = argparse.ArgumentParser(description="CLI for Event Hubs batch uploading and receiving")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # "Send" comand
    parser_upload = subparsers.add_parser('send', help="Send events in batches to Event Hubs")
    parser_upload.add_argument('--stream', required=True, type=str, help="Event Hub Namespace & Hub. Format namespace.hub")
    parser_upload.add_argument('--samples', required=True, type=int, default=5, help="Samples quantity")

    # "Receive" command
    parser_receive = subparsers.add_parser('receive', help="Receive and read events from Event Hubs one by one")
    parser_receive.add_argument('--stream', required=True, type=str, help="Event Hub Namespace & Hub. Format namespace.hub")
    parser_receive.add_argument('--group', type=str, default="$Default", help="Consumer group name (default: $Default)")

    args = parser.parse_args()
    parts = args.stream.split(".")
    namespace = parts[0]
    hub = parts[1]
    
    if args.command == 'send':
        asyncio.run(send(
            namespace=namespace,
            hub=hub,
            samples=args.samples
        ))
    elif args.command == 'receive':
        asyncio.run(receive(
            namespace=namespace,
            hub=hub,
            consumer_group=args.group
        ))
    else:
        parser.print_help() 

if __name__ == "__main__":
    main()