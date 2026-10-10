# Azure Event Hubs Utility

A high-performance Python utility designed to benchmark, execute data uploads, and stream/read events from Azure Event Hubs.

---

## 🚀 Features
* **Parallel Batching:** Sends multiple record batches concurrently using `asyncio` and random batch sizing for realistic stress testing (`send`).
* **Real-Time Consumption:** Reads and displays events one by one in real-time with automatic stopping upon completion (`receive`).
* **Async Core:** Built on `asyncio` for maximum I/O throughput.

---

## 🛠 Prerequisites
* Python 3.11+
* Azure CLI (for `AzureCliCredential`)
* Active Azure Event Hubs Namespace & Hub

---

## 📋 Usage

### 1. Installation
```bash
pip install -r requirements.txt
```

### 2. Running the Commands

The tool is invoked via `utils.py` using two main commands:

#### A. Send Events in Parallel Batches (`send`)
Sends multiple batches of mock records in parallel to the Event Hub, randomizing the size of each batch and displaying metrics upon completion.

* ```stream```: The target Azure Event Hubs namespace and hub. Format *namespace.hub*.
* ```batches```: Number of concurrent batches to send.

```bash
python utils.py send --stream ehn.eh001 --batches 10
✅ [Batch #1] Finished in 3.51s at 18:13:40 | 9 rows sent
✅ [Batch #2] Finished in 3.63s at 18:13:40 | 9 rows sent
✅ [Batch #3] Finished in 3.77s at 18:13:40 | 14 rows sent
✅ [Batch #4] Finished in 4.00s at 18:13:40 | 10 rows sent
✅ [Batch #5] Finished in 4.21s at 18:13:40 | 11 rows sent
✅ [Batch #6] Finished in 4.41s at 18:13:40 | 6 rows sent
✅ [Batch #7] Finished in 4.62s at 18:13:40 | 13 rows sent
✅ [Batch #8] Finished in 4.75s at 18:13:40 | 6 rows sent
✅ [Batch #9] Finished in 4.89s at 18:13:40 | 13 rows sent
✅ [Batch #10] Finished in 5.03s at 18:13:40 | 8 rows sent
All batch uploads to Event Hubs finished successfully.
```

#### B. Receive and Read Events (`receive`)
Listens to the Event Hub, displays messages one by one, and stops automatically once all available data has been read.

* ```stream```: The target Azure Event Hubs namespace and hub. Format *namespace.hub*.
* ```group```: Consumer group name (optional, defaults to `$Default`).

```bash
python utils.py receive --stream ehn.eh001
Starting listening on Event Hub 'eh001' (Consumer Group: $Default) ...
Reading all available messages. Will stop automatically when finished.

📥 Event received (Partition: 0, Sequence: 0) [Total: 1]:
{
  "id": 800,
  "name": "John Martinez",
  "username": "johmar33",
  "email": "john.martinez@gmail.com",
  "address": {
    "street": "Joseph Light",
    "suite": "Apt. 784",
    "city": "Smithborough",
    "zipcode": "80230-2865",
    "geo": {
      "lat": "-30.8257",
      "lng": "-121.8251"
    }
  },
  "phone": "1-652-207-7621 x41958",
  "website": "martinez14.org",
  "company": {
    "name": "Martinez-Martin",
    "catchPhrase": "Visionary maximizing middleware",
    "bs": "deploy open-source paradigms"
  }
}
...
✅ All available messages read (151 total). Stopping automatically...
Connection closed.
```