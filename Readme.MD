# Azure Event Hubs Utility

A high-performance Python utility designed to benchmark, execute data uploads, and stream/read events from Azure Event Hubs.

---

## 🚀 Features
* **Smart Sampling:** Randomly selects a subset of rows from a fake data file for stress testing (`send`).
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

#### A. Send Events in Batches (`send`)
Sends a batch of mock records from your local JSON file to the Event Hub.

* ```stream```: The target Azure Event Hubs namespace and hub. Format *namespace.hub*.
* ```samples```: Number of random rows to pick from the fake data file.

```bash
python utils.py send --stream ehns.hub1 --samples 10 
```

#### B. Receive and Read Events (`receive`)
Listens to the Event Hub, displays messages one by one, and stops automatically once all available data has been read.

* ```stream```: The target Azure Event Hubs namespace and hub. Format *namespace.hub*.

```bash
python utils.py receive --stream ehns.hub1
```