# Distributed LLM Service

# Overview

This is a fault-tolerant distributed large language model service consisting of 3 servers/nodes which will each query the Gemini API for each request. The user can choose the best answers to the query to handle hallucinations. The system is resilient by reaching consensus through Multi-Paxos.

# Architecture

We have 3 nodes and 1 server which acts as the network connecting to all 3 nodes.

# How to Use
1. Clone this repo
2. Make a .env file with your Gemini API Key
3. Run each node and server
4. Input commands to use

# User Commands
In each node, the user can type in 3 different types of commands (replace the values inside the brackets)
1. create {context number}
example:
```
create 1
```

2. query {context number} {question to gemini}
example:
```
query 1 write me a haiku
```

3. view {context number} (This shows you the queries and answers to the queries for a specific context number)
ex:
```
view 1
```

4. viewall (This shows you all the queries and answers to the queries all the contexts created)
ex:
```
viewall
```

5. choose (This allows you to choose which node's answer you prefer for the query you inputted). For example the example below shows that I prefer node 2's response to the query I asked in context 1.
ex:
```
choose 1 2
```

# To Simulate Crashes and Network Failures
Use the following commands as inputs to the server file:
1. failLink {node_number1} {node_number2}
This command simulates a network failure between node_number1 and node_number2
ex:
```
failLink 1 2
```

2. failNode {node_number}
This command simulates the crash of the node specified. To restart, run the file of the node again.
ex:
```
failNode 1
```
