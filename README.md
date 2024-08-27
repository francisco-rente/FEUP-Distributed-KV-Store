# Distributed KV Store 


## Description

A distributed key-value store developed in the context of the Distributed Systems course at FEUP.

Group members:

1. António Ribeiro up201906761@edu.fe.up.pt
2. Filipe Pinto up201907747@edu.fe.up.pt
3. Diogo Maia  up201904974@edu.fe.up.pt 


## How to Use 

How to compile the code:

- javac -d <diretório> TestClient
- javac -d <diretório> Store

How to run the code:

- java TestClient <node_ap> <operation> [<opnd>]
- java Store <IP_mcast_addr> <IP_mcast_port> <node_id>  <Store_port>
