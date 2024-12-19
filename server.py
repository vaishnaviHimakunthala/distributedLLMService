import socket
import threading
import time
node_connections = {}
failed_nodes = {}
is_fail = False 
network_failures = {(1, 2): False, (1, 3): False, (2, 3): False}


def extract_and_check_failure(node_str1, node_str2):
    node1 = int(node_str1.split("node")[1])
    node2 = int(node_str2.split("node")[1])
    connection = (node1, node2)
    if node2 < node1:
        connection = (node2, node1)
    return network_failures[connection]

def process_message(message, node_name):
    time.sleep(3)
    destination, payload = message.split("-", 1)
    if destination in node_connections:
        if "promise" in message or "accepted" in message:
            if is_fail:
                if not extract_and_check_failure(destination, node_name):
                    node_connections[destination].send(f"{node_name}-{payload}\n".encode('utf-8'))
            else:
                if not extract_and_check_failure(destination, node_name):
                    node_connections[destination].send(f"{node_name}-{payload}\n".encode('utf-8'))
        elif "-" in message:
            if not extract_and_check_failure(destination, node_name):
                node_connections[destination].send(f"{node_name}-{payload}\n".encode('utf-8'))
    else:
        print(f"destinaton {destination} not found in node conn")

def handle_node(node_name,connection):
    global leader_node
    buffer = ""
    while True:
        if failed_nodes.get(node_name, False):
            break
        try:
            message = connection.recv(1024).decode('utf-8')
            if not message:
                break
            buffer += message

            while "\n" in buffer:
                message, buffer = buffer.split("\n", 1) 

                if "leaderElected" in message:
                    continue
                handle_send = threading.Thread(target=process_message, args=(message, node_name))
                handle_send.start()
        except OSError as e:
            if failed_nodes.get(node_name, False):
                break
            else:
                print(f"OS error for {node_name}: {e}")
            break


def failNode(message):
    global is_fail, node_connections, failed_nodes
    
    fail, message = message.split()
    node_name = "node" + message
    is_fail = True

    if node_name in node_connections:
        failed_nodes[node_name] = True
        node_conn = node_connections[node_name]
        node_conn.send("fail\n".encode('utf-8'))
        node_conn.close()
        del node_connections[node_name]
        print("Node as been deleted")
    else:
        print(f"node {node_name} not found")

def failLink(node1, node2):
    global network_failures
    connection = (node1, node2)
    if node2 < node1:
        connection = (node2, node1)
    network_failures[connection] = True
    print(f"Failed link between {node1} and {node2}")

def fixLink(node1, node2):
    global network_failures
    connection = (node1, node2)
    if node2 < node1:
        connection = (node2, node1)
    network_failures[connection] = False
    print(f"Fixed link between {node1} and {node2}")

def listenInput():
    while True:
        terminal_message = input()
        if terminal_message.lower().startswith("faillink"):
            node1 = int(terminal_message.lower().split("faillink ")[1].split(" ")[0])
            node2 = int(terminal_message.lower().split("faillink ")[1].split(" ")[1])
            failLink(node1, node2)
        elif terminal_message.lower().startswith("fixlink"):
            node1 = int(terminal_message.lower().split("fixlink ")[1].split(" ")[0])
            node2 = int(terminal_message.lower().split("fixlink ")[1].split(" ")[1])
            fixLink(node1, node2)
        elif terminal_message.lower().startswith("failnode"):
            failNode(terminal_message)

def start_server():
    global failed_nodes, is_fail

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('127.0.0.1', 9000))  # Server listening on port 9000
    server_socket.listen(3)

    input_thread = threading.Thread(target=listenInput)
    input_thread.start()
    
    while True:
        node_conn, addr = server_socket.accept()
        
        node_name = node_conn.recv(1024).decode('utf-8')

        if node_name in failed_nodes:
            del failed_nodes[node_name]

            if not failed_nodes:
                is_fail = False
        node_connections[node_name] = node_conn
        threading.Thread(target=handle_node, args=(node_name,node_conn), daemon=True).start()

  
if __name__ == '__main__':
    start_server()