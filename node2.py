import socket
import threading
import time
import sys
import json
import ast
from queue import Queue
from gemini import prompt_gemini
context = {}
options = {}
ballot = [0, 0, 0]
is_leader = False
leader_name = ""
acceptNum = {}
queue = Queue()
message_queue = Queue()
processed_messages = set()
acceptVal = {}
pending_ack = {}
pending_promise = ""
shutdown = False
semaphore = 1
time_promise = ""
time_accepted = ""
count_promise = 0
count_accepted = 0
pending_accepted = ""
decide_timeout = ""
def send_messages(server_conn):
    while True:
        if not message_queue.empty():
            message = message_queue.get()
            print(f"SENT: {message}")
            server_conn.send(f"{message}\n".encode('utf-8'))
        else:
            time.sleep(0.1)

def check_promise(server_conn, message,extracted_info, ballot_compare):
    global time_promise, count_promise, is_leader
    while time_promise != "":
        time.sleep(0.1)
        if time_promise == "":
            continue
        else:
            timeout = time.time() - time_promise
            if timeout > 10:
                print("Didn't receive 2nd promise")
                if is_leader:
                    count_promise += 1
                    time_promise = ""
                    handle_message = threading.Thread(target=receive_messages, args=(server_conn, message,extracted_info, ballot_compare), daemon=True)
                    handle_message.start()
                else:
                    count_promise -= 1
                    time_promise = ""
                    handle = threading.Thread(target=sendOperationToLeader, args=(queue.queue[0], server_conn,), daemon=True)
                    handle.start()
def check_accept(server_conn, message,extracted_info, ballot_compare):
    global time_accepted, count_accepted
    while time_accepted != "":
        time.sleep(0.1)
        if time_accepted == "":
            continue
        else:
            timeout = time.time() - time_accepted
            if timeout > 10:
                print("Didn't receive 2nd accept")
                if is_leader:
                    count_accepted += 1
                    time_accepted = ""
                    handle_message = threading.Thread(target=receive_messages, args=(server_conn, message,extracted_info, ballot_compare), daemon=True)
                    handle_message.start()
                else:
                    count_accepted -= 1
                    time_accepted = ""
                    handle = threading.Thread(target=sendOperationToLeader, args=(queue.queue[0], server_conn,), daemon=True)
                    handle.start()
def check_decide_timeout(server_conn):
    global decide_timeout, queue, leader_name, semaphore, acceptVal, ballot
    while decide_timeout != "":
        time.sleep(0.1)
        if decide_timeout == "":
            continue
        else:
            timeout = time.time() - decide_timeout
            if timeout > 20:
                decide_timeout = ""
                print("Did not receive decide message, starting an election with accepted value")
                startElection(acceptVal[ballot[2]], server_conn)

def check_accepted_timeout(server_conn):
    global pending_accepted, queue, semaphore
    while pending_accepted != "":
        time.sleep(0.1)
        if pending_accepted == "":
            continue
        else:
            timeout = time.time() - pending_accepted
            if timeout > 10:
                pending_accepted = ""
                if is_leader:
                    print("Didn't receive accepted, I think I'm the leader, re-start leader election")
                    startElection(queue.queue[0], server_conn)
                else:
                    print("Didn't receive accepted, leader has changed, send it to leader")
                    handle = threading.Thread(target=sendOperationToLeader, args=(queue.queue[0], server_conn,), daemon=True)
                    handle.start()

def get_message_from_server(server_conn):
    global shutdown, time_promise, time_accepted, leader_name, count_accepted, count_promise, pending_promise, is_leader
    buffer = ""
    count_promise = 0
    count_accepted = 0
    extracted_info = []
    ballot_compare = []
    while not shutdown:
        try:
            message = server_conn.recv(1024).decode('utf-8')
            if not message:
                break
            buffer += message
            while "\n" in buffer:
                message, buffer = buffer.split("\n", 1)
                if message == "fail":
                    shutdown = True
                    print("node failed, shutting down")
                    server_conn.close()
                    sys.exit(0)
                if "-" in message:
                    original_node_name, to_print = message.split("-", 1)
                print(f"RECEIVED from {original_node_name}: {to_print}")
                if count_accepted == 2:
                    count_accepted = 0
                if "accepted: " in message and "+FAIL" not in message:
                    count_accepted += 1
                    if count_accepted == 1:
                        time_accepted = time.time()
                        listen = threading.Thread(target=check_accept, args=(server_conn, message, extracted_info, ballot_compare))
                        listen.start()
                    if count_accepted == 2:
                        time_accepted = ""
                if count_promise == 0:
                    ballot_compare = []
                    extracted_info = []
                if count_promise == 2:
                    count_promise = 0
                    extracted_info = []
                    ballot_compare = []
                

                if "promise: " in message and "+FAIL" not in message:
                    count_promise += 1
                    pending_promise = ""
                    extracted_info_1 = extractPromiseMessage(message)
                    extracted_info.append(extracted_info_1)
                    if extracted_info_1[3] != "":
                        ballot_compare.append(extracted_info_1[1])
                    if count_promise == 1:
                        time_promise = time.time()
                        listen = threading.Thread(target=check_promise, args=(server_conn, message, extracted_info, ballot_compare))
                        listen.start()
                    if count_promise == 2:
                        time_promise = ""
            handle_message = threading.Thread(target=receive_messages, args=(server_conn, message, extracted_info, ballot_compare), daemon=True)
            handle_message.start()
        except OSError as e:
            if not shutdown:
                print(f"error: {e}")
                return

def update_context(message):
    global context, options, ballot, leader_name, semaphore, is_leader
    arr = message.split("CONTEXT: ")[1].split("&")
    context_recv = ast.literal_eval(arr[0])
    int_context = {int(k): v for k, v in context_recv.items()}
    options_recv = ast.literal_eval(arr[1])
    int_options = {int(k): v for k, v in options_recv.items()}
    ballot_revc = ast.literal_eval(arr[2])
    int_ballot = [int(x) for x in ballot_revc]
    if int_ballot[2] > ballot[2]:
        leader_recv = arr[3]

        context = int_context
        options = int_options
        ballot = int_ballot
        leader_name = leader_recv
        semaphore = 1
        if leader_name != "node2":
            is_leader = False
def receive_messages(server_conn, message, extracted_info, ballot_compare):
    global message_queue, acceptVal, acceptNum, ballot, pending_ack, processed_messages, context, leader_name, semaphore, count_accepted, count_promise, pending_accepted, is_leader, decide_timeout, pending_promise
            
    if "-" in message:
            original_node_name, message = message.split("-", 1)

    if "updateME" in message:
        message_queue.put(f"{original_node_name}-CONTEXT: {context}&{options}&{ballot}&{leader_name}")

    if "changeLeader: " in message:
        l = message.split("changeLeader: ")[1]
        leader_name = l
        is_leader = False
    if "info:" in message:
        info, data = message.split(":",1)
        recieved_data = json.loads(data)
        context = recieved_data.get("context", {})
        ballot = recieved_data.get("ballot", [0,0,0])
        print("updated context and ballot")

    if "leader:" in message:
        leader, leader_data = message.split(":")
        if leader_data != "node2":
            print(f"got leader data: {leader_data}")
            leader_name = leader_data
            message_queue.put(f"{leader_name}-update")
            send_messages(server_conn)

    if "acknowledge" in message:
        msg = queue.get()
        semaphore = 1
        for op_id, op in pending_ack.items():
            if op["message"] in message:
                print("Ack recieved, forgetting op")
                del pending_ack[op_id]
                break

    if "forward: " in message:
        if is_leader:
            to_add = message.split("forward: ")[1]
            if ":" in to_add:
                to_add, original_node_name = to_add.split(":")
            acknowledgeOperation(f"{to_add}", original_node_name, server_conn)
        else:
            message_queue.put(f"{leader_name}-{message}:{original_node_name}")


    if "CONTEXT: " in message:
        pending_promise = ""
        pending_accepted = ""
        handle = threading.Thread(target=update_context, args=(message,))
        handle.start()

    if "prepare:" in message:
        handle = threading.Thread(target=processBallotMessage, args=(message, original_node_name, server_conn), daemon=True)
        handle.start()
    elif "promise:" in message:
        if count_promise == 2:
            if len(ballot_compare) > 0:
                ret_val = compareAcceptBallots(ballot_compare)
                if ret_val == 1:
                    acceptVal[ballot[2]] = extracted_info[0][3]
                    acceptNum[ballot[2]] = extracted_info[0][1]
                    original = extracted_info[0][2]
                    if original != extracted_info[0][3]:
                        queue.queue.insert(0, extracted_info[0][3])
                else:
                    acceptVal[ballot[2]] = extracted_info[1][3]
                    acceptNum[ballot[2]] = extracted_info[1][1]
                    original = extracted_info[1][2]
                    if original != extracted_info[1][3]:
                        queue.queue.insert(0, extracted_info[1][3])
            else:
                acceptVal[ballot[2]] = extracted_info[0][2]
                acceptNum[ballot[2]] = ballot
            sendAccept(acceptVal[ballot[2]], server_conn)
    elif "accept: " in message:
        processAccept(message, original_node_name, server_conn)
            
    elif "accepted: " in message:
        pending_accepted = ""
        info = message.split("accepted: ")[1].split(", ")
        if count_accepted == 2:
            command = info[3] 
            if not queue.empty() and queue.queue[0] == command:
                msg = queue.get()
            send_decide_messages(command, server_conn)
            
    elif "Decide: " in message:
        decide_timeout = ""
        command = message.split("Decide: ")[1]
        process_decide_message(command, server_conn)

    elif "Decided: " in message:
        arr = message.split("Decided: ")[1].split("id: ")[1].split(" ")
        contextid = int(arr[0])
        answer = " ".join(map(str, arr[1:]))
        soln = options.get(contextid, {})
        if original_node_name == "node3":
            soln[3] = answer
        else:
            soln[1] = answer
        options[contextid] = soln

def process_decide_message(message, server_conn):
    global ballot, semaphore
    ballot[2] += 1
    if "create" in message:
        contextid = int(message.split("create ")[1])
        context[contextid] = ""
    if "query" in message:
        contextid = int(message.split("query ")[1].split(" ")[0])
        arr = message.split("query ")[1].split(" ")
        query = " ".join(map(str, arr[1:]))
        context[contextid] += f"Query: {query}"
        to_query = context[contextid] + query + "\nAnswer:"
        answer = prompt_gemini(to_query)
        answer = answer.replace("\n", "")
        opt = options.get(contextid, {})

        opt[2] = answer
        options[contextid] = opt
        message_queue.put(f"node1-Decided: id: {contextid} {answer}")
        message_queue.put(f"node3-Decided: id: {contextid} {answer}")

    if "choose" in message:
        contextid = int(message.split("choose ")[1].split(" ")[0])
        response = int(message.split("choose ")[1].split(" ")[1])
        context[contextid]+= f", Answer: {options[contextid][response]}"
        del options[contextid]
    if is_leader:
        semaphore = 1

def send_decide_messages(message, server_conn):
    print("----PHASE 3: Decide ----")
    message_queue.put(f"node1-Decide: {message}")
    message_queue.put(f"node3-Decide: {message}")
    process_decide_message(message, server_conn)

def processBallotMessage(message, node_name, server_conn):
    global ballot
    global leader_name, is_leader
    nums = message.split("prepare: ")[1].split(", ")
    accept_ballot = [int(nums[0]), int(nums[1]), int(nums[2])]
    if(ballot[2] < accept_ballot[2]):
        message_queue.put(f"{node_name}-updateME")
    print("----PHASE 1b: Promise Phase-----")
    if compareBallots(accept_ballot):
        is_leader = False
        ballot = accept_ballot
        leader_name = node_name
        accepted_ballot = acceptNum.get(ballot[2], [0, 0, 0])
        message_queue.put(f"{leader_name}-promise: {accept_ballot[0]}, {accept_ballot[1]}, {accept_ballot[2]}, {accepted_ballot[0]}, {accepted_ballot[1]}, {accepted_ballot[2]}, {nums[3]}, {acceptVal.get(ballot[2],'')}")
    else:
        print(f"Rejected ballot {accept_ballot}, my ballot: {ballot}")
        if (ballot[2] > accept_ballot[2]):
            message_queue.put(f"{node_name}-CONTEXT: {context}&{options}&{ballot}&{leader_name}")


def compareBallots(acceptor_ballot):
    if(ballot[2] <= acceptor_ballot[2]):
        if(ballot[0] < acceptor_ballot[0]):
            return True
        if(ballot[0] == acceptor_ballot[0]):
            if(ballot[1] <= acceptor_ballot[1]):
                return True
            else:
                return False
    else:
        return False

def compareAcceptBallots(ballots):
    if len(ballots) == 1:
        return 1
    ballot1 = ballots[0]
    ballot2 = ballots[1]
    if ballot1[0] < ballot2[0]:
        return 1
    if ballot1[0] == ballot2[0] and ballot1[1] < ballot2[1]:
        return 1
    return 2

def extractPromiseMessage(message):
    global ballot, is_leader, leader_name
    temp = message.split("promise: ")[1].split(", ")
    ballot_recv = [int(temp[0]), int(temp[1]), int(temp[2])]
    ballot_accept = [int(temp[3]), int(temp[4]), int(temp[5])]
    if ballot_recv == ballot:
        is_leader = True
        leader_name = "node2"
    origMessage = temp[6]
    acceptedVal = temp[7]
    return [ballot_recv, ballot_accept, origMessage, acceptedVal]

def acknowledgeOperation(message, node_name, server_conn):
    queue.put(message)
    message_queue.put(f"{node_name}-acknowledge {message}")
    send_messages(server_conn)

def processAccept(message, node_name, server_conn):
    global ballot
    global acceptNum
    global acceptVal, decide_timeout
    nums = message.split("accept: ")[1].split(", ")
    leader_ballot = [int(nums[0]), int(nums[1]), int(nums[2])]
    print("----PHASE 2b: Accepted Phase-----")
    if leader_ballot == ballot:
        acceptNum[ballot[2]] = leader_ballot
        acceptVal[ballot[2]] = nums[3]
        message_queue.put(f"{leader_name}-accepted: {acceptNum[ballot[2]][0]}, {acceptNum[ballot[2]][1]}, {acceptNum[ballot[2]][2]}, {acceptVal[ballot[2]]}")
        decide_timeout = time.time()
        handle = threading.Thread(target=check_decide_timeout, args=(server_conn,))
        handle.start()
        send_messages(server_conn)
    else:
        if (ballot[2] > leader_ballot[2]):
            message_queue.put(f"{node_name}-CONTEXT: {context}&{options}&{ballot}&{leader_name}")
        if (ballot[2] < leader_ballot[2]):
            message_queue.put(f"{node_name}-updateME")
def sendAccept(message, server_conn):
    global ballot
    global acceptNum
    global acceptVal, pending_accepted

    acceptNum[ballot[2]] = ballot
    acceptVal[ballot[2]] = message
    print("----PHASE 2a: Accept Phase-----")
    message_queue.put(f"node1-accept: {ballot[0]}, {ballot[1]}, {ballot[2]}, {message}")
    message_queue.put(f"node3-accept: {ballot[0]}, {ballot[1]}, {ballot[2]}, {message}")
    pending_accepted = time.time()
    handle = threading.Thread(target=check_accepted_timeout, args=(server_conn,))
    handle.start()
    
    
def check_promise_timeout():
    global pending_promise, semaphore
    while pending_promise != "":
        time.sleep(0.1)
        if pending_promise == "":
            continue
        else:
            timeout = time.time() - pending_promise
            if timeout > 10:
                pending_promise = ""
                print("Didn't receive promise")
                semaphore = 1

def startElection(message, server_conn):
    global ballot, pending_promise

    ballot = [ballot[0]+1, 2, ballot[2]]
    print("----PHASE 1a: Prepare phase----")
    message_queue.put(f"node1-prepare: {ballot[0]}, {ballot[1]}, {ballot[2]}, {message}")
    message_queue.put(f"node3-prepare: {ballot[0]}, {ballot[1]}, {ballot[2]}, {message}")
    pending_promise = time.time()
    threading.Thread(target=check_promise_timeout, args=(), daemon=True).start()

def check_acknowledgment_timeout(op_id, server_conn):
    global pending_ack, semaphore

    wait_time = 10
    while True: 
        try:
            if time.time() - pending_ack[op_id]["timestamp"] > wait_time:
                message = pending_ack[op_id]["message"]
                print("did not receive ack, starting election")
                del pending_ack[op_id]
                # semaphore += 1
                startElection(message,server_conn)
                break
        except KeyError:
            break


def sendOperationToLeader(message, server_conn):
    global pending_ack
    
    if leader_name == "":
        startElection(message, server_conn)
    else:
        op_id = len(pending_ack) + 1
        if op_id in pending_ack: 
            return
        pending_ack[op_id] = {"message": message, "timestamp": time.time()}
        message_queue.put(f"{leader_name}-forward: {message}")
        print(f"FORWARDING operation {op_id} to leader: {leader_name}")
        threading.Thread(target=check_acknowledgment_timeout, args=(op_id, server_conn), daemon=True).start()
        send_messages(server_conn)

def viewall():
    to_print = {}
    for c in context:
        to_print[c] = context[c]
        if c in options:
            for i in range(3):
                to_print[c]  += f"\nOPTION {i+1}: {options[c][i+1]}"
    print(f"context: {to_print}")

def viewSpecificContext(message):
    view, contextId = message.split(" ")
    to_print = {}
    for c in context:
        to_print[c] = context[c]
        if c in options:
            for i in range(3):
                to_add = options[c].get(i+1, "")
                to_print[c]  += f"\nOPTION {i+1}: {to_add}"
    print(f"context {contextId}: {to_print[int(contextId)]}")
        
def listen_queue(server_conn):
    global queue, semaphore
    while True:
        if not queue.empty() and semaphore > 0:
            try:
                message = queue.queue[0]
            except:
                continue
            semaphore -= 1
            if is_leader:
                handle = threading.Thread(target=sendAccept, args=(message, server_conn,), daemon=True)
                handle.start()
            else:
                handle = threading.Thread(target=sendOperationToLeader, args=(message, server_conn,), daemon=True)
                handle.start()
        else:
            continue

def listenInput():
    global queue, shutdown
    while not shutdown:
        try:
            if not shutdown:
                message = input()
                if message.lower().startswith("create") or message.lower().startswith("query") or message.lower().startswith("choose"):
                    queue.put(message)
                elif message.lower().startswith("viewall"):
                    viewall()
                elif message.lower().startswith("view"):
                    viewSpecificContext(message)
        except OSError:
            break

def start_node():
    port_server = 9000
    port_server_connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    port_server_connection.connect(('127.0.0.1', port_server))

    port_server_connection.send("node2".encode('utf-8'))

    send_thread = threading.Thread(target=send_messages, args=(port_server_connection,), daemon=True)
    recv_thread = threading.Thread(target=get_message_from_server, args=(port_server_connection,), daemon=True)
    input_thread = threading.Thread(target=listenInput, daemon=True)
    handle_queue = threading.Thread(target=listen_queue, args=(port_server_connection,), daemon=True)
    handle_queue.start()
    input_thread.start()
    send_thread.start()
    recv_thread.start()
    input_thread.join()
    
if __name__ == '__main__':
    start_node()