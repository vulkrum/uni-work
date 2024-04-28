import sys
import threading
import time
from socket import *

# Constants
UDP_LOCAL = "127.0.0.1"
TCP_LOCAL = "127.0.0.1"
PORT_OFFSET = 50000
SLEEP_TIME = 10

class Peer:
    def __init__(self):
        self.id = int(sys.argv[1])
        self.succ1 = int(sys.argv[2])
        self.succ2 = int(sys.argv[3])
        self.pred1 = None
        self.pred2 = None
        self.ack = 1
        self.seq = 1
        self.port = PORT_OFFSET + int(sys.argv[1])
        self.quit_flag = False

# Globals
my_peer = Peer()

class SendThread(threading.Thread):
    def __init__(self):
        super(SendThread, self).__init__()

    def run(self):
        global my_peer

        # create UDP socket
        s = socket(AF_INET, SOCK_DGRAM)

        while not my_peer.quit_flag:
            succ_port1 = PORT_OFFSET + my_peer.succ1
            succ_port2 = PORT_OFFSET + my_peer.succ2
            msg1 = "PING " + str(my_peer.id) + " " + str(my_peer.succ1)
            msg2 = "PING " + str(my_peer.id) + " " + str(my_peer.succ2)
            msg_info = "INIT_PRED " + str(my_peer.id) + " " + str(my_peer.succ1) + " " + str(my_peer.succ2)

            s.sendto(msg1.encode("utf-8"), (UDP_LOCAL, succ_port1))
            s.sendto(msg2.encode("utf-8"), (UDP_LOCAL, succ_port2))
            s.sendto(msg_info.encode("utf-8"), (UDP_LOCAL, succ_port1))
            s.sendto(msg_info.encode("utf-8"), (UDP_LOCAL, succ_port2))

            if(int(my_peer.seq) < int(my_peer.ack) + 5):
                msg_check = "CHECK_ALIVE " + str(my_peer.id) + " " + str(my_peer.seq)
                s.sendto(msg_check.encode("utf-8"), (UDP_LOCAL, PORT_OFFSET + int(my_peer.succ1)))
                my_peer.seq += 1
            else:
                print("Peer " + str(my_peer.succ1) + " is no longer alive.")
                dead_peer = my_peer.succ1
                my_peer.succ1 = my_peer.succ2
                print("My first successor is now peer " + str(my_peer.succ1))
                s_fix = socket(AF_INET, SOCK_STREAM)
                dst_addr = (TCP_LOCAL, PORT_OFFSET + my_peer.succ1)
                s_fix.connect(dst_addr)
                msg = "FIX_KILL " + str(my_peer.id) + " " + str(my_peer.pred1) + " " + str(dead_peer)
                s_fix.send(msg.encode("utf-8"))
                s_fix.close()

                my_peer.seq = 1
                my_peer.ack = 1

            # Sleep to give me time to type console input
            time.sleep(SLEEP_TIME)

class ReceiveThread(threading.Thread):
    def __init__(self):
        super(ReceiveThread, self).__init__()

    def run(self):
        global my_peer

        s = socket(AF_INET, SOCK_DGRAM)
        my_addr = (UDP_LOCAL, my_peer.port)
        s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        s.bind(my_addr)

        while True:
            msg, addr = s.recvfrom(1024)
            msg.decode("utf-8")
            msg_data = msg.split(' ')

            if msg_data[0] == 'PING':
                ping_from = msg_data[1]
                ping_to = msg_data[2]
                print("A ping request message was received from Peer " + str(ping_from))
                msg_reply = "PING_RECEIVE " + str(ping_to) + " " + str(ping_from)
                s.sendto(msg_reply.encode("utf-8"), (UDP_LOCAL, PORT_OFFSET + int(ping_from)))

            if msg_data[0] == 'CHECK_ALIVE':
                seq_num = msg_data[2]
                msg_reply = "ALIVE " + str(my_peer.id) + " " + str(seq_num)
                s.sendto(msg_reply.encode("utf-8"), (UDP_LOCAL, PORT_OFFSET + int(msg_data[1])))

            if msg_data[0] == 'ALIVE':
                seq_num = msg_data[2]
                my_peer.ack = int(seq_num)

            if msg_data[0] == 'PING_RECEIVE':
                ping_from = msg_data[1]
                print("A ping response message was received from Peer " + str(ping_from))

            if msg_data[0] == 'INIT_PRED' and (my_peer.pred1 == None or my_peer.pred2 == None):
                if my_peer.id == int(msg_data[2]) and my_peer.pred1 == None:
                    my_peer.pred1 = int(msg_data[1])
                if my_peer.id == int(msg_data[3]) and my_peer.pred2 == None:
                    my_peer.pred2 = int(msg_data[1])
        s.close()

class RequestClientThread(threading.Thread):
    def __init__(self):
        super(RequestClientThread, self).__init__()

    def run(self):
        global my_peer
        while True:
            str_in = input()
            str_data = str_in.split(' ')

            if str_data[0] == 'request':
                fn = int(str_data[1])
                hash_value = fn % 256
                s = socket(AF_INET, SOCK_STREAM)
                s.connect((UDP_LOCAL, PORT_OFFSET + int(my_peer.succ1)))
                if (hash_value >= my_peer.id and hash_value <= my_peer.succ1):
                    msg = "FILE_IN " + str(fn) + " " + str(my_peer.succ1) + " " + str(my_peer.id)
                elif (hash_value > my_peer.succ1):
                    msg = "FILE_NOT_IN " + str(fn) + " " + str(my_peer.succ1) + " " + str(my_peer.id)
                print("File request message for " + str(fn) + " has been sent to my successor.")
                s.send(msg.encode("utf-8"))
                s.close()

            if str_data[0] == 'quit':
                my_peer.quit_flag = True
                msg = "QUIT " + str(my_peer.id) + " " + str(my_peer.succ1) + " " + str(my_peer.succ2) + " " \
                        + str(my_peer.pred1) + " " + str(my_peer.pred2)

                addr_arr = [
                            (TCP_LOCAL, my_peer.succ1 + PORT_OFFSET),
                            (TCP_LOCAL, my_peer.succ2 + PORT_OFFSET),
                            (TCP_LOCAL, my_peer.pred1 + PORT_OFFSET),
                            (TCP_LOCAL, my_peer.pred2 + PORT_OFFSET,)
                            ]

                for addr in addr_arr:
                    s = socket(AF_INET, SOCK_STREAM)
                    s.connect(addr)
                    s.send(msg.encode("utf-8"))
                    s.close()
                
            break

class RequestServerThread(threading.Thread):
    def __init__(self):
        super(RequestServerThread, self).__init__()

    def run(self):
        global my_peer
        s = socket(AF_INET, SOCK_STREAM)
        l_addr = (TCP_LOCAL, my_peer.port)
        s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        s.bind(l_addr)
        s.listen(1)
        while True:
            try:
                conn_sock, addr = s.accept()
                msg = conn_sock.recv(1024)
                msg_data = msg.split(' ')

                if msg_data[0] == 'FILE_IN':
                    fn = int(msg_data[1])
                    print("File " + str(fn) + " is here.")
                    msg_recvr = msg_data[-1]
                    s_reply = socket(AF_INET, SOCK_STREAM)
                    reply_addr = (TCP_LOCAL, PORT_OFFSET + int(msg_recvr))
                    s_reply.connect(reply_addr)
                    msg_reply = "FILE_REPLY " + str(fn) + " " + str(my_peer.id)
                    s_reply.send(msg_reply.encode("utf-8"))
                    print("A response message, destined for peer " + str(msg_recvr) + ", has been sent.")
                    s_reply.close()

                if msg_data[0] == 'FILE_NOT_IN':
                    fn = int(msg_data[1])
                    hash_value = fn % 256
                    print("File " + str(fn) + " is not stored here.")
                    fwd_addr = (TCP_LOCAL, PORT_OFFSET + my_peer.succ1)
                    s_fwd = socket(AF_INET, SOCK_STREAM)
                    s_fwd.connect(fwd_addr)

                    if (hash_value >= my_peer.id and hash_value <= my_peer.succ1) or \
                        (hash_value > my_peer.id and hash_value > my_peer.succ1 and my_peer.id > my_peer.succ1):
                        msg = "FILE_IN " + str(fn) + " " + str(my_peer.succ1) + " " + str(msg_data[-1])
                    elif (hash_value > my_peer.succ1):
                        msg = "FILE_NOT_IN " + str(fn) + " " + str(my_peer.succ1) + " " + str(msg_data[-1])
                    s_fwd.send(msg.encode("utf-8"))
                    print("File request message has been forwarded to my successor.")
                    s_fwd.close()

                if msg_data[0] == 'FILE_REPLY':
                    fn = msg_data[1]
                    fn_from = msg_data[-1]
                    print("Received a response message from peer " + str(fn_from) + ", which has the file " + str(fn))

                if msg_data[0] == 'QUIT':
                    id = int(msg_data[1])
                    succ1 = int(msg_data[2])
                    succ2 = int(msg_data[3])
                    pred1 = int(msg_data[4])
                    pred2 = int(msg_data[5])

                    if my_peer.id == succ1:
                        my_peer.pred1 = pred1
                        my_peer.pred2 = pred2

                    if my_peer.id == succ2:
                        my_peer.pred2 = pred1

                    if my_peer.id == pred1:
                        print("Peer " + str(id) + " will depart from the network.")
                        my_peer.succ1 = succ1
                        my_peer.succ2 = succ2
                        print("My first successor is now peer " + str(succ1))
                        print("My second successor is now peer " + str(succ2))

                    if my_peer.id == pred2:
                        print("Peer " + str(id) + " will depart from the network.")
                        my_peer.succ2 = succ1
                        print("My first successor is now peer " + str(pred1))
                        print("My second successor is now peer " + str(succ1))

                if msg_data[0] == 'FIX_KILL':
                    my_peer.pred1 = int(msg_data[1])
                    my_peer.pred2 = int(msg_data[2])
                    s_reply = socket(AF_INET, SOCK_STREAM)
                    s_reply.connect((TCP_LOCAL, PORT_OFFSET + int(my_peer.pred1)))
                    msg = "SUCC " + str(my_peer.succ1) + " " + str(msg_data[3])
                    s_reply.send(msg.encode("utf-8"))
                    s_reply.close()

                if msg_data[0] == 'SUCC':
                    my_peer.succ2 = int(msg_data[1])
                    print("My second successor is now peer " + str(msg_data[1]))
                    s_pred = socket(AF_INET, SOCK_STREAM)
                    s_pred.connect((TCP_LOCAL, PORT_OFFSET + int(my_peer.pred1)))
                    msg = "PRED " + str(my_peer.id) + " " + str(my_peer.succ1) + " " + str(msg_data[2])
                    s_pred.send(msg.encode("utf-8"))
                    s_pred.close()

                if msg_data[0] == 'PRED':
                    print("Peer " + str(msg_data[3]) + " is no longer alive.")
                    my_peer.succ2 = int(msg_data[2])
                    print("My first successor is now peer " + str(my_peer.succ1))
                    print("My second successor is now peer " + str(my_peer.succ2))
            except KeyboardInterrupt:
                exit()

if __name__ == "__main__":
    thread1 = SendThread()
    thread1.daemon = True
    thread1.start()
    thread2 = ReceiveThread()
    thread2.daemon = True
    thread2.start()
    thread3 = RequestClientThread()
    thread3.daemon = True
    thread3.start()
    thread4 = RequestServerThread()
    thread4.daemon = True
    thread4.start()
    #thread1.join()
    #thread2.join()
    #thread3.join()
    #thread4.join()
    while True:
        time.sleep(1)
