import argparse
from multiprocessing import Process
import time

from transfers import *


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--num_payloads', type=int)
    parser.add_argument('--num_workers', type=int)
    parser.add_argument('--payload_size_mb', type=int)
    parser.add_argument('--transfer_type', type=str)
    parser.add_argument('--start_recv_first', type=int) 

    parser.add_argument('--time_1', type=float, default=0)
    # parser.add_argument('--time_2', type=float, default=0)

    

    args = parser.parse_args()
    print("*******Config*******")
    print(args.__dict__)
    
    sender_class, receiver_class = {
        "socket" : (SocketSender, SocketReceiver),
        "mmap" : (MmapSender, MmapReceiver),
        "rpc" : (RPCSender, RPCReceiver),
        "shm" : (SharedMemorySender, SharedMemoryReceiver),
        "momentumx": (MomentumXSender, MomentumXReceiver)

    }[args.transfer_type]
    
    
    sender: Sender = sender_class(num_payloads=args.num_payloads, payload_size_mb=args.payload_size_mb, num_workers=args.num_workers)
    receiver: Receiver = receiver_class(num_payloads=args.num_payloads, payload_size_mb=args.payload_size_mb, num_workers=args.num_workers)

   # Setup processes
    if args.num_workers > 1:
        send_fn = sender.send_parallel
        recv_fn = receiver.recv_parallel
    else:
        send_fn = sender.send
        recv_fn = receiver.recv

    processes = [Process(target=send_fn), Process(target=recv_fn)]

    if args.start_recv_first:
        processes = processes[::-1]
    
    proc_1, proc_2 = processes

    proc_1.start()     
    time.sleep(args.time_1)
    proc_2.start()

    # Run to completion
    proc_1.join()
    proc_2.join()


