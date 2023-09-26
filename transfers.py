from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import mmap
import socket
import sys
import zerorpc
import time
from typing import List
import numpy
from multiprocessing import shared_memory
import momentumx as mx



class Sender:
    def __init__(self, num_payloads: int, payload_size_mb: int, num_workers: int) -> None:
        self.num_payloads = num_payloads
        self.payload_size_mb = payload_size_mb
        self.num_workers = num_workers

        print("Creating Payloads")
        self.payloads: List[bytes] = [numpy.random.bytes(self.payload_size_mb * 1024 * 1024) for _ in range(self.num_payloads)]

    def send(self, worker_idx: int = 0):
        pass

    def send_parallel(self):
        futs = []
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            for worker_idx in range(self.num_workers):
                futs.append(executor.submit(self.send, worker_idx))

    def recv(self):
        pass

    def __str__(self):
        return f"<{self.__class__} num_payloads={self.num_payloads} payload_size_mb={self.payload_size_mb}>"

    def __repr__(self):
        return str(self)


class SocketSender(Sender):
    def __init__(self, num_payloads: int, payload_size_mb: int, num_workers: int) -> None:
        super().__init__(num_payloads, payload_size_mb, num_workers)
        
        self.host = "127.0.0.1"
        self.port = 65432
        self.socket_chunk_size = 4096
 

    def send(self, worker_idx: int = 0):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.host, self.port + worker_idx))
            print("SEND", "connected to port")

            for p_idx, payload in enumerate(self.payloads):

                if p_idx % self.num_workers != worker_idx:
                    continue

                print("SEND", "payload", p_idx, len(payload))

                # Send the payload in chunks of 4096 bytes
                n = len(payload) // self.socket_chunk_size
                for idx, chunk in enumerate([payload[i : i+self.socket_chunk_size] for i in range(0, len(payload), n)]):
                    if idx % 2500 == 0:
                        print(f"Sending chunk {idx} of payload {p_idx} in worker {worker_idx}")
                    s.sendall(chunk)


class MmapSender(Sender):
    def __init__(self, num_payloads: int, payload_size_mb: int, num_workers: int) -> None:
        super().__init__(num_payloads, payload_size_mb, num_workers)
    
    
    def send(self, worker_idx: int = 0):
        block_size = self.payload_size_mb * 1024 * 1024
        
        with mmap.mmap(-1, length=block_size * self.num_payloads, access=mmap.ACCESS_WRITE) as mmap_obj:
        
            for idx, payload in enumerate(self.payloads):
                if idx % self.num_workers != worker_idx:
                    continue
        
                print(f"Writing payload {idx} in worker {worker_idx}")
                mmap_obj[(block_size * idx): block_size * (idx+ 1)] = payload


class RPCSender(Sender):
    def __init__(self, num_payloads: int, payload_size_mb: int, num_workers: int) -> None:
        super().__init__(num_payloads, payload_size_mb, num_workers)
    
    def send(self, worker_idx: int = 0):
        
        class RPC(object):
            def __init__(self, payloads):
                self.payloads = payloads

            def get(self, idx):
                print(f"Sending payload {idx}")
                return self.payloads[idx]
        
        s = zerorpc.Server(RPC(self.payloads))
        print(f"Starting server in worker {worker_idx}")
        s.bind(f"tcp://0.0.0.0:{4242 + worker_idx}")
        s.run()


class SharedMemorySender(Sender):
    def __init__(self, num_payloads: int, payload_size_mb: int, num_workers: int) -> None:
        self.block_size = payload_size_mb * 1024 * 1024
        self.shared_memory = shared_memory.SharedMemory(name="test", create=True, size=num_payloads * self.block_size)

        super().__init__(num_payloads, payload_size_mb, num_workers)
    
    def send(self, worker_idx: int = 0):

        for idx, payload in enumerate(self.payloads):
            if idx % self.num_workers != worker_idx:
                continue

            print(f"Writing payload {idx} in worker {worker_idx}")
            self.shared_memory.buf[(self.block_size * idx): self.block_size * (idx+ 1)] = payload


class MomentumXSender(Sender):
    def __init__(self, num_payloads: int, payload_size_mb: int, num_workers: int) -> None:
        self.block_size = payload_size_mb * 1024 * 1024
        self.num_workers = num_workers
        self.num_payloads = num_payloads

        super().__init__(num_payloads, payload_size_mb, num_workers)


    def send(self, worker_idx: int = 0):

        stream = mx.Producer(
            f'pycon_test_{worker_idx}', 
            buffer_size=self.block_size, 
            buffer_count=self.num_payloads, 
            sync=self.num_workers == 1
        ) 


        min_subscribers = 1

        print("waiting for subscriber(s)")
        while stream.subscriber_count < min_subscribers:
            pass

        print("All expected subscribers are ready")

        # Write the series 0-999 to a consumer 
        for idx, payload in enumerate(self.payloads):
            # if stream.subscriber_count == 0:
                # cancel_event.wait(0.5)

            if idx % self.num_workers != worker_idx:
                continue

            # Note: sending strings directly is possible via the send_string call
            # elif stream.send_string(str(n)):
            #     print(f"Sent: {n}")

            print(f"Sending payload {idx}")
            buffer = stream.next_to_send()
            buffer.write(payload)
            buffer.send()


class Receiver:
    def __init__(self, num_payloads: int, payload_size_mb: int, num_workers: int) -> None:
        self.num_payloads = num_payloads
        self.payload_size_mb = payload_size_mb
        self.num_workers = num_workers

        self.buffer: List[bytes] = []
        self.throughputs = []
        self.payloads = []

    def recv(self, worker_idx: int = 0):
        pass

    def recv_parallel(self):
        futs = []
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            # start = time.time()
            for worker_idx in range(self.num_workers):
                futs.append(executor.submit(self.recv, worker_idx))

            futs = concurrent.futures.wait(futs, return_when="ALL_COMPLETED")
            # end = time.time()
            assert len(futs.not_done) == 0

            print("Avg. Throughput recv_parallel", sum(self.throughputs) / len(self.throughputs), "GBps")


    def __str__(self):
        return f"<{self.__class__} num_payloads={self.num_payloads} payload_size_mb={self.payload_size_mb}>"

    def __repr__(self):
        return str(self)


class SocketReceiver(Receiver):
    def __init__(self, num_payloads: int, payload_size_mb: int, num_workers: int) -> None:
        super().__init__(num_payloads, payload_size_mb, num_workers)
        
        self.host = "127.0.0.1"
        self.port = 65432
        self.socket_chunk_size = 4096

    def recv(self, worker_idx: int = 0):
        start = time.time()
        buffer = []

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port + worker_idx))
            s.listen()
            conn, addr = s.accept()
            print("Connected to", (self.host, self.port + worker_idx))

            counter = 0
            with conn:
                while True:
                    data = conn.recv(self.socket_chunk_size)
                    buffer.append(data)
                    
                    if not data:
                        break
        
                    counter += 1
                    if counter % 2500 == 0:
                        print(f"Received chunk: {counter}")

            end = time.time()
            # self.times.append(end-start)
            # if worker_idx == 0:
            # total = sum(map(lambda x: len(x), self.buffer))
            total = sum(map(lambda x: len(x), buffer))
            throughput = (total / (1024 ** 3)) / (end-start)
            print("Throughput recv", throughput, "GBps")
            self.throughputs.append(throughput)
        

class MmapReceiver(Receiver):
    def __init__(self, num_payloads: int, payload_size_mb: int, num_workers: int) -> None:
        super().__init__(num_payloads, payload_size_mb, num_workers)
    
    
    def recv(self, worker_idx: int = 0):
        buffer = None
        block_size = self.payload_size_mb * 1024 * 1024
        start = time.time()

        with mmap.mmap(-1, length=block_size * self.num_payloads, access=mmap.ACCESS_WRITE) as mmap_obj:
        
            for idx in range(self.num_payloads):
                if idx % self.num_workers != worker_idx:
                    continue
        
                print(f"Reading payload {idx} in worker {worker_idx}")
                buffer = mmap_obj[(block_size * idx): block_size * (idx+ 1)]

            end = time.time()
            # if worker_idx == 0:
            # total = sum(map(lambda x: len(x), buffer))
            total = len(buffer)
            throughput = (total / (1024 ** 3)) / (end - start)
            print("Throughput recv", throughput, "GBps")
            self.throughputs.append(throughput)


class RPCReceiver(Receiver):
    def __init__(self, num_payloads: int, payload_size_mb: int, num_workers: int) -> None:
        super().__init__(num_payloads, payload_size_mb, num_workers)

        
    def recv(self, worker_idx: int = 0):
        buffer = None

        c = zerorpc.Client()
        c.connect(f"tcp://0.0.0.0:{4242 + worker_idx}")

        start = time.time()
        for idx in range(self.num_payloads):
            if idx % self.num_workers != worker_idx:
                continue
            
            print(f"Reading payload {idx}")
            buffer = c.get(idx)

        end = time.time()

        total = len(buffer)
        throughput = (total / (1024 ** 3)) / (end - start)
        print("Throughput recv", throughput, "GBps")
        self.throughputs.append(throughput)


class SharedMemoryReceiver(Receiver):
    def __init__(self, num_payloads: int, payload_size_mb: int, num_workers: int) -> None:
        self.block_size = payload_size_mb * 1024 * 1024
        self.shared_memory = shared_memory.SharedMemory(name="test", size=num_payloads * self.block_size)
        super().__init__(num_payloads, payload_size_mb, num_workers)

        
    def recv(self, worker_idx: int = 0):
        buffer = None
        start = time.time()

        for idx in range(self.num_payloads):
            if idx % self.num_workers != worker_idx:
                continue
        
            print(f"Reading payload {idx} in worker {worker_idx}")
            buffer = bytes(self.shared_memory.buf[(self.block_size * idx): self.block_size * (idx+ 1)])

        end = time.time()
        total = len(buffer)
        throughput = (total / (1024 ** 3)) / (end - start)
        print("Throughput recv", throughput, "GBps")
        self.throughputs.append(throughput)


class MomentumXReceiver(Receiver):
    def __init__(self, num_payloads: int, payload_size_mb: int, num_workers: int) -> None:
        self.block_size = payload_size_mb * 1024 * 1024
        super().__init__(num_payloads, payload_size_mb, num_workers)


    def recv(self, worker_idx: int = 0):
        print(f"Creating consumer in worker: {worker_idx}")
        stream = mx.Consumer(f'pycon_test_{worker_idx}')
        payload: bytes = None
        
        start = time.time()
        while stream.has_next:
            print(f"Readingfrom stream in worker: {worker_idx}")
            buffer = stream.receive()
            
            if buffer is not None:
                payload = buffer.read(buffer.data_size)
                print(f"Recieved Buffer in worker: {worker_idx}")
        
        end = time.time()

        print("RECV", f"{sys.getsizeof(payload)}")
        print("RECV", f"{end - start} secs")
        total = len(payload)
        throughput = (total / (1024 ** 3)) / (end - start)
        print("Throughput recv", throughput, "GBps")
        self.throughputs.append(throughput)

