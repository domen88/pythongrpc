import os
from concurrent import futures
import pandas as pd
from al import Support
import grpc
import time

import chunk_pb2, chunk_pb2_grpc

CHUNK_SIZE = 1024 * 1024  # 1MB
exit = 0
tmp_file_idx = 0


def get_file_chunks(filename):
    with open(filename, 'rb') as f:
        while True:
            piece = f.read(CHUNK_SIZE);
            if len(piece) == 0:
                return
            yield chunk_pb2.Chunk(buffer=piece)


def save_chunks_to_file(chunks, filename):
    with open(filename, 'wb') as f:
        for chunk in chunks:
            f.write(chunk.buffer)


def decomp(filename):
        df = pd.read_pickle(filename)
        print(df)


class FileClient:
    def __init__(self, address):
        channel = grpc.insecure_channel(address)
        self.stub = chunk_pb2_grpc.FileServerStub(channel)

    def upload(self, in_file_name):
        chunks_generator = get_file_chunks(in_file_name)
        response = self.stub.upload(chunks_generator)
        assert response.length == os.path.getsize(in_file_name)
        return response

    def download(self, target_name, out_file_name):
        response = self.stub.download(chunk_pb2.Request(name=target_name))
        save_chunks_to_file(response, out_file_name)


class FileServer(chunk_pb2_grpc.FileServerServicer):
    def __init__(self):

        class Servicer(chunk_pb2_grpc.FileServerServicer):
            def __init__(self):
                self.tmp_file_name = 'server.zip'

            def upload(self, request_iterator, context):
                global tmp_file_idx
                global exit
                tmp_file_name = 'server_row' + str(tmp_file_idx) + '.zip'
                save_chunks_to_file(request_iterator, tmp_file_name)
                Support.calculate(tmp_file_name)
                exit = 1
                return chunk_pb2.Reply(length=os.path.getsize(tmp_file_name))

            def download(self, request, context):
                if request.name:
                    return get_file_chunks(self.tmp_file_name)

        self.server = grpc.server(futures.ThreadPoolExecutor())
        chunk_pb2_grpc.add_FileServerServicer_to_server(Servicer(), self.server)
        #self.server.wait_for_termination()

    def start(self, port, idx):
        global tmp_file_idx
        global exit
        tmp_file_idx = idx

        self.server.add_insecure_port(f'[::]:{port}')
        self.server.start()
        #self.server.wait_for_termination()

        while True:
            if exit == 1:
                exit = 0
                time.sleep(1)
                self.server.stop(0)
                break