import socket
import numpy as np
import pickle


def start_server(host='localhost', port=65432):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((host, port))
        server_socket.listen()
        print(f"Server started at {host}:{port}")
        conn, addr = server_socket.accept()
        with conn:
            print(f'Connected by {addr}')
            
            data = conn.recv(4096)
            sub_A, B = pickle.loads(data)
            result = np.dot(sub_A, B)
            conn.sendall(pickle.dumps(result))
            

if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 65432
    start_server(port=port)
    