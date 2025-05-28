import socket
from multiprocessing import Process, Queue
import numpy as np 
import pickle

def send_to_server(host, port, sub_A, B, result_queue):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            client_socket.connect((host, port))
            data = pickle.dumps((sub_A, B))
            client_socket.sendall(data)
            
            result_data = client_socket.recv(4096)
            result = pickle.loads(result_data)
            result_queue.put((port, result))
    except Exception as e:
        print(f"Error connecting to server {host}:{port} - {e}")
        result_queue.put((port, None))
        
def main():
    
    A = np.array([[1, 0, -1], [4, -1, 2], [-1, 2, 4]])
    B = np.array([[-1, 2, -3], [5, -4, 2], [4, 1, 0]])
    print("Matrix A:\n", A)
    print("Matrix B:\n", B)
    
    num_servers = 2
    sub_A1 = A[:2, :]
    sub_A2 = A[2:, :]
    submatrices = [sub_A1, sub_A2]
    
    servers = [('localhost', 65432), ('localhost', 65433)]
    result_queue = Queue()
    processes = []
    
    for i, (host, port) in enumerate(servers):
        p = Process(target=send_to_server, args=(host, port, submatrices[i], B, result_queue))
        processes.append(p)
        p.start()
        
    for p in processes:
        p.join()
        
    results = [result_queue.get() for _ in range(num_servers)]
    print(results)
    results.sort(key=lambda x: x[0])  

   
    partial_results = []
    for port, res in results:
        if res is not None:
            print(f"Resultado do servidor na porta {port}: shape {res.shape}")
            partial_results.append(res)
        else:
            print(f"Falha ao obter resultado do servidor na porta {port}")
            return  #

   
    if len(partial_results) == num_servers:
        C = np.vstack(partial_results)
        print("Shape da Matriz C:", C.shape)
        print("Resulting Matrix C:")
        print(C)

     
        C_expected = np.dot(A, B)
        print("Expected Resulting Matrix C:")
        print(C_expected)
        print("Matrices are equal:", np.allclose(C, C_expected))
    else:
        print("Não foi possível obter todos os resultados parciais.")
    
if __name__ == "__main__":
    main()