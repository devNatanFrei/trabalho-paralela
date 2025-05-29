import socket
import pickle
import numpy as np
import argparse
from threading import Thread
from queue import Queue

def send_to_server(host, port, sub_A, B, result_queue):
    
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.settimeout(10) 
        client_socket.connect((host, int(port)))
        
       
        data = pickle.dumps((sub_A, B))
        client_socket.sendall(data)
        
       
        result_data = client_socket.recv(4096)
        result = pickle.loads(result_data)
        result_queue.put((host, port, result))
        
    except (socket.timeout, ConnectionError) as e:
        print(f"Erro ao conectar ao servidor {host}:{port}: {e}")
        result_queue.put((host, port, None))
    finally:
        if 'client_socket' in locals() and client_socket: 
            client_socket.close()

def main():
    
    parser = argparse.ArgumentParser(description="Cliente para multiplicação de matrizes distribuída.")
    
    parser.add_argument('servers', nargs='+', help="Lista de servidores no formato host:port")
    args = parser.parse_args()


    servers = [tuple(s.split(':')) for s in args.servers]
    num_servers = len(servers)

  
    A = np.array([[1, 0, -1], [4, -1, 2], [-1, 2, 4]])
    B = np.array([[-1, 2, -3], [5, -4, 2], [4, 1, 0]])

  
    if A.shape[1] != B.shape[0]:
        print("Erro: As matrizes A e B são incompatíveis para multiplicação.")
        print(f"Dimensões: A={A.shape}, B={B.shape}")
        return

    
    submatrices = np.array_split(A, num_servers, axis=0)

    
    socket.setdefaulttimeout(10)

    
    result_queue = Queue()

   
    threads = []
    for i, (host, port) in enumerate(servers):
        t = Thread(target=send_to_server, args=(host, port, submatrices[i], B, result_queue))
        t.start()
        threads.append(t)

    
    for t in threads:
        t.join()

    
    results_list = [] 
    for _ in range(num_servers):
        results_list.append(result_queue.get())
        
    final_result = []
   
    for host, port, result_matrix in results_list: 
        if result_matrix is None:
            print(f"Servidor {host}:{port} falhou. Resultado parcial não disponível.")
        else:
            final_result.append(result_matrix)

    if final_result:
       
        C = np.vstack(final_result)
        print("Matriz A:")
        print(A)
        print("Matriz B:")
        print(B)
        print("Resultado da multiplicação (A x B):")
        print(C)
    else:
        print("Nenhum resultado válido recebido dos servidores.")

if __name__ == "__main__":
    main()