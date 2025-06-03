import socket
import pickle
import numpy as np
from threading import Thread
from queue import Queue, Empty
import time

def enviar_para_servidor(indice_original, endereco_host, porta_str, sub_matriz_A, matriz_B, fila_resultados):
    socket_cliente = None
    try:
        porta = int(porta_str)
        socket_cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket_cliente.settimeout(60)
        socket_cliente.connect((endereco_host, porta))

        dados_pickle = pickle.dumps((sub_matriz_A, matriz_B))
        tamanho_dados_bytes = len(dados_pickle).to_bytes(8, byteorder='big')
        socket_cliente.sendall(tamanho_dados_bytes)
        socket_cliente.sendall(dados_pickle)

        dados_tamanho_resultado_bytes = socket_cliente.recv(8)
        if not dados_tamanho_resultado_bytes or len(dados_tamanho_resultado_bytes) < 8:
            print(f"Thread {indice_original}: Não foi possível ler o tamanho do resultado de {endereco_host}:{porta}.")
            fila_resultados.put((indice_original, endereco_host, porta_str, None))
            return
        tamanho_resultado_pickle = int.from_bytes(dados_tamanho_resultado_bytes, byteorder='big')

        dados_resultado_completos = b''
        bytes_recebidos_res = 0
        buffer_recv_cliente = 8192
        while bytes_recebidos_res < tamanho_resultado_pickle:
            bytes_para_ler_res = min(buffer_recv_cliente, tamanho_resultado_pickle - bytes_recebidos_res)
            parte_dados_res = socket_cliente.recv(bytes_para_ler_res)
            if not parte_dados_res:
                print(f"Thread {indice_original}: Conexão fechada ao receber dados do resultado de {endereco_host}:{porta}.")
                dados_resultado_completos = None
                break
            dados_resultado_completos += parte_dados_res
            bytes_recebidos_res += len(parte_dados_res)

        if dados_resultado_completos is None or bytes_recebidos_res != tamanho_resultado_pickle:
            print(f"Thread {indice_original}: Falha ao receber dados completos do resultado de {endereco_host}:{porta}.")
            fila_resultados.put((indice_original, endereco_host, porta_str, None))
            return

        resultado = pickle.loads(dados_resultado_completos)
        fila_resultados.put((indice_original, endereco_host, porta_str, resultado))

    except (OSError, socket.timeout, pickle.UnpicklingError, ValueError) as e:
        print(f"Thread {indice_original}: Erro na comunicação com {endereco_host}:{porta_str}. Erro: {e}")
        fila_resultados.put((indice_original, endereco_host, porta_str, None))
    except Exception as e:
        print(f"Thread {indice_original}: Erro inesperado com servidor {endereco_host}:{porta_str}: {e}")
        fila_resultados.put((indice_original, endereco_host, porta_str, None))
    finally:
        if socket_cliente:
            try:
                socket_cliente.close()
            except Exception as e_close:
                print(f"Thread {indice_original}: Erro ao fechar socket para {endereco_host}:{porta_str}: {e_close}")


def carregar_servidores_do_config(arquivo_config="servers.conf"):
    lista_servidores = []
    try:
        with open(arquivo_config, 'r', encoding='utf-8') as f:
            for numero_linha, linha_texto in enumerate(f, 1):
                linha_texto = linha_texto.strip()
                if linha_texto and not linha_texto.startswith('#'):
                    try:
                        endereco_host, porta_str = linha_texto.split(':')
                        if not porta_str.strip().isdigit():
                         
                            continue
                        lista_servidores.append((endereco_host.strip(), porta_str.strip()))
                    except ValueError:
                 
                        pass 
    except FileNotFoundError:
        print(f"Erro: Arquivo de configuração '{arquivo_config}' não encontrado.")
    except Exception as e:
        print(f"Erro ao ler o arquivo de configuração '{arquivo_config}': {e}")
    return lista_servidores

def main():
    config_servidores = carregar_servidores_do_config()

    if not config_servidores:
        print("Nenhum servidor carregado...")
        return

    num_servidores = len(config_servidores)
    print(f"Número de servidores configurados: {num_servidores}")

    linhas_A = 10
    cols_A_linhas_B = 10
    cols_B = 10


  
    matriz_A = np.random.rand(linhas_A, cols_A_linhas_B) 
    matriz_B = np.random.rand(cols_A_linhas_B, cols_B) 


    if matriz_A.shape[1] != matriz_B.shape[0]:
        print("Erro: As matrizes A e B são incompatíveis...")
        return
    if matriz_A.shape[0] < num_servidores:
        print(f"Erro: Número de linhas em A ({matriz_A.shape[0]}) é menor que ...")
        return

    print("\n--- Executando Multiplicação Serial (Local) ---")
    start_serial = time.perf_counter()
    matriz_C_serial = np.dot(matriz_A, matriz_B)
    end_serial = time.perf_counter()
    tempo_serial = end_serial - start_serial
    print(f"Tempo de execução serial: {tempo_serial:.6f} segundos")

    print("\n--- Executando Multiplicação Paralela (Distribuída) ---")
    start_paralelo = time.perf_counter()

    sub_matrizes_A = np.array_split(matriz_A, num_servidores, axis=0)
    fila_resultados = Queue()
    lista_threads = []

    for i, (endereco_host, porta_str) in enumerate(config_servidores):
        thread = Thread(target=enviar_para_servidor, args=(i, endereco_host, porta_str, sub_matrizes_A[i], matriz_B, fila_resultados))
        lista_threads.append(thread)
        thread.start()

    for thread in lista_threads:
        thread.join()

    resultados_temporarios_ordenados = [None] * num_servidores
    info_servidores_falha = {}
    timeout_geral_fila_soma = num_servidores * 40 + 60

    for _ in range(num_servidores):
        try:
            timeout_item_fila = (timeout_geral_fila_soma / num_servidores) if num_servidores > 0 else timeout_geral_fila_soma
            timeout_item_fila = max(20, timeout_item_fila)
            indice_original, host_serv, porta_serv, resultado_parcial = fila_resultados.get(timeout=timeout_item_fila)
            if resultado_parcial is not None:
                resultados_temporarios_ordenados[indice_original] = resultado_parcial
            else:
                resultados_temporarios_ordenados[indice_original] = None
                info_servidores_falha[indice_original] = (host_serv, porta_serv)
        except Empty:
            print("Timeout ao esperar por todos os resultados da fila...")
            for idx in range(num_servidores):
                if resultados_temporarios_ordenados[idx] is None and idx not in info_servidores_falha:
                    info_servidores_falha[idx] = ("Desconhecido", "Desconhecida")
            break

    todos_bem_sucedidos = True
    matrizes_resultado_final = []
    matriz_C_paralelo = None

    for i in range(num_servidores):
        parte_resultado = resultados_temporarios_ordenados[i]
        if isinstance(parte_resultado, np.ndarray):
            matrizes_resultado_final.append(parte_resultado)
        else:
            todos_bem_sucedidos = False
            host_falha, porta_falha = info_servidores_falha.get(i, ("Desconhecido", "Desconhecida"))
            print(f"  Parte {i}: Falha no servidor {host_falha}:{porta_falha} ou resultado não recebido.")

    if todos_bem_sucedidos and matrizes_resultado_final:
        try:
            matriz_C_paralelo = np.vstack(matrizes_resultado_final)
        except ValueError as e_vstack:
            print(f"\nErro ao empilhar os resultados para formar a matriz C: {e_vstack}")
            todos_bem_sucedidos = False

    end_paralelo = time.perf_counter()
    tempo_paralelo = end_paralelo - start_paralelo
    print(f"Tempo de execução paralela (distribuída): {tempo_paralelo:.6f} segundos")

    if todos_bem_sucedidos and matriz_C_paralelo is not None:
        print("\n--- RESULTADO FINAL (Distribuído) ---")
        print("Matriz A :")
        print(matriz_A)
        print("Matriz B :")
        print(matriz_B)
        print("Resultado da multiplicação (A x B):")
        print(matriz_C_paralelo)
    else:
        print("\n--- Resultado da multiplicação (Distribuído) ---")
        print(f"Shape de A: {matriz_A.shape}, Shape de B: {matriz_B.shape}, Shape de C: {matriz_C_paralelo.shape}")

        print("\n--- Verificação de Correção e Comparação de Tempo ---")
        if np.allclose(matriz_C_serial, matriz_C_paralelo, atol=1e-5, rtol=1e-5):
            print("VERIFICAÇÃO: O resultado distribuído CORRESPONDE ao resultado serial.")
            if tempo_paralelo > 0:
                speedup = tempo_serial / tempo_paralelo
                print(f"Speedup (Serial/Paralelo): {speedup:.2f}x")
            else:
                print("Não é possível calcular o speedup.")
      
   

if __name__ == "__main__":
    main()