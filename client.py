import socket
import pickle
import numpy as np
from threading import Thread
from queue import Queue, Empty 



def enviar_para_servidor(indice_original, endereco_host, porta_str, sub_matriz_A, matriz_B, fila_resultados):
   
    try:
        porta = int(porta_str) 
        socket_cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket_cliente.settimeout(15) 
        print(f"Thread {indice_original}: Tentando conectar a {endereco_host}:{porta}...")
        socket_cliente.connect((endereco_host, porta))
        print(f"Thread {indice_original}: Conectado a {endereco_host}:{porta}.")

       
        dados = pickle.dumps((sub_matriz_A, matriz_B))
        socket_cliente.sendall(dados)
        print(f"Thread {indice_original}: Dados enviados para {endereco_host}:{porta}.")

        
        dados_resultado_serializados = socket_cliente.recv(8192) 
        if not dados_resultado_serializados:
            print(f"Thread {indice_original}: Nenhum dado de resultado recebido de {endereco_host}:{porta}.")
            fila_resultados.put((indice_original, endereco_host, porta_str, None))
            return

        resultado = pickle.loads(dados_resultado_serializados)
        print(f"Thread {indice_original}: Resultado recebido de {endereco_host}:{porta} com shape {resultado.shape}.")
        fila_resultados.put((indice_original, endereco_host, porta_str, resultado))

    except (socket.timeout, ConnectionRefusedError) as e:
        print(f"Thread {indice_original}: Erro ao conectar ou enviar dados para {endereco_host}:{porta_str}. Erro: {e}")
        fila_resultados.put((indice_original, endereco_host, porta_str, None))


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
                            print(f"Aviso: Porta '{porta_str}' na linha {numero_linha} do arquivo de configuração não é um número. Linha ignorada: '{linha_texto}'")
                            continue
                        lista_servidores.append((endereco_host.strip(), porta_str.strip()))
                    except ValueError:
                        print(f"Aviso: Linha mal formatada no arquivo de configuração na linha {numero_linha} ignorada: '{linha_texto}' (esperado formato host:porta)")
    except FileNotFoundError:
        print(f"Erro: Arquivo de configuração '{arquivo_config}' não encontrado.")
    except Exception as e:
        print(f"Erro ao ler o arquivo de configuração '{arquivo_config}': {e}")
    return lista_servidores

def main():
    
    config_servidores = carregar_servidores_do_config()

    if not config_servidores:
        print("Nenhum servidor carregado do arquivo de configuração ou arquivo não encontrado. Encerrando.")
        return

    num_servidores = len(config_servidores)
    print(f"Número de servidores configurados: {num_servidores}")


    matriz_A = np.array([[1, 2, 3],
                     [4, 5, 6],
                     [7, 8, 9],
                     [1, 0, 1]])
    matriz_B = np.array([[1, 0],
                     [0, 1],
                     [1, 1]])

    if matriz_A.shape[1] != matriz_B.shape[0]:
        print("Erro: As matrizes A e B são incompatíveis para multiplicação.")
        print(f"Dimensões: A={matriz_A.shape}, B={matriz_B.shape}")
        return


    if matriz_A.shape[0] < num_servidores:
        print(f"Erro: Número de linhas em A ({matriz_A.shape[0]}) é menor que o número de servidores ({num_servidores}).")
        print("Não é possível dividir a matriz A adequadamente.")
        return


    sub_matrizes_A = np.array_split(matriz_A, num_servidores, axis=0)

    fila_resultados = Queue()
    lista_threads = []

    print("Iniciando threads para enviar trabalho aos servidores...")
    for i, (endereco_host, porta_str) in enumerate(config_servidores):
    
        thread = Thread(target=enviar_para_servidor, args=(i, endereco_host, porta_str, sub_matrizes_A[i], matriz_B, fila_resultados))
        lista_threads.append(thread)
        thread.start()


    for i, thread in enumerate(lista_threads):
        thread.join()
        print(f"Thread {i} finalizada.")


    resultados_temporarios_ordenados = [None] * num_servidores
    info_servidores_falha = {} 

    timeout_geral_fila = num_servidores * 20 
    print(f"Coletando resultados da fila (timeout de {timeout_geral_fila}s)...")

    for _ in range(num_servidores):
        try:
           
            indice_original, host_serv, porta_serv, resultado_parcial = fila_resultados.get(timeout=timeout_geral_fila / num_servidores + 5)
            if resultado_parcial is not None:
                resultados_temporarios_ordenados[indice_original] = resultado_parcial
            else:
               
                resultados_temporarios_ordenados[indice_original] = None 
                info_servidores_falha[indice_original] = (host_serv, porta_serv)
        except Empty:
            print("Timeout ao esperar por todos os resultados da fila. Alguma thread pode ter falhado criticamente ou não colocou resultado.")
           
            for idx in range(num_servidores):
                if resultados_temporarios_ordenados[idx] is None and idx not in info_servidores_falha:
                    info_servidores_falha[idx] = ("Desconhecido", "Desconhecida") 
            break 


    todos_bem_sucedidos = True
    matrizes_resultado_final = []

    print("\nVerificando resultados recebidos:")
    for i in range(num_servidores):
        parte_resultado = resultados_temporarios_ordenados[i]
        if isinstance(parte_resultado, np.ndarray):
            matrizes_resultado_final.append(parte_resultado)
            print(f"  Parte {i}: Recebida com sucesso (shape: {parte_resultado.shape}).")
        else:
            todos_bem_sucedidos = False
            host_falha, porta_falha = info_servidores_falha.get(i, ("Desconhecido", "Desconhecida"))
            if host_falha != "Desconhecido":
                print(f"  Parte {i}: Falha no servidor {host_falha}:{porta_falha} ou resultado não recebido.")
            else:
                print(f"  Parte {i}: Resultado não recebido (possível falha crítica de thread).")


   
    if todos_bem_sucedidos and matrizes_resultado_final:
        
        num_cols_esperado = matriz_B.shape[1]
        if not all(m.shape[1] == num_cols_esperado for m in matrizes_resultado_final):
            print("\nErro: As submatrizes resultantes dos servidores têm diferentes números de colunas.")
            for i_res, m_res in enumerate(matrizes_resultado_final):
                print(f"  Resultado da parte original {i_res}: shape {m_res.shape}") 
            return

        try:
            matriz_C = np.vstack(matrizes_resultado_final)
            print("\n--- RESULTADO FINAL ---")
            print("Matriz A:")
            print(matriz_A)
            print("Matriz B:")
            print(matriz_B)
            print("Resultado da multiplicação (A x B):")
            print(matriz_C)
        except ValueError as e_vstack: 
            print(f"\nErro ao empilhar os resultados para formar a matriz C: {e_vstack}")
            print("Resultados parciais recebidos (na ordem original esperada das partes de A):")
            for i_res, item_res in enumerate(resultados_temporarios_ordenados):
                if isinstance(item_res, np.ndarray):
                    print(f"  Parte {i_res} (sucesso): shape {item_res.shape}\n{item_res}")
                else:
                    hf, pf = info_servidores_falha.get(i_res, ("Desconhecido","Desconhecida"))
                    print(f"  Parte {i_res} (falha no servidor {hf}:{pf} ou não recebida)")

    elif not matrizes_resultado_final and not todos_bem_sucedidos:
        print("\nNenhum resultado válido foi recebido e ocorreram falhas em todos os servidores ou threads.")
    elif not matrizes_resultado_final:
        print("\nNenhum resultado válido foi recebido dos servidores (возможно, todos falharam ou nenhum foi configurado).")
    else: 
        print("\nResultados parciais foram recebidos, mas nem todos os servidores foram bem-sucedidos.")
        print("A matriz C completa não pode ser formada.")
        print("Detalhes das partes processadas:")
        for i in range(num_servidores):
            item = resultados_temporarios_ordenados[i]
            if isinstance(item, np.ndarray):
                print(f"  Parte {i} (sucesso): shape {item.shape}")
            else:
                host_f, porta_f = info_servidores_falha.get(i, ("Desconhecido", "Desconhecida"))
                print(f"  Parte {i} (falha no servidor {host_f}:{porta_f} ou não recebida)")

if __name__ == "__main__":
    main()