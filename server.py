import socket
import numpy as np
import pickle
import sys

def iniciar_servidor(endereco_host='localhost', porta=65432):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket_servidor:
        socket_servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        socket_servidor.bind((endereco_host, porta))
        socket_servidor.listen()
        print(f"Servidor iniciado em {endereco_host}:{porta}")
        print("Aguardando conexões...")

        while True:
            conexao = None
            endereco_cliente_str = "um cliente desconhecido"
            try:
                conexao, endereco_cliente = socket_servidor.accept()
                endereco_cliente_str = str(endereco_cliente)
                with conexao:
                   

                    dados_tamanho_msg_bytes = conexao.recv(8)
                    if not dados_tamanho_msg_bytes or len(dados_tamanho_msg_bytes) < 8:
                        print(f"Não foi possível ler o tamanho da mensagem de {endereco_cliente_str}. Conexão fechada.")
                        continue
                    tamanho_msg_pickle = int.from_bytes(dados_tamanho_msg_bytes, byteorder='big')
                    

                    dados_serializados_completos = b''
                    bytes_recebidos = 0
                    buffer_recv_servidor = 8192
                    while bytes_recebidos < tamanho_msg_pickle:
                        bytes_para_ler = min(buffer_recv_servidor, tamanho_msg_pickle - bytes_recebidos)
                        parte_dados = conexao.recv(bytes_para_ler)
                        if not parte_dados:
                            print(f"Conexão fechada por {endereco_cliente_str} ao receber dados (recebido {bytes_recebidos}/{tamanho_msg_pickle} bytes).")
                            dados_serializados_completos = None
                            break
                        dados_serializados_completos += parte_dados
                        bytes_recebidos += len(parte_dados)

                    if dados_serializados_completos is None or bytes_recebidos != tamanho_msg_pickle:
                        print(f"Falha ao receber dados completos de {endereco_cliente_str} (esperado {tamanho_msg_pickle}, recebido {bytes_recebidos}).")
                        continue

                    sub_matriz_A, matriz_B = pickle.loads(dados_serializados_completos)
                    

                    resultado_parcial = np.dot(sub_matriz_A, matriz_B)
                    

                    dados_resultado_serializados = pickle.dumps(resultado_parcial)
                    tamanho_resultado_bytes = len(dados_resultado_serializados).to_bytes(8, byteorder='big')
                    conexao.sendall(tamanho_resultado_bytes)
                    conexao.sendall(dados_resultado_serializados)
                  

            except pickle.UnpicklingError as e:
                print(f"Erro ao deserializar dados de {endereco_cliente_str}: {e}.")
            except ConnectionResetError:
                print(f"Conexão resetada por {endereco_cliente_str}.")
            except socket.timeout:
                 print(f"Timeout na conexão com {endereco_cliente_str}.")
            except OSError as e:
                 print(f"Erro de OS/Socket com {endereco_cliente_str}: {e}")
            except Exception as e:
                print(f"Ocorreu um erro inesperado com {endereco_cliente_str}: {e}")
          
if __name__ == "__main__":
    if len(sys.argv) > 1:
        try:
            porta_servidor = int(sys.argv[1])
        except ValueError:
            print("Porta inválida fornecida. Usando a porta padrão 65432.")
            porta_servidor = 65432
    else:
        porta_servidor = 65432
    iniciar_servidor(porta=porta_servidor)