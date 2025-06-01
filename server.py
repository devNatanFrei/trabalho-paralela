import socket
import numpy as np
import pickle
import sys 

#

def iniciar_servidor(endereco_host='localhost', porta=65432):
 
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket_servidor:
        socket_servidor.bind((endereco_host, porta))
        socket_servidor.listen()
        print(f"Servidor iniciado em {endereco_host}:{porta}")
        print("Aguardando conexões...")

        while True: 
            try:
                conexao, endereco_cliente = socket_servidor.accept()
                with conexao:
                    print(f'Conectado por {endereco_cliente}')

            
                    dados_serializados = conexao.recv(8192) 
                    if not dados_serializados:
                        print(f"Nenhum dado recebido de {endereco_cliente}. Conexão fechada.")
                        continue 

                    sub_matriz_A, matriz_B = pickle.loads(dados_serializados)

                    print(f"Recebido sub_matriz_A com shape: {sub_matriz_A.shape}")
                    print(f"Recebido matriz_B com shape: {matriz_B.shape}")

                 
                    resultado_parcial = np.dot(sub_matriz_A, matriz_B)
                    print(f"Calculado resultado_parcial com shape: {resultado_parcial.shape}")

                   
                    dados_resultado_serializados = pickle.dumps(resultado_parcial)
                    conexao.sendall(dados_resultado_serializados)
                    print(f"Resultado parcial enviado para {endereco_cliente}.")

            except pickle.UnpicklingError:
                print(f"Erro ao deserializar dados de {endereco_cliente}. A conexão pode ter sido interrompida.")
            except ConnectionResetError:
                print(f"Conexão resetada por {endereco_cliente}.")
            except Exception as e:
                print(f"Ocorreu um erro inesperado: {e}")
            
            print("Aguardando próxima conexão...")


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