import socket
import json
import time

msg = {
    "tipo": "PING",
    "origem": "srv1",
    "destino": "srv2",
    "timestamp": int(time.time()),
    "conteudo": ""
}

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# coloque o IP da Máquina A aqui
s.connect(("IP_DA_MAQUINA_A", 9000))

s.sendall(json.dumps(msg).encode())

resposta = s.recv(1024)
print("Resposta recebida:", resposta.decode())

s.close()