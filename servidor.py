import socket  
import json  
import time  
  
HOST = "0.0.0.0"   # aceita conexões de qualquer IP  
PORT = 9000  
  
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
s.bind((HOST, PORT))  
s.listen(5)  
  
print("Servidor ouvindo na porta", PORT)  
  
while True:  
    conn, addr = s.accept()  
    print("Conexão de:", addr)  
  
    data = conn.recv(1024)  
    if not data:  
        conn.close()  
        continue  
  
    try:  
        msg = json.loads(data.decode())  
        print("Recebido:", msg)  
  
        # Lógica para tipos diferentes  
        if msg["tipo"] == "STATUS":  
            resposta = {  
                "tipo": "STATUS",  
                "origem": "srv2",  
                "destino": msg.get("origem"),  
                "timestamp": int(time.time()),  
                "conteudo": "OK"  
            }  
  
        elif msg["tipo"] == "MSG":  
            resposta = {  
                "tipo": "CONFIRM",  
                "origem": "srv2",  
                "destino": msg.get("origem"),  
                "timestamp": int(time.time()),  
                "conteudo": "Recebido"  
            }  
  
        elif msg["tipo"] == "CMD":  
            comando = msg.get("conteudo")  
            resposta = {  
                "tipo": "CMD",  
                "origem": "srv2",  
                "destino": msg.get("origem"),  
                "timestamp": int(time.time()),  
                "conteudo": f"Comando '{comando}' executado"  
            }  
  
        elif msg["tipo"] == "PING":  
            resposta = {  
                "tipo": "PONG",  
                "origem": "srv2",  
                "destino": msg.get("origem"),  
                "timestamp": int(time.time()),  
                "conteudo": ""  
            }  
  
        else:  
            resposta = {  
                "tipo": "ERRO",  
                "origem": "srv2",  
                "destino": msg.get("origem"),  
                "timestamp": int(time.time()),  
                "conteudo": "Tipo desconhecido"  
            }  
  
    except Exception as e:  
        resposta = {  
            "tipo": "ERRO",  
            "origem": "srv2",  
            "destino": "desconhecido",  
            "timestamp": int(time.time()),  
            "conteudo": f"Erro ao processar mensagem: {e}"  
        }  
  
    conn.sendall(json.dumps(resposta).encode())  
    conn.close()