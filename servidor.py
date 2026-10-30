import socket
import json
import time
import uuid
import threading
import struct
import os

S2S_VERSION = "S2S/0.2"
DEFAULT_PORT = 9000
DISCOVERY_PORT = 9001
DISCOVERY_MESSAGE = "S2S_DISCOVERY"
MAX_FRAME_SIZE = 1024 * 1024

NODE_NAME = socket.gethostname()


def load_node_id():
    """Carrega um identificador persistente ou cria um novo."""
    path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        ".s2s_node_id"
    )

    try:
        with open(path, "r", encoding="utf-8") as file:
            node_id = file.read().strip()

        uuid.UUID(node_id)
        return node_id

    except (OSError, ValueError):
        node_id = str(uuid.uuid4())

        try:
            with open(path, "w", encoding="utf-8") as file:
                file.write(node_id)
        except OSError:
            pass

        return node_id


def send_frame(sock, message):
    """Envia uma mensagem JSON precedida pelo tamanho em 4 bytes."""
    data = json.dumps(
        message,
        ensure_ascii=False,
        separators=(",", ":")
    ).encode("utf-8")

    if len(data) > MAX_FRAME_SIZE:
        raise ValueError("Mensagem excede o tamanho máximo permitido.")

    header = struct.pack("!I", len(data))
    sock.sendall(header + data)


def recv_exact(sock, size):
    """Lê exatamente size bytes, salvo se a conexão for encerrada."""
    chunks = []
    remaining = size

    while remaining > 0:
        chunk = sock.recv(remaining)

        if not chunk:
            return None

        chunks.append(chunk)
        remaining -= len(chunk)

    return b"".join(chunks)


def recv_frame(sock):
    """Recebe um frame TCP e decodifica o JSON."""
    header = recv_exact(sock, 4)

    if header is None:
        return None

    length = struct.unpack("!I", header)[0]

    if length == 0:
        raise ValueError("Frame vazio.")

    if length > MAX_FRAME_SIZE:
        raise ValueError("Frame muito grande.")

    data = recv_exact(sock, length)

    if data is None:
        raise ConnectionError(
            "Conexão encerrada durante a recepção do frame."
        )

    try:
        message = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("JSON inválido.") from exc

    if not isinstance(message, dict):
        raise ValueError("A mensagem precisa ser um objeto JSON.")

    return message


class S2SNode:

    def __init__(self, host="0.0.0.0", port=DEFAULT_PORT):
        self.host = host
        self.port = port

        self.node_id = load_node_id()
        self.node_name = NODE_NAME
        self.start_time = time.time()

        self.nodes = {}
        self.nodes_lock = threading.Lock()

        self.running = False
        self.server_socket = None

        self.capabilities = [
            "MSG",
            "PING",
            "STATUS",
            "DISCOVERY"
        ]

    # --------------------------------------------------
    # MENSAGENS
    # --------------------------------------------------

    def make_message(
        self,
        msg_type,
        destination="",
        content=None,
        message_id=None
    ):
        return {
            "version": S2S_VERSION,
            "tipo": msg_type,
            "id": message_id or str(uuid.uuid4()),
            "origem": self.node_id,
            "nome_origem": self.node_name,
            "destino": destination,
            "timestamp": int(time.time()),
            "conteudo": content
        }

    def status(self):
        return {
            "node_id": self.node_id,
            "name": self.node_name,
            "version": S2S_VERSION,
            "uptime": int(time.time() - self.start_time),
            "capabilities": self.capabilities
        }

    # --------------------------------------------------
    # SERVIDOR TCP
    # --------------------------------------------------

    def start(self):
        if self.running:
            print("O nó S2S já está em execução.")
            return

        server = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        server.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1
        )

        try:
            server.bind((self.host, self.port))
            server.listen(10)

        except OSError:
            server.close()
            raise

        self.server_socket = server
        self.running = True

        print()
        print("================================")
        print("           S2S NODE")
        print("================================")
        print("Versão  :", S2S_VERSION)
        print("Node ID :", self.node_id)
        print("Nome    :", self.node_name)
        print("Endereço:", self.host)
        print("Porta   :", self.port)
        print("================================")
        print()

        threading.Thread(
            target=self.discovery_listener,
            daemon=True
        ).start()

        threading.Thread(
            target=self.discovery_sender,
            daemon=True
        ).start()

        try:
            while self.running:
                try:
                    conn, addr = server.accept()

                    thread = threading.Thread(
                        target=self.handle_connection,
                        args=(conn, addr),
                        daemon=True
                    )
                    thread.start()

                except OSError:
                    if self.running:
                        raise

        except KeyboardInterrupt:
            print("\nEncerrando o nó S2S...")

        finally:
            self.stop()

    def stop(self):
        self.running = False

        if self.server_socket:
            try:
                self.server_socket.close()
            except OSError:
                pass

            self.server_socket = None

    # --------------------------------------------------
    # HANDSHAKE E CONEXÃO
    # --------------------------------------------------

    def handle_connection(self, conn, addr):
        conn.settimeout(15)

        print("[TCP] Conexão de", addr)

        try:
            # O cliente deve iniciar com HELLO.
            hello = recv_frame(conn)

            if hello is None:
                return

            if hello.get("tipo") != "HELLO":
                send_frame(
                    conn,
                    self.make_message(
                        "ERROR",
                        content={"code": "EXPECTED_HELLO"}
                    )
                )
                return

            if hello.get("version") != S2S_VERSION:
                send_frame(
                    conn,
                    self.make_message(
                        "ERROR",
                        content={"code": "UNSUPPORTED_VERSION"}
                    )
                )
                return

            # Resposta HELLO.
            send_frame(
                conn,
                self.make_message("HELLO")
            )

            # Identificação do servidor.
            send_frame(
                conn,
                self.make_message(
                    "IDENTIFY",
                    content=self.status()
                )
            )

            # Indica que o servidor está pronto.
            send_frame(
                conn,
                self.make_message("READY")
            )

            # Recebe as operações da conexão.
            while self.running:
                message = recv_frame(conn)

                if message is None:
                    break

                if message.get("version") != S2S_VERSION:
                    response = self.make_message(
                        "ERROR",
                        destination=message.get("origem", ""),
                        content={"code": "UNSUPPORTED_VERSION"},
                        message_id=message.get("id")
                    )
                else:
                    response = self.process_message(message)

                send_frame(conn, response)

        except socket.timeout:
            print("[TCP] Tempo limite excedido:", addr)

        except (
            ConnectionError,
            OSError,
            ValueError
        ) as exc:
            print("[TCP] Conexão encerrada:", addr, "-", exc)

        finally:
            try:
                conn.close()
            except OSError:
                pass

            print("[TCP] Conexão encerrada:", addr)

    # --------------------------------------------------
    # PROCESSAMENTO DAS MENSAGENS
    # --------------------------------------------------

    def process_message(self, message):
        msg_type = message.get("tipo")
        origin = message.get("origem", "")
        message_id = message.get("id")

        if msg_type == "PING":
            return self.make_message(
                "PONG",
                destination=origin,
                content="",
                message_id=message_id
            )

        if msg_type == "STATUS":
            return self.make_message(
                "STATUS",
                destination=origin,
                content=self.status(),
                message_id=message_id
            )

        if msg_type == "MSG":
            content = message.get("conteudo", "")

            if not isinstance(content, str):
                return self.make_message(
                    "ERROR",
                    destination=origin,
                    content={"code": "INVALID_MESSAGE"},
                    message_id=message_id
                )

            print(
                "\n[MSG] {}: {}".format(
                    message.get("nome_origem", origin),
                    content
                )
            )

            return self.make_message(
                "OK",
                destination=origin,
                content={"code": "MESSAGE_RECEIVED"},
                message_id=message_id
            )

        if msg_type == "DISCOVER":
            return self.make_message(
                "IDENTIFY",
                destination=origin,
                content=self.status(),
                message_id=message_id
            )

        return self.make_message(
            "ERROR",
            destination=origin,
            content={"code": "UNSUPPORTED"},
            message_id=message_id
        )

    # --------------------------------------------------
    # CLIENTE TCP
    # --------------------------------------------------

    def send(
        self,
        address,
        msg_type,
        content="",
        destination="",
        remote_port=None
    ):
        port = remote_port if remote_port is not None else self.port

        request_id = str(uuid.uuid4())

        sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )
        sock.settimeout(10)

        try:
            sock.connect((address, port))

            # Envia HELLO.
            send_frame(
                sock,
                self.make_message("HELLO")
            )

            # Confere resposta HELLO.
            hello = recv_frame(sock)

            if hello is None:
                raise ConnectionError(
                    "O servidor não respondeu ao HELLO."
                )

            if hello.get("tipo") == "ERROR":
                raise ConnectionError(
                    "Handshake recusado: {}".format(
                        hello.get("conteudo")
                    )
                )

            if hello.get("tipo") != "HELLO":
                raise ConnectionError(
                    "Resposta HELLO inválida."
                )

            if hello.get("version") != S2S_VERSION:
                raise ConnectionError(
                    "Versão do protocolo incompatível."
                )

            # Espera IDENTIFY.
            identify = recv_frame(sock)

            if (
                identify is None
                or identify.get("tipo") != "IDENTIFY"
            ):
                raise ConnectionError(
                    "Resposta IDENTIFY inválida."
                )

            print(
                "[REMOTE]",
                identify.get("conteudo")
            )

            # Espera READY.
            ready = recv_frame(sock)

            if ready is None or ready.get("tipo") != "READY":
                raise ConnectionError(
                    "O servidor não enviou READY."
                )

            # Envia operação solicitada.
            message = self.make_message(
                msg_type,
                destination=destination,
                content=content,
                message_id=request_id
            )

            send_frame(sock, message)

            response = recv_frame(sock)

            if response is None:
                raise ConnectionError(
                    "O servidor encerrou a conexão sem responder."
                )

            if response.get("id") != request_id:
                raise ConnectionError(
                    "O identificador da resposta não corresponde "
                    "à solicitação."
                )

            return response

        finally:
            sock.close()

    # --------------------------------------------------
    # DESCOBERTA UDP
    # --------------------------------------------------

    def discovery_listener(self):
        sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM
        )

        sock.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1
        )

        try:
            sock.bind(("0.0.0.0", DISCOVERY_PORT))
            sock.settimeout(1)

            print(
                "[DISCOVERY] Escutando UDP na porta",
                DISCOVERY_PORT
            )

            while self.running:
                try:
                    data, addr = sock.recvfrom(4096)

                    if data.decode(
                        "utf-8",
                        errors="ignore"
                    ).strip() != DISCOVERY_MESSAGE:
                        continue

                    response = json.dumps(
                        self.status(),
                        ensure_ascii=False
                    ).encode("utf-8")

                    sock.sendto(response, addr)

                except socket.timeout:
                    continue

                except OSError:
                    if self.running:
                        print("[DISCOVERY] Erro de socket.")
                    break

        except OSError as exc:
            print("[DISCOVERY] Não foi possível iniciar:", exc)

        finally:
            sock.close()

    def discovery_sender(self):
        sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM
        )

        sock.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_BROADCAST,
            1
        )
        sock.settimeout(1)

        try:
            while self.running:
                try:
                    sock.sendto(
                        DISCOVERY_MESSAGE.encode("utf-8"),
                        ("255.255.255.255", DISCOVERY_PORT)
                    )

                    deadline = time.time() + 2

                    while self.running and time.time() < deadline:
                        try:
                            data, addr = sock.recvfrom(4096)

                            node = json.loads(
                                data.decode("utf-8")
                            )

                            node_id = node.get("node_id")

                            if not node_id or node_id == self.node_id:
                                continue

                            with self.nodes_lock:
                                self.nodes[node_id] = {
                                    "address": addr[0],
                                    "port": DEFAULT_PORT,
                                    "name": node.get("name"),
                                    "version": node.get("version"),
                                    "last_seen": time.time()
                                }

                            print(
                                "[DISCOVERY] Nó encontrado:",
                                node.get("name"),
                                addr[0]
                            )

                        except socket.timeout:
                            break

                        except (
                            ValueError,
                            UnicodeDecodeError,
                            json.JSONDecodeError
                        ):
                            continue

                except OSError as exc:
                    if self.running:
                        print("[DISCOVERY] Erro:", exc)

                time.sleep(10)

        finally:
            sock.close()

    # --------------------------------------------------
    # TABELA DE NÓS
    # --------------------------------------------------

    def show_nodes(self):
        print("\n===== S2S NODE TABLE =====")

        with self.nodes_lock:
            nodes = list(self.nodes.items())

        if not nodes:
            print("Nenhum nó encontrado.")

        for node_id, node in nodes:
            age = int(time.time() - node["last_seen"])
            state = "online" if age < 30 else "offline"

            print(
                "{} | {} | {} | {} | último contato: {} s".format(
                    node["name"],
                    node["address"],
                    state,
                    node["version"],
                    age
                )
            )

        print("==========================\n")


if __name__ == "__main__":
    node = S2SNode()
    node.start()
