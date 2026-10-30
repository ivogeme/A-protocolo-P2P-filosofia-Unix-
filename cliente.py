import sys

from servidor import S2SNode


def usage():
    print("""
S2S Client - S2S/0.2

Uso:

  python cliente.py IP ping
  python cliente.py IP status
  python cliente.py IP msg "Olá"

Porta alternativa:

  python cliente.py IP ping --port 9000

Exemplos:

  python cliente.py 192.168.1.10 ping

  python cliente.py 192.168.1.10 status

  python cliente.py 192.168.1.10 msg "Olá do S2S"
""")


def main():
    if len(sys.argv) < 3:
        usage()
        return

    ip = sys.argv[1]
    command = sys.argv[2].lower()

    port = 9000
    args = sys.argv[3:]

    if "--port" in args:
        index = args.index("--port")

        if index + 1 >= len(args):
            print("Erro: informe o número da porta.")
            return

        try:
            port = int(args[index + 1])

            if not 1 <= port <= 65535:
                raise ValueError

        except ValueError:
            print("Erro: a porta precisa ser um número entre 1 e 65535.")
            return

        del args[index:index + 2]

    node = S2SNode()

    try:
        if command == "ping":
            response = node.send(
                ip,
                "PING",
                remote_port=port
            )

        elif command == "status":
            response = node.send(
                ip,
                "STATUS",
                remote_port=port
            )

        elif command == "msg":
            if not args:
                print("Erro: informe a mensagem.")
                return

            message = " ".join(args)

            response = node.send(
                ip,
                "MSG",
                content=message,
                remote_port=port
            )

        else:
            usage()
            return

        print("\nResposta recebida:")
        print(response)

    except (OSError, ValueError, ConnectionError) as exc:
        print("\nFalha na comunicação S2S:")
        print(exc)


if __name__ == "__main__":
    main()
