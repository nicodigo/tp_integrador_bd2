import socket
import threading


PROXY_PUERTO = 3060
PROXY_ADDR = '192.168.0.10'

SERV_PUERTO = 3050
SERV_IP = '192.168.0.128'


def pasa_datos(fuente:socket.socket, destino:socket.socket, flag: int):
    try:
        while True:
            datos = fuente.recv(4096)
            if flag:
                print("------------------CLIENTE--------------------")
                print("\nop_code:", int.from_bytes(datos[:4]))
                print("\n datos: \n", datos) 
                if (int.from_bytes(datos[:4]) == 62):
                    print("decode: \n", datos.decode(encoding='utf-8', errors='ignore')) 
                elif (int.from_bytes(datos[:4]) == 1):
                    print("decode: \n", datos[16:(16+50)].decode(encoding='utf-8', errors='ingnore'))
            else:
                print("-------------------------SERVIDOR----------------------")
                print("\nop_code:", int.from_bytes(datos[:4]))
                print("\n datos: \n", datos) 
                if (int.from_bytes(datos[:4]) == 62):
                    print("decode: \n", datos.decode(encoding='utf-8', errors='ignore')) 
    
            if not datos:
                print("salgo")
                break
            destino.sendall(datos)
    except Exception as e:
        print(f"Error: {e}")


def manejar_cliente(socket_cliente: socket.socket):
    with socket.create_connection((SERV_IP, SERV_PUERTO)) as sServer:
        cliente_a_servidor = threading.Thread(target=(pasa_datos), args=(socket_cliente, sServer, 1))
        servidor_a_cliente = threading.Thread(target=(pasa_datos), args=(sServer, socket_cliente, 0))

        cliente_a_servidor.start()
        servidor_a_cliente.start()

        cliente_a_servidor.join()
        servidor_a_cliente.join()





def main():
    proxy_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    proxy_socket.bind((PROXY_ADDR, PROXY_PUERTO))
    proxy_socket.listen()
    print(f"servidor escuchando en {PROXY_ADDR}:{PROXY_PUERTO}")

    sCLiente, aCliente = proxy_socket.accept()
    print(f"conexion aceptada en {aCliente[0]}:{aCliente[1]}")
    manejar_cliente(sCLiente)
    
    sCLiente.close()
    proxy_socket.close()
if __name__ == '__main__':
    main()