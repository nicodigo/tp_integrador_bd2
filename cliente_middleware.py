import json
from sys import argv
import socket
import struct
from prettytable import PrettyTable as PT


def enviar_mensaje(sCliente:socket.socket, mensaje: str):
    mensaje_bytes = mensaje.encode('utf-8')
    size = len(mensaje_bytes)

    size_bytes = struct.pack('!I', size)

    sCliente.sendall(size_bytes)
    sCliente.sendall(mensaje_bytes)
    return

def recibir_mensaje(socket_cli: socket.socket):
    
    size_datos = socket_cli.recv(4)
    if not size_datos:
        return None
    
    size = struct.unpack('!I', size_datos)[0]

    datos = b""
    while len(datos) < size:
        paquete = socket_cli.recv(size - len(datos))
        if not paquete:
            return None
        datos += paquete
    
    return datos.decode('utf-8')

def imprimirRespuesta(res: str):
    if res.upper().startswith('ERROR'):
        print(res)
        return
    
    res_dict = json.loads(res)
    filas_afectadas = res_dict['filas_afectadas']
    cols = res_dict['columnas']
    if cols == []:
        return
    filas = res_dict['filas']
    tabla = PT(cols)
    for fila in filas:
        tabla.add_row(fila)

    print(tabla)    

def input_sql() -> str:
    buffer = []
    try:
        while True:
            linea = input('> ')
            buffer.append(linea.strip())
            if linea.strip().endswith(';'):
                break
        return "".join(buffer).strip()
    except KeyboardInterrupt:
        return 'QUIT;'
    

def input_desde_archivo(miSocket: socket.socket, nombre_archivo: str):
    try:
        with open(nombre_archivo, 'r') as script_sql:
            script = script_sql.read()

            comandos = [com.strip() for com in script.split(';') if com.strip()]

            for comando in comandos:
                enviar_mensaje(miSocket, comando)
                respuesta = recibir_mensaje(miSocket)
                if not respuesta:
                    break
                imprimirRespuesta(respuesta)
    except Exception as e:
        print(e)


def main():
    if len(argv) != 5:
        print('Uso correcto: ./cliente.py ip_servidor puerto_servidor usuario contraseña')
        return
    ip_serv = argv[1]
    try: 
        puerto_serv = int(argv[2])
    except ValueError:
        print("Error: el puerto debe ser un entero valido")
        return

    user = argv[3]
    if not user.isalnum():
        print('Usuario invalido')
        return
    passw = argv[4]
    if not passw.isalnum():
        print('Contraseña invalida')
        return      


    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as miSocket:
        try:
            miSocket.connect((ip_serv, puerto_serv))
        except:
            print("Error al conectar")
            return
        enviar_mensaje(miSocket, f'{user}|{passw}')
        respuesta = recibir_mensaje(miSocket)
        if not respuesta:
            miSocket.close()
            return
        print(respuesta)
        if respuesta.startswith('ERROR'):
            return
        
        while True:
            sql = input_sql()
            if sql.upper().strip() == 'QUIT;':
                break
            if sql.upper().strip().startswith('INPUT '):
                input_desde_archivo(miSocket, sql.strip().split(' ')[1].removesuffix(';'))
                continue
            enviar_mensaje(miSocket, sql)
            respuesta = recibir_mensaje(miSocket)
            if not respuesta:
                break
            imprimirRespuesta(respuesta)
        
        miSocket.close()
        return


    

if __name__ == '__main__':
    main()