import json
import firebird.driver as fdb
from sys import argv
import socket
import threading
import struct
import random

archivo_servidores = './servidores.json'
servers = {}
IP = '192.168.0.10'
PUERTO = 3060
IND_SERV_ACTUAL = 0
CANT_SERV = 0

def cargar_servers():
    try:
        with open(archivo_servidores, 'r') as server_info:
            global servers 
            servers = dict(json.load(server_info)['servers'])
            global CANT_SERV
            CANT_SERV = len(servers)
    except FileNotFoundError:
        print("No se encontro el archivo ", archivo_servidores)

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

def getRandomServ(conexiones: list) -> int:
    return random.randint(0, len(conexiones)-1)

def ejecutar_select(sCliente: socket.socket, conexiones: list, sql: str):
    print("Ejecutar_Select")
    con = conexiones[getRandomServ(conexiones)]
    print(con)
    try:    
        cur = con.cursor()
        cur.execute(sql)
        respuesta = {}
        columnas = [col[0] for col in cur.description]
        filas = cur.fetchall()
        respuesta['columnas'] = columnas
        respuesta['filas'] = filas
        respuesta['filas_afectadas'] = cur.affected_rows
        print("filas ", filas)
        print("col", columnas)
        print("Filas_af", respuesta['filas_afectadas'])
        respuesta_json = json.dumps(respuesta)
        enviar_mensaje(sCliente, respuesta_json)
    except Exception as e:
        enviar_mensaje(sCliente, f'Error: {e}')
    finally:
        cur.close()

def prepare_transaction(conexiones, sql) -> bool:
    try:
        for con in conexiones:
            con.begin()
            cur = con.cursor()
            cur.execute(sql)
        print("TRANSACCION PREPARADA")
        return True
    except:
        print("error prepare trasaction")
        return False
    
def commit_prepared(conexiones):
    for con in conexiones:
        con.commit()

def rollback_prepared(conexiones):
    for con in conexiones:
        con.rollback()

def ejecutar_query(sCliente:socket.socket, conexiones: list, sql: str):
    if prepare_transaction(conexiones, sql):
        print("EMPIEZO EL COMMIT")
        cur = conexiones[0].cursor()
        respuesta = {}
        columnas = []
        filas = ()
        respuesta['columnas'] = columnas
        respuesta['filas'] = filas
        respuesta['filas_afectadas'] = 0
        print("ME VOY A COMITEAR----------------")
        commit_prepared(conexiones)
        respuesta_json = json.dumps(respuesta)
        enviar_mensaje(sCliente, respuesta_json)
    else:
        rollback_prepared(conexiones)
        enviar_mensaje(sCliente, 'ERROR: no se pudo completar la transaccion')


def obtenerUserPassw(msg:str) -> tuple:
    return tuple(msg.split('|'))

def conectarCliente(user:str, passw:str) -> list | None:
    
    conexiones = []
    try:
        for serv_name in servers:
            print(f'{servers[serv_name]['host']}/{servers[serv_name]['puerto']}:/{servers[serv_name]['path']}', user, passw)
            con = fdb.connect(
                database=f'{servers[serv_name]['host']}/{servers[serv_name]['puerto']}:{servers[serv_name]['path']}', 
                user= user,
                password= passw
            )
            conexiones.append(con)
        
        print("conexiones cargadas con exito")
        return conexiones
    except:
        print("Error al cargar las conexiones")
        for conec in conexiones:
            conec.close()
        return None


def manejar_cliente(sCliente: socket.socket):
    msg = recibir_mensaje(sCliente)
    if msg == None:
        sCliente.close()
        return
    user, passw = obtenerUserPassw(msg)
    conexiones = conectarCliente(user, passw)
    if conexiones == None:
        enviar_mensaje(sCliente, 'ERROR: El usuario o contraseña no estan definidos. Conluta con el administrador de base de datos')
        sCliente.close()
        return
    else:
        enviar_mensaje(sCliente, 'Cliente Conectado Con exito')
    while True:
        msg = recibir_mensaje(sCliente)
        print(msg)
        if msg == None:
            for con in conexiones:
                con.close()
            sCliente.close()
            break
        print(conexiones)
        if msg.upper().startswith('SELECT'):
            ejecutar_select(sCliente, conexiones, msg)
        else:
            ejecutar_query(sCliente, conexiones, msg)

def main():
    cargar_servers()
    print("servidores Cargados")
    print(servers)
    socket_middleware = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    socket_middleware.bind((IP, PUERTO))
    socket_middleware.listen()
    print(f'Servidores escuchando en {IP}:{PUERTO}')

    try:
        while True:
            sCliente, dirCliente = socket_middleware.accept()
            print(f"conexion aceptada en {dirCliente[0]}:{dirCliente[1]}")
            manejar_cliente(sCliente)
    except Exception as e:
        print("exception: ", e)
    finally:
        socket_middleware.close()



    
if __name__ == '__main__':
    main()