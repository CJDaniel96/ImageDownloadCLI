from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import time


def create_session(ssh_tunnel: dict, database: dict) -> sessionmaker:
    ssh_address_or_host = ssh_tunnel['ssh_address_or_host']
    ssh_username = ssh_tunnel['ssh_username']
    ssh_password = ssh_tunnel['ssh_password']
    
    db_engine = database['ENGINE']
    db_name = database['NAME']
    db_user = database['USER']
    db_password = database['PASSWORD']
    db_host = database['HOST']
    db_port = database['PORT']
    
    if ssh_address_or_host and ssh_username and ssh_password:
        server = SSHTunnelForwarder(
            ssh_address_or_host=(ssh_address_or_host, 22),
            ssh_username=ssh_username,
            ssh_password=ssh_password,
            remote_bind_address=(db_host, db_port)
        )
        server.start()
        local_host = '127.0.0.1'
        local_port = server.local_bind_port
    else:
        local_host = db_host
        local_port = db_port
        
    time.sleep(3)

    engine_url = f'{db_engine}://{db_user}:{db_password}@{local_host}:{local_port}/{db_name}'
    engine = create_engine(engine_url, echo=False)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    session = Session()

    return session