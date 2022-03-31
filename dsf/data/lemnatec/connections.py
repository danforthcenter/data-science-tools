import paramiko
import psycopg2
import psycopg2.extras


def open_sftp_connection(config):
    """Open an SFTP connection to the database server.

    Keyword arguments:
    config = Instance of the class Config.

    Returns:
    sftp = paramiko SFTP connection object.

    :param config: dsf.data.lemnatec.config.Config
    :return sftp: paramiko.sftp_client.SFTPClient
    """
    # SSH connection client
    ssh = paramiko.SSHClient()
    # Set missing host key policy
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # Connect to the database server
    ssh.connect(config.hostname, username='root', password=config.password)
    sftp = ssh.open_sftp()

    return sftp


def open_database_connection(config):
    """Open a database connection cursor.

    Keyword arguments:
    config = Instance of the class Config.

    Returns:
    db = Database connection cursor.

    :param config: dsf.data.lemnatec.config.Config
    :return db: psycopg2.extras.DictCursor
    """
    # Connect to the LemnaTec database
    conn = psycopg2.connect(host=config.hostname,
                            user=config.username,
                            password=config.password,
                            database=config.database)
    db = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    return db
