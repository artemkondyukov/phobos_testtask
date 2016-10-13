from concurrent.futures import ThreadPoolExecutor
from functools import partial
import os

from utils.processor import *

CONCURRENCY = 4


class Pinger(Processor):
    def __init__(self):
        super().__init__()
        self.database_name = "pinger.db"
        self.token_prefix = "g"
        self.token_table_name = "pings"

        self.executor = ThreadPoolExecutor(CONCURRENCY)

        conn = sqlite3.connect(self.database_name)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS pings (token VARCHAR(20) UNIQUE, response TEXT);")
        conn.commit()
        cursor.close()
        conn.close()

    def _ping(self, token, server, N):
        """
        Uses standard utils so as to ping the specified server
        :param server: string
        :param N: -n parameter for ping
        :return: output of the ping util
        """
        response = []
        for i in range(N):
            response.append(str(os.system("ping -c 1 %s" % server)))
        result = " ".join(response)
        conn = sqlite3.connect(self.database_name)
        cursor = conn.cursor()
        cursor.execute("UPDATE pings SET response='%s' WHERE token='%s';" % (result, token))
        conn.commit()
        cursor.close()
        conn.close()

    def get_token(self, **kwargs):
        """
        Wrapper for encapsulated function _ping
        :return: unique token
        """
        if "number" not in kwargs or "server" not in kwargs:
            raise ValueError("Not enough parameters")

        N = kwargs["number"]
        server = kwargs["server"]

        token = self.new_token_name()
        conn = sqlite3.connect(self.database_name)
        cursor = conn.cursor()
        self.executor.submit(partial(self._ping, token=token, server=server, N=N))

        cursor.execute("INSERT INTO pings (token) VALUES ('%s');" % token)
        conn.commit()
        cursor.close()
        conn.close()
        return token

    def get_token_result(self, token):
        """
        finds entry for the token given
        :param token: a unique token given to the client
        :return: the prime number asked if it has been calculated, None otherwise
        """
        if not self.is_token_in_db(token):
            raise ValueError("The token given is not in database")

        conn = sqlite3.connect(self.database_name)
        cursor = conn.cursor()
        token_data = cursor.execute("SELECT * FROM pings WHERE token='%s';" % token).fetchone()
        result = token_data[1]
        cursor.close()
        conn.close()
        return result
