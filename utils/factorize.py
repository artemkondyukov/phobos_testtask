from concurrent.futures import ThreadPoolExecutor
from functools import partial

from utils.processor import *

CONCURRENCY = 4


class Factorizer(Processor):
    def __init__(self, prime_numbers):
        super().__init__()
        # Here we don't know in advance number of threads, so it's better to use pool
        self.executor = ThreadPoolExecutor(CONCURRENCY)
        self.database_name = "factorize.db"
        self.token_table_name = "factorizations"
        self.token_prefix = "f"
        self.prime_numbers = prime_numbers

        conn = sqlite3.connect(self.database_name)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS factorizations (token VARCHAR(20) UNIQUE, factorization TEXT);")
        conn.commit()

    def _factorize(self, token, number):
        """
        Do actual purpose of this class in the brute-force manner. Must be optimized.
        :param number: a number to factorize
        :return: list of integers
        """
        i = 2
        arr = []

        while i < number ** (1 / 2) + 1:
            while number % i == 0:
                number //= i
                arr.append(str(i))
            i = self.prime_numbers.next_prime(i)
        arr.append(str(number))

        result = " ".join(arr)
        conn = sqlite3.connect(self.database_name)
        cursor = conn.cursor()
        cursor.execute("UPDATE factorizations SET factorization='%s' WHERE token='%s';" % (result, token))
        conn.commit()
        cursor.close()
        conn.close()

    def get_token(self, **kwargs):
        """
        Wrapper for encapsulated function _factorize
        :return: unique token
        """
        if "number" not in kwargs:
            raise ValueError("The value to factorize must be specified")
        number = kwargs["number"]
        token = self.new_token_name()
        conn = sqlite3.connect(self.database_name)
        cursor = conn.cursor()
        self.executor.submit(partial(self._factorize, token=token, number=number))

        cursor.execute("INSERT INTO factorizations (token) VALUES ('%s');" % token)
        conn.commit()
        cursor.close()
        conn.close()
        return token

    def get_token_result(self, token):
        """
        finds entry for the token given
        :param token: a unique token given to the client
        :return: the factorization of the number given if it has been calculated, None otherwise
        """
        if not self.is_token_in_db(token):
            raise ValueError("The token given is not in database")

        conn = sqlite3.connect(self.database_name)
        cursor = conn.cursor()
        token_data = cursor.execute("SELECT * FROM factorizations WHERE token='%s';" % token).fetchone()
        result = token_data[1]
        cursor.close()
        conn.close()
        return result
