import threading

from utils.processor import *


class PrimeNumbers(Processor):
    def __init__(self):
        super().__init__()
        self.batch_size = 100
        self.database_name = "primes.db"
        self.token_prefix = "p"
        self.token_table_name = "evaluated"

        conn = sqlite3.connect(self.database_name)
        cursor = conn.cursor()

        cursor.execute("CREATE TABLE IF NOT EXISTS primes (id INTEGER UNIQUE, value BLOB UNIQUE);")
        cursor.execute("CREATE TABLE IF NOT EXISTS evaluated "
                       "(token VARCHAR(20) UNIQUE, prime_id INTEGER, evaluated BOOLEAN);")
        cursor.execute("INSERT OR REPLACE INTO primes (id, value) VALUES (1, 2);")
        conn.commit()

        self.alive = True
        self.max_index = -1
        self.max_index_changed = threading.Event()

        self.background_thread = threading.Thread(target=self.calculate_primes, args=(self.max_index_changed,))
        self.background_thread.start()

        cursor.close()
        conn.close()

    def _insert_primes_array(self, arr):
        """
        Opens connection to the DB and puts calculated primes there
        :param arr: list of tuples (id, value)
        :return: None
        """
        if len(arr) == 0:
            return

        conn = sqlite3.connect(self.database_name)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO primes (id, value) VALUES " +
                       ", ".join(["(%d, %d)" % (num, val) for num, val in arr]) + ";")
        cursor.execute("UPDATE evaluated SET evaluated=1 WHERE prime_id IN (SELECT id FROM primes);")
        conn.commit()
        cursor.close()
        conn.close()

    def _get_prime(self, number):
        """
        Uses brute force for evaluation.
        :param number: index
        :return: prime number
        """
        conn = sqlite3.connect(self.database_name)
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM primes WHERE id = %d;" % number)
        result = cursor.fetchone()

        if result is not None:
            cursor.close()
            conn.close()
            return result[0]

        curr_ind, curr_num = cursor.execute(
            "SELECT * FROM primes WHERE id = (SELECT MAX(id) FROM primes);"
        ).fetchone()
        cursor.close()
        conn.close()

        tmp_primes = []
        for i in range(curr_ind, number + 1):
            curr_ind += 1
            curr_num = self.next_prime(curr_num)
            tmp_primes.append((curr_ind, curr_num))

            # DB connect costs a lot, so do it rarely
            if i % self.batch_size == 0:
                self._insert_primes_array(tmp_primes)
                tmp_primes = []

        self._insert_primes_array(tmp_primes)

    def calculate_primes(self, event):
        """
        Background function which starts after event has been set
        :param event: threading.Event. It should be set when self.max_index has been changed
        :return: None
        """
        while self.alive:
            event.wait()
            event.clear()
            current_max_index = self.max_index
            self._get_prime(current_max_index)
            # If max_index changed since evaluation started, restart it.
            while current_max_index < self.max_index:
                self._get_prime(current_max_index)

    def next_prime(self, prev):
        """
        Sequentially go from the number given an test whether the current number is prime or not
        :param prev: number to start
        :return: the next prime number
        """
        if prev == 2:
            current = 3
        else:
            current = prev + 2
        i = 2
        while self.alive:
            if current % i == 0 and i != current:
                current += 2
                i = 2
                continue
            if i == current or i > current ** (1 / 2) + 1:
                break

            # should optimize it by storing part of DB in memory and using only prime divisors
            i += 1

        return current

    def get_token(self, **kwargs):
        """
        Wrapper for encapsulated function _get_prime
        :param number: index
        :return: unique token
        """
        if "number" not in kwargs:
            raise ValueError("Order number must be specified")
        number = kwargs["number"]
        token = self.new_token_name()
        conn = sqlite3.connect(self.database_name)
        cursor = conn.cursor()

        if self.max_index < number:
            self.max_index = number
            if not self.max_index_changed.is_set():
                self.max_index_changed.set()

        prime_needed = cursor.execute("SELECT id FROM primes WHERE id=%d" % number).fetchone()
        is_prime_calculated = 1 if prime_needed is not None else 0

        cursor.execute("INSERT INTO evaluated (token, prime_id, evaluated) "
                       "VALUES ('%s', %d, %d)" % (token, number, is_prime_calculated))
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
        token_data = cursor.execute("SELECT * FROM evaluated WHERE token='%s';" % token).fetchone()
        token, prime_id, evaluated = token_data
        if evaluated:
            result = cursor.execute("SELECT value FROM primes WHERE id=%d;" % prime_id).fetchone()[0]
        else:
            result = None
        cursor.close()
        conn.close()
        return result

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.alive = False
        self.background_thread.join()
