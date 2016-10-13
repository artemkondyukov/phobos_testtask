import random
import sqlite3


class Processor:
    def __init__(self):
        self.database_name = None
        self.token_table_name = ""
        self.token_prefix = ""

    def new_token_name(self):
        conn = sqlite3.connect(self.database_name)
        cursor = conn.cursor()
        token = self.token_prefix + str(random.randint(100000, 999999))  # Replace it with something more serious
        result = cursor.execute("SELECT * FROM %s WHERE token='%s';" % (self.token_table_name, token)).fetchone()

        while result is not None:
            token = self.token_prefix + str(random.randint(100000, 999999))  # Replace it with something more serious
            result = cursor.execute("SELECT * FROM %s WHERE token='%s';" % (self.token_table_name, token)).fetchone()

        cursor.close()
        conn.close()
        return token

    def is_token_in_db(self, token):
        """
        Checks whether a task has been assigned to the token given
        :param token: string, unique token
        :return: bool
        """
        conn = sqlite3.connect(self.database_name)
        cursor = conn.cursor()
        token_data = cursor.execute("SELECT * FROM %s WHERE token='%s'" % (self.token_table_name, token)).fetchone()
        cursor.close()
        conn.close()
        if token_data is None:
            return False
        return True

    def get_token(self, **kwargs):
        raise NotImplementedError

    def get_token_result(self, token):
        raise NotImplementedError
