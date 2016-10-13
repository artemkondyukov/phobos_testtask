from flask import Flask, request
from utils.prime_numbers import *
from utils.factorize import *
from utils.pinger import *

app = Flask(__name__)
prime_numbers = PrimeNumbers()
factorizer = Factorizer(prime_numbers)
pinger = Pinger()


@app.route('/primes/<int:number>', methods=['POST'])
def set_calculation(number):
    """
    Processes request of client and returns unique token for the request
    :param number: counting order of prime number to calculate
    :return: response with token
    """
    token = prime_numbers.get_token(number=number)
    return str(token)


@app.route('/factorize/<int:number>', methods=["POST"])
def factorize(number):
    """
    Processes request of client and returns unique token for the request
    :param number: a number to factorize
    :return: response with token
    """
    token = factorizer.get_token(number=number)
    return str(token)


@app.route('/ping/<string:server>/<int:number>', methods=["POST"])
def ping(server, number):
    """
    Pings the specified server _number_ times
    :param server: string
    :param number: int
    :return: response
    """
    token = pinger.get_token(server=server, number=number)
    return str(token)


@app.route('/result/<token>', methods=["GET"])
def get_result(token):
    """
    Checks whether the calculations for the token given finished and returns result
    :param token: int token obtained from set_calculation
    :return: depends on the task assigned to the token given
    """
    processers = [prime_numbers, factorizer, pinger]
    for processer in processers:
        if processer.is_token_in_db(token):
            result = processer.get_token_result(token)
            if result is None:
                return "Your request is still processing."
            else:
                return str(result)
    return "Invalid token"

if __name__ == "__main__":
    app.run()
