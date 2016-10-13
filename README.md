### Phobos company test task solution.  

This project is a toy-webserver with the following functionality:  
1. calculating the value of Nth prime number  
2. factorizing numbers  
3. ping servers  

This server is based on Flask microframework  

### Install  
`git clone https://github.com/artemkondyukov/phobos_testtask`  
`cd phobos_testtask`  
`pip install -r requirements.txt`  
### Usage
`python main.py`  
It will start a local server on your machine. So as to test functions of the server you will need a way to send requests. Using cURL it looks like:  
`curl -X POST http://localhost:5000/primes/100000`  
This request creates a task for finding the 100.000th prime number. Because of non-optimal solution and general high-complexity of this problem, it can take a large amount of time. However you do not have to wait and can send several other requests, e.g.:  
`curl -X POST http://localhost:5000/factorize/123456`  
This request creates a task for finding all divisors of the given number. Clearly, this task depends on previous, so it also can take lots of time. One more request type is:  
`curl -X POST http://localhost:5000/ping/google.com/10`  
This request creates a taks for pinging _google.com_ 10 times.  

As a response for any kind of request you get a token, i.e. a char sequence looking like *p123877*. You need it to get result for your previous requests:  
`curl http://localhost:5000/result/p123877`  
If your task has been completed, you will get estimated response, i.e. for the first request you'll get:
`1299709`  
For the second request:
`2 2 2 2 2 2 3 643`  
and for the third:
`0 0 0 0 0 0 0 0 0 0` in case of 10 successful pings or a combination of zeros and ones in case of unsuccessful attempts
