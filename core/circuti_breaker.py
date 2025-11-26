import time 


class circuit_breaker:
    def __init__(self, max_failures = 5 , reset_timeout= 30):
        self.max_failures =  max_failures
        self.reset_time = reset_timeout
        self.state = "CLOSED"
        self.failure_count = 0 
        self.last_failure_time = 0 

    
    def allow_request(self):
        if self.state == "OPEN":
            if time.time() - self.last_failure_time >= self.reset_time:
                self.state = "HALF OPEN"
                return True
            return False 
        return True
    
    def success(self):
        self.state = "CLOSED "
        self.failure_count = 0 

    def on_failure(self):
        self.failure_count += 1 
        self.last_failure_time = time.time()

        if self.failure_count >= self.max_failures:
            self.state = "OPEN"
            
class circuit_breaker_open_error(Exception):
    pass