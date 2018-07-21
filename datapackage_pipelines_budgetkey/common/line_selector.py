class LineSelector():

    def __init__(self):
        self.x = 1
    
    def __call__(self, i):
        i += 1
        if i - self.x > 10:
            self.x *= 10
        return 0 <= i - self.x <= 10
