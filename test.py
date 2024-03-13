import random
import time

if __name__ == '__main__':

    random.seed(42)


    s1 = time.time()

    for i in range(0, 100):
        print(random.randint(0, 16000))
