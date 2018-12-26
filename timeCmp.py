from tornado import ioloop
from tornado import gen, web
import datetime
from datetime import timedelta

start = 22
end = 2

csv_files = ['leads_part_1.csv', 'leads_part_2.csv', 'leads_part_3.csv',
             'leads_part_4.csv', 'leads_part_5.csv', 'leads_part_6.csv',
             'leads_part_7.csv', 'leads_part_8.csv', 'leads_part_9.csv']

def hour_in_range(start, end, x):
    """Return true if x is in the range [start, end]"""
    if start <= end:
        return start <= x < end
    else:
        return start <= x or x < end

@gen.coroutine
def wake_scheduler():
    loop = ioloop.IOLoop.current()
    #ZMQ SUB
    portSUB = "8003"
    # Socket to talk to server
    while True:
        print(hour_in_range(start, end, 22))
        print(hour_in_range(start, end, 2))
        print(hour_in_range(start, end, 24))
        print(hour_in_range(start, end, 10))
        print(hour_in_range(start, end, 0))

        print(hour_in_range(start, end, datetime.datetime.now().hour))
        print("*********")
        yield gen.Task(loop.add_timeout, timedelta(seconds=10))


def main():
    print("Current time: {}".format(datetime.datetime.now().hour))
    #wake_subscriber2()
    ioloop.IOLoop.current().start()

if __name__ == '__main__':
    for i in range(1,10):
        while True:
            print("perro {}".format(i))
            break


    main()