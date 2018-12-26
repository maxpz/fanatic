from datetime import timedelta

from tornado import gen, ioloop


@gen.coroutine
def broadcast():
    loop = ioloop.IOLoop.current()
    while True:
        print "broadcast"
        #for c in webSocketConnectionList:
            #c.write_message("message")

        yield gen.Task(loop.add_timeout, timedelta(seconds=1))


def main():
    broadcast()
    print "ak7"
    ioloop.IOLoop.current().start()

if __name__ == "__main__":
    #broadcast()
    #print "Aqui ak7"
    #ioloop.IOLoop.current().start()
    main()
