#!/usr/bin/env python
# Takes large chunks of code from
# the Segment Python logging library: https://github.com/segmentio/analytics-python
# and this post on Stack overflow for reading the command line: https://stackoverflow.com/questions/11109859/pipe-output-from-shell-command-to-a-python-script
# MIT License
from uuid import uuid4
from datetime import datetime
from threading import Thread
import logging
import sys
import time
import requests
import atexit
try:
    import queue
    from queue import Empty
except:
    import Queue as queue
    from Queue import Empty

REMOTE_URL = 'https://requestb.in/wobde5wo'

class Client(object):
    logger = logging.getLogger('stream-logger')

    def __init__(self, max_queue_size=10000, host=None):
        self.queue = queue.Queue(max_queue_size)
        self.consumer = Consumer(self.queue, host=host, headers=None)
        self.input_stream = None
        self.logger.setLevel(logging.DEBUG)
        # self.logger.addHandler(logging.StreamHandler())
        self.logger.info("------ first post")
        # On program exit, allow the consumer thread to exit cleanly.
        # This prevents exceptions and a messy shutdown when the interpreter is
        # destroyed before the daemon thread finishes execution. However, it
        # is *not* the same as flushing the queue! To guarantee all messages
        # have been delivered, you'll still need to call flush().
        atexit.register(self.join)
        self.consumer.start()
        self.open_stream()
        self.run()

    def open_stream(self):
        # Modifed source from https://stackoverflow.com/questions/11109859/pipe-output-from-shell-command-to-a-python-script
        if not sys.stdin.isatty():
            # use stdin if it's full
            self.input_stream = sys.stdin
        else:
            # otherwise, read the given filename
            try:
                input_filename = sys.argv[1]
            except IndexError:
                message = 'need filename as first argument if stdin is not full'
                raise IndexError(message)
            else:
                self.input_stream = open(input_filename, 'rU')

    def run(self):
        inputEmptyCount = 0
        try:
            while True:
                line = self.input_stream.readline()
                if line:
                    self.log(line)
                    inputEmptyCount = 0
                if not self.input_stream.isatty():
                    inputEmptyCount += 1
                    if inputEmptyCount > 2:
                        self.logger.info("EMPTY COUNT")
                        break
        except Exception as e:
            self.logger.error(e)
        finally:
            self.flush()
            self.logger.info("EXITING")

    def log(self, msg):
        """Push a new `msg` onto the queue, return `(success, msg)`"""
        try:
            self.queue.put(msg, block=False)
            self.logger.info('enqueued message. %s' % msg)
            return True, msg
        except queue.Full:
            self.log.warn('analytics-python queue is full')
            return False, msg

    def flush(self):
        """Forces a flush from the internal queue to the server"""
        queue = self.queue
        size = queue.qsize()
        queue.join()
        # Note that this message may not be precise, because of threading.
        self.logger.info('successfully flushed about %s items.', size)

    def join(self):
        """Ends the consumer thread once the queue is empty. Blocks execution until finished"""
        self.consumer.pause()
        try:
            self.consumer.join()
        except RuntimeError:
            # consumer thread has not started
            pass

class Consumer(Thread):
    """Consumes the messages from the client's queue."""
    logger = logging.getLogger('stream-logger')

    def __init__(self, queue, host=None, upload_size=100, headers=None):
        """Create a consumer thread."""
        Thread.__init__(self)
        # Make consumer a daemon thread so that it doesn't block program exit
        self.daemon = True
        self.upload_size = upload_size
        self.host = host
        self.headers = headers
        self.queue = queue
        # It's important to set running in the constructor: if we are asked to
        # pause immediately after construction, we might set running to True in
        # run() *after* we set it to False in pause... and keep running forever.
        self.running = True
        self.retries = 3
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(logging.StreamHandler())
        self.logger.info("HELPPP!!!!")

    def run(self):
        """Runs the consumer."""
        self.logger.info('consumer is running...')
        while self.running:
            self.upload()

        self.logger.info('consumer exited.')

    def pause(self):
        """Pause the consumer."""
        self.running = False

    def upload(self):
        """Upload the next batch of items, return whether successful."""
        success = False
        batch = self.next()
        if len(batch) == 0:
            return False

        try:
            self.logger.debug('going to request!')
            self.request(batch)
            success = True
        except Exception as e:
            self.logger.error('error uploading: %s', e)
            success = False
            if self.on_error:
                self.on_error(e, batch)
        finally:
            # mark items as acknowledged from queue
            for item in batch:
                self.queue.task_done()
            return success

    def next(self):
        """Return the next batch of items to upload."""
        queue = self.queue
        items = []

        while len(items) < self.upload_size:
            try:
                item = queue.get(block=True, timeout=0.5)
                items.append(item)
            except Empty:
                break

        return items

    def request(self, batch, attempt=0):
        """Attempt to upload the batch and retry before raising an error """
        msg = {}
        timestamp = msg.get('timestamp')
        if timestamp is None:
            timestamp = datetime.utcnow()
        msg['timestamp'] = timestamp.isoformat()
        msg['messageId'] = str(uuid4())
        msg['value'] = ":::".join(batch)
        try:
            # post(self.write_key, self.host, batch=batch)
            self.logger.info("-------")
            self.logger.info(msg)
            self.logger.info(self.host)
            self.logger.info(self.headers)
            r = requests.post(
                self.host,
                headers=self.headers,
                json=msg
            )
            if r.status_code >= 200 and r.status_code < 300:
                self.logger.debug('data uploaded successfully')
                return r
            else:
                self.logger.debug('non 2XX response')
                self.logger.debug(r)
        except:
            if attempt > self.retries:
                raise
            self.request(batch, attempt+1)


client = Client(host=REMOTE_URL)
