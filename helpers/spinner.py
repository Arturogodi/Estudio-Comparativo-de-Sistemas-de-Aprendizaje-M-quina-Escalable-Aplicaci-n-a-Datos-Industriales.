# helpers/spinner.py
import sys
import threading
import time


class Spinner:
    def __init__(self, message="Procesando..."):
        self.stop_running = False
        self.thread = None
        self.message = message

    def start(self):
        def spin():
            symbols = ['|', '/', '-', '\\']
            idx = 0
            while not self.stop_running:
                sys.stdout.write(f"\r{self.message} {symbols[idx % len(symbols)]}")
                sys.stdout.flush()
                idx += 1
                time.sleep(0.2)
            sys.stdout.write("\r")  # Limpia línea

        self.thread = threading.Thread(target=spin)
        self.thread.start()

    def stop(self):
        self.stop_running = True
        self.thread.join()
        print(f"\r{self.message} ✅")
