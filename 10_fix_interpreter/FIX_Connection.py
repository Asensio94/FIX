import socket
import time
import simplefix
import configparser
from datetime import datetime as dt
import tkinter as tk
from tkinter import ttk
from threading import Thread
from queue import Queue
import re

#change working directory to this file location
import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))

def load_config(config_file):
    # Load the configuration in the config instance.
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read(config_file)

    # Create a dictionary with the configuration options.
    config_data = {
        "sender_comp_id": config.get("DEFAULT", 
                                     "SenderCompID"),
        "target_comp_id": config.get("DEFAULT", 
                                     "TargetCompID"),
        "host": config.get("DEFAULT", "SocketConnectHost"),
        "port": config.getint("DEFAULT", "SocketConnectPort"),
        "heartbeat_interval": config.getint("DEFAULT", 
                                            "HeartBtInt"),
        "username": config.get("LOGIN", "Username"),
        "password": config.get("LOGIN", "Password"),
        "session_start": config.get("SESSION", "Start"),
        "session_end": config.get("SESSION", "End"),
        "timezone": config.get("SESSION", "TimeZone"),
    }

    return config_data

def create_fix_message(msg_type, sender_comp_id, 
                       target_comp_id, 
                       sequence_number,
                       symbols=None,
                       **kwargs):
    # This function creates a new FIX message.
    # It takes as inputs the message type, sender and 
    # target company IDs, sequence number, a list 
    # of symbols.
    # After that, we can specify any number of FIX tags.

    # First of all, we create an empty FIX message.
    # We append the tags that are common to all the
    # FIX messages. These tags are:
    # - 8: FIX version
    # - 9: Body length
    # - 35: Message type
    # - 34: Sequence number
    msg = simplefix.FixMessage()
    msg.append_pair(8, "FIX.4.4")
    msg.append_pair(9, None) # Body length
    msg.append_pair(35, msg_type)
    msg.append_pair(34, sequence_number)
    msg.append_pair(49, sender_comp_id)
    msg.append_pair(56, target_comp_id)
    msg.append_pair(52, dt.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3])

    # After that, we include in the FIX message the tags
    # specified in the 
    # kwargs dictionary.
    for tag, value in kwargs.items():
        msg.append_pair(int(tag[1:]), value) #msg.append_pair(int(tag), value)

    # If we want to send a market data subscription message,
    # we need to include the symbols for which we want to
    # receive market data.
    if symbols and msg_type == "V":
        msg.append_pair(146, str(len(symbols)))  # Añadir el número de símbolos con el tag 146
        for symbol in symbols:
            msg.append_pair(55, symbol)

    # Finally, we need to append the checksum tag (tag 10).
    # This tag is calculated as the modulo 256 sum of 
    # all the characters in the message, excluding the 
    # checksum tag itself.
    # We append it empty and it will be filled afterwards.
    msg.append_pair(10, None)
    return msg


def process_fix_messages(socket, parser, queue):
    buffer = b""
    while True:
        data = socket.recv(4096)
        if not data:
            break

        buffer += data
        parser.append_buffer(buffer)
        buffer = b""

        while True:
            fix_message = parser.get_message()
            if not fix_message:
                break

            print("Received FIX message:", fix_message)
            msg_type = fix_message.get(35)
            if msg_type == b"W":
                bids = []
                asks = []
                i = 0
                while fix_message.get(269, i) is not None:
                    entry_type = fix_message.get(269, i)
                    price = fix_message.get(270, i)
                    size = fix_message.get(271, i)
                    
                    if entry_type == b'0':  # Bid
                        bids.append((float(price), float(size)))
                    elif entry_type == b'1':  # Offer
                        asks.append((float(price), float(size)))

                    i += 1

                print(f"Received Market Data: Bids: {bids}, Asks: {asks}")
                queue.put((bids, asks))


def process_fix_message(fix_message):
    print("Received FIX message:", fix_message)
    msg_type = fix_message.get(35)

    if msg_type == "W":
        bid = fix_message.get(132)
        offer = fix_message.get(133)
        print(f"Received Market Data: Bid: {bid}, Offer: {offer}")


class OrderBookGUI(tk.Tk):
    def __init__(self, queue):
        super().__init__()

        self.title("Order Book")
        self.geometry("300x400")

        self.bid_label = tk.Label(self, text="Bids:", font=("Arial", 14))
        self.bid_label.pack(pady=10)

        self.bid_listbox = tk.Listbox(self, width=30, height=5, font=("Arial", 12))
        self.bid_listbox.pack(pady=10)

        self.ask_label = tk.Label(self, text="Asks:", font=("Arial", 14))
        self.ask_label.pack(pady=10)

        self.ask_listbox = tk.Listbox(self, width=30, height=5, font=("Arial", 12))
        self.ask_listbox.pack(pady=10)

        self.queue = queue
        self.after(100, self.check_queue)

    def check_queue(self):
        while not self.queue.empty():
            msg_type, data = self.queue.get()
            self.update_order_book(data)
        self.after(100, self.check_queue)

    def update_order_book(self, data):
        self.bid_listbox.delete(0, tk.END)
        self.ask_listbox.delete(0, tk.END)

        for price, size in data["bid"], data["bid_size"]:
            self.bid_listbox.insert(tk.END, f"{price} x {size}")

        for price, size in data["ask"], data["ask_size"]:
            self.bid_listbox.insert(tk.END, f"{price} x {size}")

        self.update()


def interpret_fix_messages(socket, parser, queue: Queue):
    while True:
        data = socket.recv(31661)
        if not data:
            print("No data received. Connection closed.")
            break

        parser.append_buffer(data)
        while True:
            fix_message = parser.get_message()
            if not fix_message:
                break

            msg_type = fix_message.get(35)
            
            if msg_type == b"A":
                print("Logon message received.")

            elif msg_type == b"i":
                print("Market Status message received.")
                print(fix_message)
                bid_size = fix_message.get(134)
                ask_size = fix_message.get(135)
                bid = fix_message.get(188)
                ask = fix_message.get(190)
                # Aquí podrías hacer algo con estos datos
                queue.put(("MarketStatus", {'bid': bid, 'bid_size': bid_size, 'ask': ask, 'ask_size': ask_size}))
                print(f"Received Market Status: Bid: {bid}, Bid size: {bid_size}, Ask: {ask}, Ask size: {ask_size}")

            elif msg_type == b"W":
                print("Market Data message received.")
                bids = []
                asks = []
                i = 0
                while True:
                    entry_type = fix_message.get(269, i)
                    if entry_type is None:
                        break

                    price = fix_message.get(270, i)
                    size = fix_message.get(271, i)

                    if entry_type == b'0':  # Bid
                        bids.append((float(price), float(size)))
                    elif entry_type == b'1':  # Offer
                        asks.append((float(price), float(size)))

                    i += 1

                print(f"Received Market Data: Bids: {bids}, Asks: {asks}")
                queue.put(("MarketData", {'bids': bids, 'asks': asks}))

            elif msg_type == b"1":
                print("Heartbeat message received.")
                # Aquí podrías manejar el mensaje de latido si es necesario

            elif msg_type == b"i":
                print("Mass Quote message received.")
                num_quotes = int(fix_message.get(295))
                quotes = []

                i = 0  # Para navegar por el mensaje FIX
                for j in range(num_quotes):  # Para cada grupo de cotización
                    quote_entry = {}
                    quote_entry['quote_id'] = fix_message.get(299, i)
                    quote_entry['bid_size'] = fix_message.get(134, i)
                    quote_entry['bid_price'] = fix_message.get(188, i)
                    quote_entry['offer_size'] = fix_message.get(135, i)
                    quote_entry['offer_price'] = fix_message.get(190, i)

                    quotes.append(quote_entry)
                    i += 1  # Aumenta el índice para la próxima entrada en el mensaje FIX

                print(f"Received Mass Quote: {quotes}")
                queue.put(("MassQuote", {'quotes': quotes}))
                
            else:
                print(f"Unhandled message type: {msg_type}")

def parse_mass_quote(liquidity_book, fix_message):
    # \x01 is the delimiter for FIX messages in UTF-8 = | in ASCII.
    symbol_chunks = re.split(r'(302=[^\x01]+\x01)', fix_message.encode().decode('utf-8'))

    for i in range(1, len(symbol_chunks), 2):
        
        level_content_all = symbol_chunks[i] + symbol_chunks[i+1]
        level_chunks = re.split(r'(299=\d\x01)', level_content_all)
        
        for i in range(1, len(level_chunks), 2):
            level_content = level_chunks[i] + level_chunks[i+1]
            
            level_match = re.search(r'299=([\d.]+)\x01', level_content)
            level = int(level_match.group(1))
            
            bid_match = re.search(r'188=([-?\d.]+)\x01', level_content)
            ask_match = re.search(r'190=([-?\d.]+)\x01', level_content)
            bid_size_match = re.search(r'134=([-?\d.]+)\x01', level_content)
            ask_size_match = re.search(r'135=([-?\d.]+)\x01', level_content)
            
            if bid_match:
                liquidity_book[level]['Bid'] = float(bid_match.group(1))
            if ask_match:
                liquidity_book[level]['Ask'] = float(ask_match.group(1))
            if bid_size_match:
                size = float(bid_size_match.group(1))
                if size < 0:  # quote cancelled.
                    size = 0
                    liquidity_book[level]['Bid'] = 0
                liquidity_book[level]['Bid Size'] = size
            if ask_size_match:
                size = float(ask_size_match.group(1))
                if size < 0:  # quote cancelled.
                    size = 0
                    liquidity_book[level]['Ask'] = 0
                liquidity_book[level]['Ask Size'] = size
    return liquidity_book

def main():
    # Load the config using the configparser library and the file "myconfig.cfg".
    config_file = "myconfig.cfg"
    config = load_config(config_file)

    # Socket connection: it makes a bidirectional connection to the server.
    # The socket is used to send and receive FIX messages.
    # By passing the arguments socket.AF_INET, we are saying that we want to use IPv4.
    # By passing the arguments socket.SOCK_STREAM, we are saying that we want to use TCP.
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Connect to the server using the host and port specified in the config file.
    s.connect((config["host"], config["port"]))

    # Create the logon message.
    # Type A message (1st argument) is a logon message.
    # The sender and target company IDs are specified in the config file.
    # The sender company ID is the ID of the client. It is a unique ID.
    # The target company ID is the ID of the server. It is a unique ID.
    # After the sequence number (1), we can specify any number of FIX tags.
    # In this case we are specifying the encryption method, the heartbeat 
    # interval (108), the username (553) and the password (554).
    logon_message = create_fix_message("A", 
                                       config["sender_comp_id"], 
                                       config["target_comp_id"], 
                                       1, 
                                       _98='0', # No encryption
                                       _108=config["heartbeat_interval"],
                                       _141='Y', # Reset the market data book
                                       _553= config["username"], 
                                       _554= config["password"])
    
    # Send the logon message to the server.
    s.sendall(logon_message.encode())

    # Create the market data subscription message.
    # It must be a list of symbols.
    symbols = ['EUR/USD', ]
    number_of_symbols = len(symbols)
    # Subscription request type (263) = 1 (snapshot + updates)
    # This means that we want to subscribe to updates 
    # for the specified symbol.
    subscription_request_type = "1"
    # Market data request ID (262) = 1000
    # This is a unique ID for the market data request.
    # This asures that we can identify the market data request.
    market_data_request_id = 1000

    # Create the market data subscription message.
    # V is the message type for market data subscription.
    # Tag 141 specifies if we want to reset the market data book.
    # Tag 264 specifies the depth of the book.
    # Tag 267 specifies the type of book. 2 means that 
    # we want to receive Bid and ASK quotes.
    # Tag 146 specifies the number of symbols for which
    # we want to receive market data.
    # Tag 55 specifies the symbol for which we want to
    # receive market data.

    market_data_subscription_msg = create_fix_message(
        "V",
        config["sender_comp_id"],
        config["target_comp_id"],
        market_data_request_id,
        symbols=symbols,
        _141='N', # Reset the market data book
        _262=market_data_request_id,
        _263=subscription_request_type,
        _264='5', # Depth of the book
        _267='2', # Bid and Ask
        # _146=str(number_of_symbols),
    )
    s.sendall(market_data_subscription_msg.encode())
    
    # Create a queue to store the market data.
    # This queue allows the comunication between different 
    # threads. It is passed to the GUI to access to the 
    # messages that are queued
    # FixParser is a class from the simplefix library that
    # parses the FIX messages.

    queue = Queue()
    gui = OrderBookGUI(queue)
    parser = simplefix.FixParser()
    
    # Create a thread to process the FIX messages.
    # We specified that the function process_fix_messages
    # will be the responsible for processing the messages.
    # Besides, we pass the socket, the parser and the queue.
    thread = Thread(target=interpret_fix_messages, args=(s, parser, queue))
    # We set the thread as a daemon thread. This means that
    # the thread will be closed when all the no daemon threads
    # are closed.
    thread.daemon = True
    # We start the thread.
    thread.start()

    gui.mainloop()

    s.close()


if __name__ == "__main__":
    main()
