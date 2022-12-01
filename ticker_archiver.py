# Ticker Archiver
# Constantly streams exchange data from Binance and writes to a .csv file
import io

# Libraries
import unicorn_binance_websocket_api
import argparse
import datetime as dt
import os


# Functions
def create_or_append_file(filename: str) -> io.TextIOWrapper:
    """ Opens specified file in append mode if found, creates a file with OCHLV headers if not found

    :param filename: The name of the file
    :return: The file handle to the file
    :rtype: io.TextIOWrapper
    """
    if filename not in os.listdir():
        file = open(filename, "x")
        file.write("Date,Open,Close,High,Low,Volume\n")
        return file
    else:
        return open(filename, "a")


# Get ticker symbol
parser = argparse.ArgumentParser()
parser.add_argument("-t", "--ticker", help="The ticker symbol", type=str)

args = parser.parse_args()
ticker = args.ticker

# Init websocket stream thingy
ubwa = unicorn_binance_websocket_api.BinanceWebSocketApiManager(exchange="binance.com")
ubwa.create_stream(["kline_1s"], ticker, output="dict")

while True:
    # Get data from buffer
    data = ubwa.pop_stream_data_from_stream_buffer()
    if data:
        if "data" in data.keys():
            # If data is successfully returned
            ticker_data = data["data"]["k"]

            # Open the file
            f = create_or_append_file(ticker + ".csv")

            # Convert timestamp to human readable date
            # https://stackoverflow.com/questions/37494983/python-fromtimestamp-oserror
            timestamp = ticker_data["t"] / 1000
            date = dt.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")

            # Extract date with OCHLV data
            # Docs: https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md
            to_write = [
                date,
                ticker_data["o"],
                ticker_data["c"],
                ticker_data["h"],
                ticker_data["l"],
                ticker_data["q"],
            ]

            # Convert to_write to string and format into valid .csv format
            to_write = [str(x) for x in to_write]
            to_write = ",".join(to_write)

            f.write(to_write + "\n")
            f.close()

            print("[SUCCESS] Written data to: " + ticker + ".csv!")

        elif "result" in data.keys():
            # Usually means empty buffer or invalid ticker symbol
            print("[ERROR] Error received! Maybe check if your ticker symbol exists?")

        else:
            # Bruh u screwed up lmao
            print("[ERROR] Unable to locate key \"data\"")
