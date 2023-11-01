import streamlit as st
import re
from datetime import datetime

def parse_fix_log_v9(file_path, target_symbol, target_time):
    liquidity_book = {level: {'Bid': 0, 'Ask': 0, 'Bid Size': 0, 'Ask Size': 0} for level in range(5)}
    target_time_obj = datetime.strptime(target_time, '%Y-%m-%d %H:%M:%S.%f')

    with open(file_path, 'r') as file:
        for line in file:
            msg_time = datetime.strptime(line.split('|')[0], '%Y-%m-%d %H:%M:%S.%f')
            if msg_time > target_time_obj:
                break

            symbol_chunks = re.split(r'(302=[A-Z0-9_]+\x01)', line)
            for i in range(1, len(symbol_chunks), 2):
                if target_symbol in symbol_chunks[i]:
                    level_content = symbol_chunks[i] + symbol_chunks[i+1]

                    level_match = re.search(r'299=([\d.]+)\x01', level_content)
                    level = int(level_match.group(1))

                    bid_match = re.search(r'188=([\d.]+)\x01', level_content)
                    ask_match = re.search(r'190=([\d.]+)\x01', level_content)
                    bid_size_match = re.search(r'134=([\d.]+)\x01', level_content)
                    ask_size_match = re.search(r'135=([\d.]+)\x01', level_content)

                    if bid_match:
                        liquidity_book[level]['Bid'] = float(bid_match.group(1))
                    if ask_match:
                        liquidity_book[level]['Ask'] = float(ask_match.group(1))
                    if bid_size_match:
                        liquidity_book[level]['Bid Size'] = int(bid_size_match.group(1))
                    if ask_size_match:
                        liquidity_book[level]['Ask Size'] = int(ask_size_match.group(1))

                    
                     # Reorder Bid and Bid Size columns
                    for l in reversed(range(5)):
                        # Reorder Bid and Bid Size columns
                        for i in range(l, 0, -1):
                            if liquidity_book[i]['Bid'] > liquidity_book[i - 1]['Bid']:
                                liquidity_book[i]['Bid'], liquidity_book[i - 1]['Bid'] = liquidity_book[i - 1]['Bid'], liquidity_book[i]['Bid']
                                liquidity_book[i]['Bid Size'], liquidity_book[i - 1]['Bid Size'] = liquidity_book[i - 1]['Bid Size'], liquidity_book[i]['Bid Size']
                            else:
                                break

                        # Reorder Ask and Ask Size columns
                        for i in range(l, 0, -1):
                            if liquidity_book[i]['Ask'] < liquidity_book[i - 1]['Ask']:
                                liquidity_book[i]['Ask'], liquidity_book[i - 1]['Ask'] = liquidity_book[i - 1]['Ask'], liquidity_book[i]['Ask']
                                liquidity_book[i]['Ask Size'], liquidity_book[i - 1]['Ask Size'] = liquidity_book[i - 1]['Ask Size'], liquidity_book[i]['Ask Size']
                            else:
                                break

    return liquidity_book

st.title("Herramienta interactiva para visualizar el libro de liquidez")

file_path = '20230425-0800_quote.log'
target_symbol = "EURUSD_0"

st.subheader("Selecciona el tiempo objetivo")
hours = st.slider("Hora", 0, 23, 7)
minutes = st.slider("Minutos", 0, 59, 0)
seconds = st.slider("Segundos", 0, 59, 30)
milliseconds = st.slider("Milisegundos", 0, 999, 0)

target_time = f"2023-04-25 {hours:02d}:{minutes:02d}:{seconds:02d}.{milliseconds:03d}"
liquidity_book = parse_fix_log_v9(file_path, target_symbol, target_time)

st.subheader("Selecciona el símbolo objetivo")

target_symbol = st.text_input("Símbolo objetivo", "EURUSD_0")

# Crear un contenedor para la tabla y un marcador de posición
with st.container():
    table_placeholder = st.empty()

    # Crear la tabla de liquidez
    liquidity_table = []

    # Agrega los encabezados de la tabla
    liquidity_table.append(["Level", "Bid", "Ask", "Bid Size", "Ask Size"])

    for level, data in liquidity_book.items():
        formatted_bid = format(data["Bid"], ".5f")
        formatted_ask = format(data["Ask"], ".5f")
        liquidity_table.append([f"Level {level}", formatted_bid, formatted_ask, data["Bid Size"], data["Ask Size"]])

    table_placeholder.table(liquidity_table)