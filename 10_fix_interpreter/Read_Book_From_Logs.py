import re
from datetime import datetime

def parse_fix_log_v9(file_path, target_symbol, target_time):
    liquidity_book = {level: {'Bid': 0.0, 'Ask': 0.0, 'Bid Size': 0.0, 'Ask Size': 0.0} for level in range(5)}
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
# rango entre 4 y 0, de 1 en 1 con range


file_path = '20230425-0800_quote.log'
target_symbol = "EURUSD_0"
target_time = "2023-04-25 07:00:30.000"
liquidity_book = parse_fix_log_v9(file_path, target_symbol, target_time)

for level, data in liquidity_book.items():
    print(f"Level {level}: Bid={data['Bid']}, Ask={data['Ask']}, Bid Size={data['Bid Size']}, Ask Size={data['Ask Size']}")
