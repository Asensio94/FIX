import re
import copy
import pandas as pd
import plotly.express as px
from datetime import datetime
import os

# Change to working path
os.chdir(os.path.dirname(os.path.abspath(__file__)))


"""

Prints the liquidity book at a given target_time for a given target_symbol.
If plot=True, it plots bid/ask/spread.

"""

def print_fix(s):
    print(s.replace('\x01', '|'))


# Mass Quote
def parse_mass_quote(liquidity_book, line, target_symbol_str):

    symbol_chunks = re.split(r'(302=[^\x01]+\x01)', line)

    for i in range(1, len(symbol_chunks), 2):
        if target_symbol_str not in symbol_chunks[i]:
            continue
        
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


# Market Data - Snapshot/Full Refresh
def parse_full_refresh(liquidity_book, line, target_symbol_str):
    # print_fix(line)
    if f'55{target_symbol_str}' not in line and f'262{target_symbol_str}' not in line:
        return liquidity_book

    level_chunks = re.split(r'(269=\d\x01)', line)

    for i in range(1, len(level_chunks), 2):
        level_content = level_chunks[i] + level_chunks[i+1]
        
        entry_type = int(level_chunks[i][4])  # can only have one char.
        
        level_match = re.search(r'299=([\d.]+)\x01', level_content)
        level = int(level_match.group(1))
        
        price_match = re.search(r'270=([-?\d.]+)\x01', level_content)
        size_match = re.search(r'271=([-?\d.]+)\x01', level_content)
        
        if entry_type == 0:  # Bid
            if price_match:
                liquidity_book[level]['Bid'] = float(price_match.group(1))
            if size_match:
                size = float(size_match.group(1))
                if size < 0:  # quote cancelled.
                    size = 0
                    liquidity_book[level]['Bid'] = 0
                liquidity_book[level]['Bid Size'] = size
        elif entry_type == 1:  # Ask
            if price_match:
                liquidity_book[level]['Ask'] = float(price_match.group(1))
            if size_match:
                size = float(size_match.group(1))
                if size < 0:  # quote cancelled.
                    size = 0
                    liquidity_book[level]['Ask'] = 0
                liquidity_book[level]['Ask Size'] = size
    return liquidity_book


def add_spread_data(df, liquidity_book, target_symbol, msg_time):
    
    if 'JPY' in target_symbol:
        pip = 0.01
    else:
        pip = 0.0001
        
    highest_bid = max(liquidity_book, key=lambda d: d['Bid'] if(d['Bid'] > 0 and d['Bid Size'] > 0) else -float('inf'))
    lowest_ask = min(liquidity_book, key=lambda d: d['Ask'] if (d['Ask'] > 0 and d['Ask Size'] > 0) else float('inf'))
    
    if highest_bid['Bid'] <= 0 or lowest_ask['Ask'] <= 0:
        return df

    new_row = pd.DataFrame({
        'Bid': highest_bid['Bid'], 
        'Ask': lowest_ask['Ask'],
        'Spread': (lowest_ask['Ask'] - highest_bid['Bid']) / pip
    }, index=[msg_time])

    return pd.concat([df, new_row])


def plot_data(df, target_symbol):

    fig1 = px.line(df, y=['Bid', 'Ask'])
    fig1.update_layout(xaxis_title='', yaxis_title=f'{target_symbol} Quote')
    fig1.show()

    fig2 = px.line(df, y='Spread')
    fig2.update_layout(xaxis_title='', yaxis_title=f'{target_symbol} Spread')
    fig2.show()


def analyze_fix_log(target_symbol, target_time, file_path, plot=True, n_depths=5):
    
    df = pd.DataFrame()
    liquidity_book = [{'Bid': 0.0, 'Ask': 0.0, 'Bid Size': 0.0, 'Ask Size': 0.0} for level in range(n_depths)]
    target_time_obj = datetime.strptime(target_time, '%Y-%m-%d %H:%M:%S.%f')
    target_symbol_str = "=" + target_symbol + '\x01'


    with open(file_path, 'r') as file:
        for line in file:
            if target_symbol_str not in line:
                continue
            
            msg_time = datetime.strptime(line.split('|')[0], '%Y-%m-%d %H:%M:%S.%f')
            if msg_time > target_time_obj:
                break
            
            if '35=W' in line:
                liquidity_book = parse_full_refresh(liquidity_book, line, target_symbol_str)
            elif '35=i' in line:
                liquidity_book = parse_mass_quote(liquidity_book, line, target_symbol_str)
            else:
                continue
            
            if plot:
                df = add_spread_data(df, liquidity_book, target_symbol, msg_time)

    if plot:
        plot_data(df, target_symbol)

    # New solution for ordering because I think the other one could only handle one shift at a time, not sure.
    sorted_bid = sorted(copy.deepcopy(liquidity_book), key=lambda d: d['Bid'] if d['Ask'] > 0 else -float('inf'), reverse=True)
    sorted_ask = sorted(copy.deepcopy(liquidity_book), key=lambda d: d['Ask'] if d['Ask'] > 0 else float('inf'))
    for i in range(n_depths):
        liquidity_book[i]['Bid'] = sorted_bid[i]['Bid']
        liquidity_book[i]['Bid Size'] = sorted_bid[i]['Bid Size']
        liquidity_book[i]['Ask'] = sorted_ask[i]['Ask']
        liquidity_book[i]['Ask Size'] = sorted_ask[i]['Ask Size']

    df_book = pd.DataFrame.from_records(liquidity_book, index=range(n_depths))
    print(df_book)
    return df_book


#------------------------------------------------------------------------------------------#

plot = True

target_symbol = "AUDNZD_0"

# target_time = '2023-04-25 07:00:30.000'
target_time = '2023-04-25 07:05:00.039'  # the second 35=W message for AUDNZD_0.
# target_time = '2023-04-25 07:59:59.996'  # latest time

file_path = '20230425-0800_quote.log'

analyze_fix_log(target_symbol, target_time, file_path, plot)
