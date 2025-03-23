from bs4 import BeautifulSoup
import pandas as pd
import csv
import json
import requests
import time
from collections import Counter
from datetime import datetime
import schedule

pd.options.display.width=None
pd.options.display.max_columns=None
pd.set_option('display.max_rows', 10000)
pd.set_option('display.max_columns', 20)
pd.set_option('display.max_colwidth', 100)


def surebet_scraper():
    start_time = time.time()
    url = 'https://en.surebet.com/surebets'
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"}
    with open('cookies.json') as f:
        cookie_list = json.load(f)

    cookies = {cookie['name']: cookie['value'] for cookie in cookie_list}

    # Create a new instance of the Chrome driver
    page = requests.get(url, headers=headers, cookies=cookies)
    soup = BeautifulSoup(page.text, 'html.parser')

    bookie_list = []
    extra_url_list = []
    percent_list = []

    table = soup.find('table',{'id':'surebets-table'})
    for record in table.find_all('tbody', {'class':'surebet_record'}):
        bookie_pair = []
        for side, i in zip(record.find_all('tr'), range(3)):
            if i == 0:
                percent = side.find('td', {'class':'profit-box'}).find('span', {'class':'profit'}).text
                percent_list.append(percent)
                bookie = side.find('td', {'class':'booker'}).find('a').text
                real_bookie = bookie.replace('\u200b', '')
                bookie_pair.append(real_bookie)
            elif i == 1:
                bookie = side.find('td', {'class':'booker'}).find('a').text
                real_bookie = bookie.replace('\u200b', '')
                bookie_pair.append(real_bookie)
                bookie_list.append(bookie_pair)
            elif i == 2:
                try:
                    extra_url = side.find('td', {'class':'extra'}).find('a').get('href')
                    real_extra = "https://en.surebet.com" + extra_url
                    extra_url_list.append(real_extra)
                except:
                    pass

    normal_flat_list = [item for sublist in bookie_list for item in sublist]
    unique_bookie = set(normal_flat_list)
    print("Number of unique bookie:", len(unique_bookie))
    frequency = Counter(normal_flat_list)
    df_time1 = datetime.now().strftime('%H:%M')
    print(f'Regular Arbitrage Opportunities Distribution Table at {df_time1}:')
    regular_df = pd.DataFrame(list(frequency.items()), columns=['Bookie', 'Frequency'])
    print(regular_df)
    timestamp1 = datetime.now().strftime('%Y%m%d_%H%M')
    filename1 = f'regular_frequency_{timestamp1}.csv'
    regular_df.to_csv(filename1, index=False)

    regular_bookie = bookie_list
    regular_percent_list = percent_list
    regular_arb_df = pd.DataFrame({'Bookie Pair':regular_bookie,'Profit %':regular_percent_list})
    filename3 = f'regular_arb_games_{timestamp1}.csv'
    regular_arb_df.to_csv(filename3, index=False)

    for extra in extra_url_list:
        extra_page = requests.get(extra, headers=headers, cookies=cookies)
        extra_soup = BeautifulSoup(extra_page.text, 'html.parser')
        extra_table = extra_soup.find('table', {'class':'app-table app-wide'})
        for record in extra_table.find_all('tbody', {'class':'surebet_record'}):
            extra_bookie_pair = []
            for side, i in zip(record.find_all('tr')[:2], range(2)):
                if i == 0:
                    percent = side.find('td', {'class': 'profit-box'}).find('span', {'class': 'profit'}).text
                    percent_list.append(percent)
                    bookie = side.find('td', {'class':'booker'}).find('a').text
                    real_bookie = bookie.replace('\u200b', '')
                    extra_bookie_pair.append(real_bookie)
                elif i == 1:
                    bookie = side.find('td', {'class': 'booker'}).find('a').text
                    real_bookie = bookie.replace('\u200b', '')
                    extra_bookie_pair.append(real_bookie)
                    bookie_list.append(extra_bookie_pair)

    print('=======================================================')
    print("Numbers of games with extra arbitrage opportunities:",len(extra_url_list))

    flat_list = [item for sublist in bookie_list for item in sublist]
    unique_bookie_with_extra = set(flat_list)
    print("Number of unique bookie:", len(unique_bookie_with_extra))
    frequency = Counter(flat_list)
    df_time2 = datetime.now().strftime('%H:%M')
    print(f'Full Arbitrage Opportunities Distribution Table at {df_time2}:')
    full_df = pd.DataFrame(list(frequency.items()), columns=['Bookie', 'Frequency'])
    print(full_df)
    timestamp2 = datetime.now().strftime('%Y%m%d_%H%M')
    filename2 = f'full_{timestamp2}.csv'
    full_df.to_csv(filename2, index=False)

    full_bookie = bookie_list
    full_percent_list = percent_list
    full_arb_df = pd.DataFrame({'Bookie Pair':full_bookie,'Profit %':full_percent_list})
    filename4 = f'full_arb_games_{timestamp2}.csv'
    full_arb_df.to_csv(filename4, index=False)

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time} seconds")

    return regular_df, full_df, regular_arb_df, full_arb_df


def funds_allocation(freq_df, current_bal, extra_buffer, is_reg):
    total_freq = freq_df['Frequency'].sum()
    freq_df['Percentage'] = (freq_df['Frequency'] / total_freq)
    freq_df['Funds'] = current_bal * freq_df['Percentage'] * extra_buffer
    print(freq_df)
    ts = datetime.now().strftime('%Y%m%d_%H%M')
    if is_reg:
        file = f'regular_funds_allocation_{ts}.csv'
    elif not is_reg:
        file = f'full_funds_allocation_{ts}.csv'

    return freq_df.to_csv(file, index=False)


if __name__ == '__main__':
    bal = 1200
    buffer = 1.3
    regular_freq, full_freq, regular_arb, full_arb = surebet_scraper()
    rfa = funds_allocation(regular_freq, bal, buffer, True)
    ffa = funds_allocation(full_freq, bal, buffer, False)
    schedule.every(15).minutes.do(surebet_scraper)
    schedule.every(15).minutes.do(funds_allocation(regular_freq, bal, buffer, True))
    schedule.every(15).minutes.do(funds_allocation(full_freq, bal, buffer, False))

    # Run the scheduler
    while True:
        schedule.run_pending()
        time.sleep(1)