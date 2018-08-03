from futuquant import *
import pandas as pd

quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
plate_response = quote_ctx.get_plate_list(Market.HK,Plate.ALL)
plate_frame = plate_response[1]
print(plate_frame)
corr_frame = pd.DataFrame(columns=['plate','code1','name1','code2','name2','corr_value'])
error_set = []
empty_set = []
new_stock_set = []

for i in range(len(plate_frame)):
    plate_name = plate_frame.loc[i,'plate_name']
    plate_code = plate_frame.loc[i,'code']
    print(plate_name)
    plate_stock_response = quote_ctx.get_plate_stock(plate_code)
    if plate_stock_response[0] != 0:
        print('plate code error!')
        print(plate_stock_response[1])
        i = i+1
        continue
    else:
        plate_stock_frame = plate_stock_response[1]

    print(plate_stock_frame)
    for p in range(len(plate_stock_frame)):
        plate_list = []
        code1_list = []
        name1_list = []
        code2_list = []
        name2_list = []
        corr_list = []
        code1 = plate_stock_frame.loc[p,'code']
        name1 = plate_stock_frame.loc[p,'stock_name']
        print('code1 is processing.',code1)
        stock1_response = quote_ctx.get_history_kline(code1,start='2017-07-01',
                                                      end='2018-07-10',ktype='K_1M',autype=AuType.QFQ)
        if stock1_response[0] != 0:
            print(code1,'stock1 data error!')
            print(stock1_response[1])
            error_set.append(code1)
            continue

        stock1_frame = stock1_response[1]

        if stock1_frame.empty:
            print(code1,'empty frame')
            empty_set.append(code1)
            continue

        stock1_frame = stock1_frame.set_index(pd.to_datetime(stock1_frame['time_key'])).loc[:, 'close']
        stock1_serials = pd.Series(stock1_frame).resample('D').last().dropna()
        if len(stock1_serials) < 20:
            new_stock_set.append(code1)
            continue

        k = 1
        while k < (len(plate_stock_frame)-p):
            code2 = plate_stock_frame.loc[p+k,'code']
            name2 = plate_stock_frame.loc[p+k,'stock_name']
            print('code2 is processing.',code2)
            stock2_response = quote_ctx.get_history_kline(code2,start='2017-07-01',
                                                      end='2018-07-10',ktype='K_1M',autype=AuType.QFQ)
            if stock2_response[0] != 0:
                print(code2, 'stock2 data error!')
                print(stock2_response[1])
                k = k+1
                continue

            stock2_frame = stock2_response[1]

            if stock2_frame.empty:
                k = k+1
                continue

            stock2_frame = stock2_frame.set_index(pd.to_datetime(stock2_frame['time_key'])).loc[:, 'close']
            stock2_serials = pd.Series(stock2_frame).resample('D').last().dropna()

            if len(stock2_serials) < 20:
                k = k+1
                continue

            intersected_index = [val for val in list(stock1_serials.index) if val in list(stock2_serials.index)]
            if len(intersected_index) < 10:
                print(code1, 'and', code2, 'dates not fit')
                k = k + 1
                continue
            else:
                temp1 = stock1_serials[intersected_index].pct_change().dropna()
                temp2 = stock2_serials[intersected_index].pct_change().dropna()

                cor_value = temp1.corr(temp2)
                print('corr_value',cor_value)
                plate_list.append(plate_name)
                code1_list.append(code1)
                name1_list.append(name1)
                code2_list.append(code2)
                name2_list.append(name2)
                corr_list.append(cor_value)
                k = k + 1
                continue

        new_dict = {'plate':plate_list,'code1': code1_list,'name1':name1_list,'code2': code2_list,
                    'name2':name2_list,'corr_value': corr_list}
        new_frame = pd.DataFrame(new_dict)
        corr_frame = corr_frame.append(new_frame, ignore_index=True)

corr_frame.to_csv('plate_corr.csv',encoding='gbk')

pd.DataFrame(error_set).to_csv('error_plate.csv',encoding='gbk')
pd.DataFrame(empty_set).to_csv('empty_plate.csv',encoding='gbk')
pd.DataFrame(new_stock_set).to_csv('new_stock_plate.csv',encoding='gbk')

quote_ctx.close()