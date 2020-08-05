import psycopg2
import time
import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from statsmodels.tsa.stattools import adfuller
from datetime import datetime
import pandas as pd
# import statsmodels.tsa.stattools as ts
# from arch.unitroot import ADF
import numpy as np
time_start = time.time()
DB_HOST = '192.168.3.11'
DB_PORT = 5432
DB_USERNAME = 'postgres'
DB_PASSWORD = '123456'
DB_NAME = 'test'
date = ['2020-01-02', '2020-01-03', '2020-01-06', '2020-01-07', '2020-01-08', '2020-01-09', '2020-01-10', '2020-01-13',
        '2020-01-14', '2020-01-15', '2020-01-16', '2020-01-17', '2020-01-20', '2020-01-21', '2020-01-22', '2020-01-23',
        '2020-02-03', '2020-02-04', '2020-02-05', '2020-02-06', '2020-02-07', '2020-02-10', '2020-02-11', '2020-02-12',
        '2020-02-13', '2020-02-14', '2020-02-17', '2020-02-18', '2020-02-19', '2020-02-20', '2020-02-21', '2020-02-24',
        '2020-02-25', '2020-02-26', '2020-02-27', '2020-02-28', '2020-03-02', '2020-03-03', '2020-03-04', '2020-03-05',
        '2020-03-06', '2020-03-09', '2020-03-10', '2020-03-11', '2020-03-12', '2020-03-13', '2020-03-16', '2020-03-17',
        '2020-03-18', '2020-03-19', '2020-03-20', '2020-03-23', '2020-03-24', '2020-03-25', '2020-03-26', '2020-03-31',
        '2020-04-01', '2020-04-02', '2020-04-03', '2020-04-07', '2020-04-08', '2020-04-09', '2020-04-10', '2020-04-13',
        '2020-04-14', '2020-04-15', '2020-04-16', '2020-04-17', '2020-04-20', '2020-04-21', '2020-04-22', '2020-04-23',
        '2020-04-24', '2020-04-27', '2020-04-28', '2020-04-29', '2020-04-30', '2020-05-06', '2020-05-07', '2020-05-08',
        '2020-05-11', '2020-05-12', '2020-05-13', '2020-05-14', '2020-05-15', '2020-05-18', '2020-05-19', '2020-05-20',
        '2020-05-21', '2020-05-22', '2020-05-25', '2020-05-26', '2020-05-27', '2020-05-28', '2020-05-29', '2020-06-01',
        '2020-06-02', '2020-06-03', '2020-06-04', '2020-06-05', '2020-06-08', '2020-06-09', '2020-06-10', '2020-06-11',
        '2020-06-12', '2020-06-15', '2020-06-16', '2020-06-17', '2020-06-18', '2020-06-19', '2020-06-22', '2020-06-23',
        '2020-06-24', '2020-06-29', '2020-06-30']
ti={}
t1=()
ran={}

def connectPSQL():
    conn=psycopg2.connect(database=DB_NAME, user=DB_USERNAME , password=DB_PASSWORD, host=DB_HOST , port=DB_PORT)
    print('connect successful')
    cur = conn.cursor()
    time1=[]
    t1 = '2020-01-03'
    t2 = '2020-06-30'
    n = 'cu____'  # 期货'XX____' 期权看涨'XX____C%' 看跌'XX____P%'
    for timelist in date:
        if int(timelist.replace('-',''))>=int(t1.replace('-','')) and int(timelist.replace('-', '')) <= int(t2.replace('-', '')) :
            time1.append(timelist)
    for t in time1:
        cur.execute('''
            select instrumentid,lastvolumerank from vrank where vrank.instrumentid like '{}' and vrank.lastvolumerank <=2
            and vrank.updatetime=\'{}\'
            order by lastvolumerank'''.format(n, t))
        rank=cur.fetchall()
        r1=rank[0][0]
        r2=rank[1][0]
        print(r1,rank[0][1])
        print(r2,rank[1][1])
        cur.execute('''
              SELECT InstrumentID,updatetime,bidprice1,askprice1 FROM company1
    where updatetime<'{} 24:%' and updatetime>\'{}\' and instrumentid in ( select instrumentid from vrank where vrank.instrumentid like '{}' and vrank.lastvolumerank <=2
    and vrank.updatetime=\'{}\' )
    order by updatetime,last_day_volume_rank'''.format(t,t,n,t))
        data=cur.fetchall()
        a1=''
        for i in data:
            if a1!=i[0] and t1==i[1] :
                bp1 = i[2]
                bp2 = i[3]
                p2 = (bp1 + bp2) / 2 #rank2 price
                if i[0]==r2:
                    p=p2-p1
                else:
                    p=p1-p2
                if p==0:
                    pass
                elif p>=250 or p<=-400:
                    pass
                else:
                    ti[t1]=p
            a1 = i[0] #合约名称
            t1 = i[1] #时间
            bp1 = i[2]
            bp2 = i[3]
            p1 = (bp1 + bp2) / 2 #rank1 price]
        print(t)
    ss = pd.Series(ti)
    print('标准差',ss.std())
    print('最大值',ss.max())
    print('最小值',ss.min())
    print('均值',ss.mean())
    # adf = ts.adfuller(ss)
    # print(adf)
    # print(ADF(ss))
    time_end = time.time()
    print(round(time_end - time_start, 2), 's')
    conn.commit()
    cur.close()
    conn.close()
    plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
    plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator())
    plt.plot(ss,linewidth=1)
    plt.gcf().autofmt_xdate()  # 自动旋转日期标记
    plt.show()


if __name__ == '__main__':
    connectPSQL()
