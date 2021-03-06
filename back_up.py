# -*- coding: utf-8 -*-
"""
Created on Wed May 23 10:51:15 2018

@author: jm01 (Chonglou)

@ Completed on ???
"""

# -*- coding: utf-8 -*-
"""
==========================================================
This script is work to generate lab data 2.0(USER WEB LOG AND TRANSACTIONS)
==========================================================

INFO:
# Total Running Time: 10 mins for each sales day
# Table Structure Generated: []
# Total Rows Generated: depends...

RESOURCES(MySQL):
originaldata.customer
originaldata.cust_property
originaldata.prod_online
originaldata.demo_tmp_dim_product_purchases
originaldata.demand
originaldata.noise
originaldata.ad_click
originaldata.advertising
originaldata.promo_received
originaldata.promotion1
originaldata.inventory
originaldata.inventory_transactions
"""
# !!!!!Please Make Sure Available Memory Have At Least 32G

# import sys
from tqdm import tqdm
import csv
import math
import random
import numpy as np
import pandas as pd
import pymysql.cursors
from datetime import *
import time 
from sqlalchemy import create_engine
from copy import deepcopy
# deepcopy: forces objects to be copied in memory, so that methods called on the new objects are not applied to the source object.

time_start=time.time()
print('Start at', time.strftime("%H:%M:%S") )

# global variables
oridata = create_engine(
    'mysql+pymysql://jm01:Arc@201801@rm-uf6f7sl8tkx54io559o.mysql.rds.aliyuncs.com:3306/originaldata')

start_dt = date(2011, 5, 18)
print("start_dt is", start_dt)

'''⚠️ please modify the end_dt for smoke testing'''
end_dt = date(2019, 12, 31)
print("end_dt is", end_dt)

# nominate depend tables and their date keys in case of any change in the database
dp_t1 = 'customer'
dp_t2, dp_t2_dtkey = 'cust_property', 'modify_date'
dp_t3, dp_t3_dtkey = 'ad_click', 'ad_click_date'
dp_t4, dp_t4_begindt, dp_t4_enddt = 'advertising', 'ad_begin_date', 'ad_end_date'
dp_t5, dp_t5_dtkey = 'promo_received', 'send_date'
dp_t6, dp_t6_begindt, dp_t6_enddt = 'promotion', 'promo_begin_date', 'promo_end_date'
dp_t7, dp_t7_dtkey = 'prod_online', 'refresh_date'
dp_t8, dp_t8_dtkey = 'demo_inventory', 'Date'
dp_t9, dp_t9_dtkey = 'demo_inventory_trans', 'transaction_date'
dp_t10, dp_t10_dtkey = 'demand', 'date'
dp_t11, dp_t11_dtkey = 'noise', 'date'
dp_t12 = 'demo_tmp_dim_product_purchases'
dp_t13 = 'product'

dp_t14 = 'sales_order_details'
dp_t15 = 'sales_order'    
result_tbl = 'sales_order'
result_tbl_dtkey = 'date'

def main():

    # find refresh date
    refresh_dt = findRefresh_dt(oridata, result_tbl, result_tbl_dtkey)
    # QA
    print("refresh_dt begins at", refresh_dt)

    # 开始每一天的循环
    while refresh_dt <= end_dt:

        # 找到库存表最大日期
        invt_maxdt = findMax_dt(oridata, dp_t8, dp_t8_dtkey)

        # 找到需求表最大日期
        dmd_maxdt = findMax_dt(oridata, dp_t10, dp_t10_dtkey)

        # 判断库存表和需求表是否能够满足当天生产所需的数据
        if refresh_dt <= invt_maxdt + timedelta(days=1) and refresh_dt <= dmd_maxdt:

            # PART 1 ======================================================================================

            # 1.1 开始生成3个df
            print("Dataframes production for {0} begins".format(refresh_dt))

            # 1.2 抽取3表格当天的数据
            df_demand = pd.read_sql(
                """SELECT * FROM {0} WHERE {1} = '{2}' """.format(dp_t10, dp_t10_dtkey, refresh_dt), oridata)
            df_noise = pd.read_sql(
                """SELECT * FROM {0} WHERE {1} = '{2}' """.format(dp_t11, dp_t11_dtkey, refresh_dt), oridata)
            df_ads_click = pd.read_sql(
                """SELECT * FROM {0} WHERE {1} = '{2}' """.format(dp_t3, dp_t3_dtkey, refresh_dt), oridata)

            # 1.3 添加或补充新列：Origin 浏览来源,first_buy_mark 购买概率标记,land_page 浏览开始页
            df_demand['prob_origin'] = np.random.random(df_demand.shape[0])
            df_demand['origin'] = np.where(
                df_demand['prob_origin'] > 0.2, 'direct', 'search')
            df_demand['land_page'] = 'home_page'
            df_noise['prob_origin'] = np.random.random(df_noise.shape[0])
            df_noise['origin'] = np.where(
                df_noise['prob_origin'] > 0.3, 'direct', 'search')
            df_noise['land_page'] = 'home_page'
            df_ads_click['origin'] = 'ads'
            df_ads_click['first_buy_mark'] = 0
            df_ads_click['land_page'] = 'ads_page'
            # QA
            print('row of df_ads_click is ', len(df_ads_click))

            # PART 2 ======================================================================================

            # 2.1 抽取并链接 广告表 和 产品购买基准表 中的 quantity字段，得到广告产品表

            # dp_t4, dp_t4_begindt, dp_t4_enddt = 'advertising', 'ad_begin_date', 'ad_end_date'
            # dp_t12 = 'demo_tmp_dim_product_purchases'

            sql_ads_quantity = """
                SELECT a.*, b.purchase_quantity
                FROM {0} as a LEFT JOIN {1} as b
                ON a.product_key = b.product_key
                WHERE a.{2} <= '{4}' AND a.{3} >= '{4}'
                """.format(dp_t4, dp_t12, dp_t4_begindt, dp_t4_enddt, refresh_dt)
            df_ads_quantity = pd.read_sql(sql_ads_quantity, oridata)

            '''小问题：这里fillna是用来保证 广告表 中的产品 不在 产品购买基准表 的情况下，purchase_quantity 为空值'''
            df_ads_quantity['purchase_quantity'].fillna(value=1, inplace=True)
            # QA
            print('row of df_ads_quantity is ', len(df_ads_quantity))

            # 2.2 链接广告点击表和广告产品表,产生笛卡尔积,使每一个顾客都有N个产品
            df_ads = pd.merge(df_ads_click, df_ads_quantity[['advertising_key', 'product_key', 'purchase_quantity']],
                              how='left', on='advertising_key')
            # QA
            print('row of df_ads is ', len(df_ads))

            # PART 3 ======================================================================================
            # 将demand,noise和ads的列名统一，然后合并3张表
            df_demand.rename(columns={'calendar_date': 'date'}, inplace=True)
            df_noise.rename(columns={'calendar_date': 'date'}, inplace=True)
            df_ads.rename(columns={'ad_click_date': 'date',
                                   'purchase_quantity': 'quantity'}, inplace=True)
            df_visits = df_demand.append([df_noise, df_ads], ignore_index=True)
            # QA
            print('row of df_noise is ', len(df_noise))
            print('row of df_demand is ', len(df_demand))
            print('row of df_visits is ', len(df_visits))

            # PART 4 ======================================================================================
            # 抽取loyalty
            print('fetching cust_property ... ')
            df_cust_property = pd.read_sql(
                """SELECT * FROM {0} WHERE {1} IS NULL or {1} <= '{2}' """.format(dp_t2, dp_t2_dtkey, refresh_dt), oridata)
            df_cust_property = df_cust_property[[
                'customer_key', 'cust_loyalty', 'modify_date']]
            df_cust_property.sort_values(
                by='modify_date', ascending=True, inplace=True)  # 只保留最新的loyalty记录
            df_cust_property = df_cust_property.groupby('customer_key').tail(1)

            print('merging loyalty ... ')  
            # 链接 loyalty 到 df_visits
            df_visits = pd.merge(df_visits, df_cust_property[[
                                 'customer_key', 'cust_loyalty']], how='left', on='customer_key')

            '''已解决：当天的顾客表中没有测试数据的那些顾客，所以下面这行是用来保证loyalty这一列无空值'''
            '''下面这行测试用，用来补齐空值，demand表改完之后注释掉，再测试一次'''
            # df_visits['cust_loyalty'] = df_visits['cust_loyalty'].apply(
            #     lambda x: np.random.random() if pd.isnull(x) else x)

            # QA
            print('row of df_visits is ', len(df_visits))

            # PART 5 ======================================================================================
            # 抽取价格 price

            # dp_t7, dp_t7_dtkey = 'prod_online', 'refresh_date'

            print('fetching price ... ')
            df_price = pd.read_sql(
                """SELECT * FROM {0} WHERE {1} <= '{2}' """.format(dp_t7, dp_t7_dtkey, refresh_dt), oridata)
            df_price = df_price[['product_key', 'price', 'refresh_date']]
            df_price.sort_values(
                by='refresh_date', ascending=True, inplace=True)  # 只保留最新的price记录
            df_price = df_price.groupby('product_key').tail(1)
            # print("df_price's first 5 rows is ", df_price.head())

            print('merging price ... ')  # 链接 df_visits
            df_visits = pd.merge(
                df_visits, df_price[['product_key', 'price']], how='left', on='product_key')

            '''已解决:当天irst_buy_mark为1的人需求的产品，必须已经上架，否则price那一列会出现空值'''
            '''下面这行用来补空值，方便buy_score计算'''
            df_visits['price'].fillna(value=0, inplace=True)

            # QA
            print('row of df_visits is ', len(df_visits))

            # PART 6 ======================================================================================
            # 计算顾客状态 Status

            print('fetching customer ... ')
            df_cust = pd.read_sql(
                """SELECT id,customer_key,cust_first_order_date,close_date,return_date
                FROM {0} """.format(dp_t1), oridata)

            print('calculating status ... ')
            df_cust_status = cust_seg(df_cust, refresh_dt)

            print('merging status ... ')  # 链接 df_visits
            df_visits = pd.merge(df_visits, df_cust_status,
                                 how='left', on='customer_key')

            # 当天的需求表中没有状态的那些顾客，说明他们当天不能进行 购买，否则和他们的status计算结果会发生冲突
            # 需要将他们的first_buy_mark改为-1
            df_visits['cust_status'].fillna(value=0, inplace=True)
            df_visits['first_buy_mark'] = df_visits.apply(lambda row: fbm_modify(row), axis=1)

            # QA
            print('row of df_visits is ', len(df_visits))

            # PART 7 ======================================================================================
            # 根据顾客收到的促销，新增促销信息（种类、力度、时间等）

            # dp_t5, dp_t5_dtkey = 'promo_received', 'send_dt'
            # dp_t6, dp_t6_begindt, dp_t6_enddt = 'promotion1', 'promo_begin_date', 'promo_end_date'

            # 7.1
            print('fetching promotion ... ')
            df_promotion = pd.read_sql(
                """SELECT *
                FROM {0}
                WHERE '{1}' BETWEEN {2} AND {3} """.format(dp_t6, refresh_dt, dp_t6_begindt, dp_t6_enddt), oridata)

            # 获取当天顾客获得的促销
            print('fetching promo_received ...')
            df_promo_received = pd.read_sql(
                """SELECT * FROM {0} WHERE {1} <= '{2}' """.format(dp_t5, dp_t5_dtkey, refresh_dt), oridata)

            # 链接 promo 和 promo_received
            print('merging customers, promotion products and discount types ... ')
            df_promo = pd.merge(df_promotion, df_promo_received,
                                how='inner', on='promo_key')
            # QA
            print('row of df_promo is ', len(df_promo))

            # 链接 df_visits 中的商品信息
            print(
                'merging promotions to df_visits: visit_key，customers and product info... ')
            df_visits['visit_key'] = range(1, len(df_visits) + 1)
            df_cpp = pd.merge(df_visits[['visit_key', 'customer_key', 'product_key', 'quantity', 'price']], df_promo,
                              how='left', on=['customer_key', 'product_key'])
            # QA
            print('row of df_cpp is ', len(df_cpp))

            # 7.2 在df_cpp中，根据价格、数量和促销类型，计算并新增sales, adj_sales 和 adj_quantity
            '''小问题：由于demo数据的原因，df_cpp基本都是空值，测试promo_affect函数需要构建一个虚拟的df'''
            print('calculating adjusted sales and quantities ... ')
            df_cpp_adj = promo_affects(df_cpp)
            df_cpp_adj = df_cpp_adj[['visit_key', 'sales', 'promo_key', 'discount_type', 'adj_quantity', 'adj_sales']]

            # 链接 df_visits
            print('merging adjusted sales and quantities to df_visits ... ')
            df_visits = pd.merge(df_visits, df_cpp_adj,
                                 how='left', on='visit_key')
            # QA
            print('row of df_visits is ', len(df_visits))

            # PART 8 ======================================================================================

            # 判断顾客是否愿意下单
            print('calculating buying scores... ')
            df_visits = buy_decision(df_visits)
            # QA
            print('row of df_visits is ', len(df_visits))

            # PART 9 ======================================================================================
            # 判断库存量是否能完成订单

            # 9.1 抓取产品部门编码 department code
            # dp_t13 = 'product'
            print('fetching department ... ')
            df_department = pd.read_sql(
                """SELECT DISTINCT product_key,department_cd FROM {0} """.format(dp_t13), oridata)

            # 链接 department_cd 到 df_visits
            print('merging department to df_visits ... ')
            df_visits = pd.merge(df_visits, df_department,
                                 how='left', on='product_key')
            # QA
            print('row of df_visits is ', len(df_visits))

            # 9.2 生成订单
            print('Generating Orders ... ')
            df_visits, df_allp_invt_Comb = orderGenerate(df_visits,refresh_dt, df_department) 
            # QA
            print('row of df_visits is  ', len(df_visits))


            '''网页表部分 开始'''
            # PART 10 ======================================================================================
            # 从这里开始 以 df_visits 为基础 ，生成df_orders即订单表 和 df_sessions即网页浏览表
            '''网页表部分 结束'''

            # 10.1 合并同一顾客的当日订单 

            # 筛选出所有订单
            df_orders = deepcopy(df_visits.loc[df_visits['trans_done'] == 1, :])
            print('row of df_orders is  ', len(df_orders))
            df_orders = df_orders.reset_index(drop=True)
            df_orders = df_orders[['visit_key', 'date', 'time_stamp', 'customer_key',
                       'product_key', 'promo_key',  'adj_sales', 'adj_quantity']]
            # 计算实际交易单价
            df_orders['adj_price'] = df_orders['adj_sales']/df_orders['adj_quantity']

            # 生成order_key
            df_orders['dt_key'] = df_orders['date'].apply(to_integer)
            df_orders['order_key'] = df_orders.apply(lambda row: orderkey(row), axis=1)

            # 生成一个满足（0，1）正态分布的随机数，作为合并订单的参数
            df_orders['combine_flag'] = np.random.normal(0, 1, size=df_orders.shape[0])

            print('row of df_orders is ', len(df_orders))

            # 调用订单合并函数
            print('Combinging Orders ... ')
            df_orders_combined = sales_orders(df_orders)

            # QA
            print('row of df_orders_combined is ', len(df_orders_combined))

            # 10.2 拆分为交易表和交易明细表
            # sales_order_details
            df_sales_order_details = df_orders_combined[['order_key','product_key','adj_quantity','adj_price','promo_key','date']]
            df_sales_order_details.rename(columns={'adj_quantity': 'quantity','adj_price': 'unit_price'}, inplace=True)
            print('row of df_sales_order_details is ', len(df_sales_order_details))

            # sales_order
            df_sales_order = df_orders_combined[['order_key','customer_key','date']].groupby(['order_key'],as_index=False).max()
            print('row of sales_order is ', len(df_sales_order))

            # 10.3 写入数据库
            rows_sales_order_details = InsertSalesData(oridata, dp_t14, refresh_dt, df_sales_order_details)
            print(" Inserted {0} rows of sales data into {1} on {2}".format(rows_sales_order_details, dp_t14, refresh_dt))
            
            rows_sales_order = InsertSalesData(oridata, dp_t15, refresh_dt, df_sales_order)
            print(" Inserted {0} rows of sales data into {1} on {2}".format(rows_sales_order, dp_t15, refresh_dt))

            # PART 12 ======================================================================================
            # 网页浏览表
            # 🚧 🚧 🚧 🚧 🚧 🚧 🚧 🚧 🚧 🚧

            # 🚧 🚧 🚧 🚧 🚧 🚧 🚧 🚧 🚧 🚧
            print("Dataframes production for {0} ends".format(refresh_dt))
            print("#############################################")

        else:
            print("Inventory table is not ready for ", refresh_dt)
            break

        refresh_dt=refresh_dt + timedelta(days = 1)
    
    # 调试多天
    return df_visits, df_sales_order, rows_sales_order_details, df_allp_invt_Comb


def checkTableExists(engine, tablename):  # check if sales_order exists #tested

    sql="""
        SELECT COUNT(*) as ct
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(tablename)

    count=pd.read_sql(sql, engine)

    if count['ct'][0] == 1:
        print(tablename, "exits")
        return True

    else:
        print(tablename, "does not exit")
        return False


def findRefresh_dt(engine, tablename, dtkey):  # find refresh date #working

    if checkTableExists(engine, tablename):
        sql="""
            SELECT max({0}) as max_dt
            FROM {1}
            """.format(dtkey, tablename)

        df_maxdt=pd.read_sql(sql, engine)

        refresh_dt=df_maxdt['max_dt'][0] + timedelta(days = 1)
        refresh_dt = refresh_dt.date()
        print("refresh_dt is {0}".format(refresh_dt))
        print("type of refresh_dt is {0}".format(type(refresh_dt)))

    else:
        refresh_dt=start_dt

    return refresh_dt


def findMax_dt(engine, tablename, dtkey):  # find max date in the table #working

    if checkTableExists(engine, tablename):
        sql="""
            SELECT max({0}) as max_dt
            FROM {1}
            """.format(dtkey, tablename)

        df_maxdt=pd.read_sql(sql, engine)

        maxdt=df_maxdt['max_dt'][0]

    return maxdt


def cust_seg(customer, current_date):

    # 没有close或者return的顾客，将时间字段赋予0001-01-01，以便比较
    customer=customer.fillna(date(1, 1, 1))

    customer=customer.loc[customer['cust_first_order_date'] <= current_date]
    customer=customer.loc[customer['close_date'] <= current_date]
    customer=customer.loc[customer['return_date'] <= current_date]

    max_id=customer['id'].groupby(customer['customer_key']).max()
    max_id=pd.DataFrame(max_id)
    max_id=max_id.reset_index(drop = True)

    cust_date=pd.merge(max_id, customer, how = 'left',
                         left_on = 'id', right_on = 'id')

    i=0
    lista=[]

    while i < len(cust_date):

        # 提取出每个顾客相应的时间数据
        fd=cust_date['cust_first_order_date'][i]
        cd=cust_date['close_date'][i]
        rd=cust_date['return_date'][i]

        # 筛选cd和rd是0001-01-01的顾客，即还没有close或者return的
        if fd >= cd and fd >= rd:

            # 首次购买日期距离current_date在30天之内的认定为新顾客
            if fd >= (current_date - timedelta(days=30)):  
                lista.append('New')

            else:  # 其余的认定为积极顾客
                lista.append('Reactive')

        # close_date最大的顾客判断为流失顾客
        elif cd > fd and cd > rd:  
            lista.append('Close')
        
        # 其余的判定为回流顾客
        else:  
            lista.append('Return')

        i=i + 1

    cust_date['cust_status']=lista
    cust_status=cust_date[['customer_key', 'cust_status']]

    return cust_status


def fbm_modify(row):

    if row['cust_status'] == 0:

        return -1

    else:

        return row['first_buy_mark']


def trans_into_dict(row_value):

    if row_value == None:
        return None

    elif pd.isnull(row_value):
        return None

    else:
        return eval(row_value)


def promo_affects(tbl):

    # 类型转换
    tbl['discount_type']=tbl['discount_type'].astype(str)
    tbl['quantity']=tbl['quantity'].astype(float) 
    tbl['price']=tbl['price'].astype(float)
    tbl['fcp_treatment']=tbl['fcp_treatment'].apply(trans_into_dict)
    tbl['bp_treatment']=tbl['bp_treatment'].apply(trans_into_dict)
    tbl['mp_treatment']=tbl['mp_treatment'].apply(trans_into_dict)

    # 计算原始销售额
    tbl['sales']=tbl['quantity'] * tbl['price']

    # 初始化两个seires
    # adjusted sales & adjusted quantity
    list_as=[]
    list_aq=[]

    # 调整销售额、成交量
    for i in range(len(tbl)):

        # fixed_cart
        if tbl['discount_type'][i] == 'fixed_cart':

            if tbl['sales'][i] >= tbl['fcp_treatment'][i]['full']:
                list_as.append(tbl['sales'][i] -
                               tbl['fcp_treatment'][i]['minus'])

            else:
                list_as.append(tbl['sales'][i])

            list_aq.append(tbl['quantity'][i])

        # percent
        elif tbl['discount_type'][i] == 'percent':

            list_as.append(tbl['sales'][i] * tbl['pp_treatment'][i])

            list_aq.append(tbl['quantity'][i])

        # bogo
        elif tbl['discount_type'][i] == 'bogo':

            list_as.append(tbl['sales'][i])

            # 除法取整
            quotient=tbl['quantity'][i] // tbl['bp_treatment'][i]['buy']

            # 原始成交量是Q件，买x送y, Q//x 取整得quot，调整后成交量 = Q + quot * y
            list_aq.append(tbl['quantity'][i] +
                           quotient * tbl['bp_treatment'][i]['get free'])

        # multibuy
        elif tbl['discount_type'][i] == 'multibuy':

            # 除法取整
            quotient = tbl['quantity'][i] // tbl['mp_treatment'][i]['quantity']

            # 除法取余
            remainder = tbl['quantity'][i] % tbl['mp_treatment'][i]['quantity']

            if quotient >= 1:
                list_as.append(quotient *
                               tbl['mp_treatment'][i]['price'] +
                               remainder * tbl['price'][i])

            else:
                list_as.append(tbl['sales'][i])

            list_aq.append(tbl['quantity'][i])

        # xxoff
        elif tbl['discount_type'][i] == 'xxoff':

            list_as.append(tbl['sales'][i] - tbl['xp_treatment'][i])

            list_aq.append(tbl['quantity'][i])

        # null值，没有促销卷
        else:
            list_as.append(tbl['sales'][i])

            list_aq.append(tbl['quantity'][i])

    # QA
    print("length of list_as is ", len(list_as))
    print("length of list_aq is ", len(list_aq))

    tbl['adj_sales'] = list_as
    tbl['adj_quantity'] = list_aq

    return tbl


def score_cs(row_value):

    if row_value == 'New':
        return abs(np.random.normal(1, 0.1))

    elif row_value == 'Reactive':
        return abs(np.random.normal(1, 0.05))

    elif row_value == 'Return':
        return abs(np.random.normal(1.1, 0.1))

    else:
        return 0


def score_prom(row_value):

    if pd.isnull(row_value):
        return abs(np.random.normal(1, 0.05))

    elif row_value == None:
        return abs(np.random.normal(1, 0.05))

    else:
        return abs(np.random.normal(0.7, 0.1))


def buy_score(row):

    #  该顾客第一次购买
    if row['first_buy_mark'] == 1:
        return 1
    
    #  该顾客不是第一次购买
    elif row['first_buy_mark'] == 0:

        # 计算放弃分
        score = row['score_flag'] * row['score_cs'] * row['score_prom']

        # 比较顾客忠诚度和放弃分，如果分数没有超过顾客忠诚度，则购买分数为-1
        if row['cust_loyalty'] >= score:

            #如果该产品已上架
            if row['price'] != 0:
                return -1

            #如果该产品未上架
            else:
                return 0
        
        # 放弃分超过顾客忠诚度，购买分数为0
        else:
            return 0

    # 该顾客这次不能购买，row['first_buy_mark'] == -1
    else:
        return 0



def buy_decision(tbl):

    list_cs = []

    tbl['score_flag'] = np.random.random(tbl.shape[0])

    tbl['score_cs'] = tbl['cust_status'].apply(score_cs)

    tbl['score_prom'] = tbl['promo_key'].apply(score_prom)

    tbl['cust_loyalty'] = tbl['cust_loyalty'].astype(float)

    tbl['buy_score'] = tbl.apply(lambda row: buy_score(row), axis=1)

    return tbl


def randomTimeStamp(refresh_dt, number):

    start_year = refresh_dt.year
    start_month = refresh_dt.month
    start_day = refresh_dt.day
    stop_year = refresh_dt.year
    stop_month = refresh_dt.month
    stop_day = refresh_dt.day

    # 设置开始日期时间元组（如 1976-01-01 00：00：00）
    a1 = (start_year, start_month, start_day, 0, 0, 0,
          0, 0, 0)
    # 设置结束日期时间元组（如 1990-12-31 23：59：59）
    a2 = (stop_year, stop_month, stop_day, 23, 59, 59,
          0, 0, 0)

    # 生成开始时间戳
    start = time.mktime(a1)
    # 生成结束时间戳
    end = time.mktime(a2)

    # 随机生成n个日期字符串
    random_timestamp = []

    for i in range(number):

        # 在开始和结束时间戳中随机取出一个
        t = random.randint(start, end)

        # 将时间戳生成时间元组
        date_touple = time.localtime(t)

        # 将时间元组转成格式化字符串（1976-05-21）
        # date = time.strftime("%Y-%m-%d %H:%M:%S", date_touple)
        date = time.strftime("%H:%M:%S", date_touple)
        random_timestamp.append(date)

    # type(random_timestamp) is list, elements in this list are strings with ''
    return(random_timestamp)


def fetchAllp_invtInfo(engine, tablename_1, dtkey_1, tablename_2, dtkey_2, invt_dt):

    # 库存表
    sql_1 = """
        SELECT *
        FROM {0}
        WHERE {1} = '{2}'
        """.format(tablename_1, dtkey_1, invt_dt)

    df_invt = pd.read_sql(sql_1, engine)

    # 库存交易表
    sql_2 = """
        SELECT *
        FROM {0}
        WHERE {1} = '{2}' AND transaction_type = 'purchase'
        """.format(tablename_2, dtkey_2, invt_dt)

    df_invt_trans = pd.read_sql(sql_2, engine)

    return df_invt, df_invt_trans


def fetchBalance(row, df_allp_invt_Comb):

    # print("row begins...............")
    # print("product_key is", row['product_key'])

    # 有购买意向的行为，包括两种人，1为必须购买成功，-1为不必购买成功
    if row['buy_score'] != 0:
        # print("有购买意向")

        while True:

            if row['department_cd'] == 34 or row['department_cd'] == 37:
                BK = 'TransBalance'

            else:
                BK = 'CombinedBalance'

            dq = row['adj_quantity']
            iq = df_allp_invt_Comb.loc[df_allp_invt_Comb['Product_key'] ==
                                       row['product_key'], [BK]][BK].iloc[0]

            # 能直接满足
            if dq <= iq:

                row['trans_quantity'] = dq

                df_allp_invt_Comb.loc[df_allp_invt_Comb['Product_key'] ==
                                      row['product_key'], BK] = iq - row['trans_quantity']

                row['trans_done'] = 1

            # 不能直接满足
            else:

                # 还有库存:成功
                if iq != 0:

                    row['trans_quantity'] = iq

                    df_allp_invt_Comb.loc[df_allp_invt_Comb['Product_key']
                                          == row['product_key'], BK] = 0

                    row['trans_done'] = 1

                # 没有库存了
                else:

                    # 必须购买
                    if row['buy_score'] == 1:

                        row['trans_quantity'] = 0
                        row['trans_done'] = 0

                    # 非必须购买
                    else:

                        row['trans_quantity'] = 0
                        row['trans_done'] = 0

            # 如果必须购买的人没有成功，继续循环，更换产品和需求量；否则可以终止循环
            if row['buy_score'] == 1 and row['trans_done'] == 0:

                # change Product_key
                row['product_key'] = int(
                    df_allp_invt_Comb['Product_key'].sample().iloc[0])
                # change department_cd
                row['department_cd'] = df_allp_invt_Comb.loc[df_allp_invt_Comb['Product_key'] ==
                                                             row['product_key'], ['department_cd']]['department_cd'].iloc[0]
                # change adj_quantity
                row['adj_quantity'] = 1

                continue

            else:
                # ct = ct + 1
                break

        return row

    # 无购买意向的行为，不需更改
    else:

        return row



def orderGenerate(tbl, refresh_dt, df_department):

    # 一、需求表准备
    df = tbl.copy()
    # 打上随机时间戳，并按时间先后排序，初始化trans_done,CombinedBalance,TransBalance
    time_stamp = randomTimeStamp(refresh_dt, len(df))
    df['time_stamp'] = time_stamp
    df.sort_values(by='time_stamp', ascending=True, inplace=True)
    df['visit_key'] = range(1, len(df) + 1)
    df['trans_done'] = 0
    df['trans_quantity'] = 0
    df = df.reset_index(drop=True)
    # adding a dummy row to deal with df.apply() which is designed to run twice on first row/column
    df.loc[-1] = df.iloc[0]
    df['buy_score'].loc[-1] = 0  # critical
    df.index = df.index + 1  # shifting index
    df.sort_index(inplace=True)
    print('df is Ready ', len(df))

    # 二、库存表准备
    # 库存日为刷新日的前一天：是否能成交需要检查的是前一天的库存和前一天的备货
    invt_dt = refresh_dt - timedelta(days=1)

    # 1.1 抓取所有产品 库存日(刷新日前一天) 的 静态 合并库存CombinedBalance 和 备货库存TransBalance
    df_allp_invt, df_allp_invt_trans = fetchAllp_invtInfo(
        oridata, dp_t8, dp_t8_dtkey, dp_t9, dp_t9_dtkey, invt_dt)

    # 1.2 合并 df_allp_invt 到 df_allp_invt_trans
    print('merging df_allp_invt to df_allp_invt_trans ... ')
    df_allp_invt_Comb = pd.merge(df_allp_invt, df_allp_invt_trans[[
                                 'product_key', 'quantity']], how='outer', left_on='Product_key', right_on='product_key')
    # QA
    if len(df_allp_invt_Comb) == len(df_allp_invt) + len(df_allp_invt_trans) - len(set(df_allp_invt['Product_key']) & set(df_allp_invt_trans['product_key'])):
        print("df_allp_invt_Comb QA is OK")
    else:
        print("WARNING 1: df_allp_invt_Comb QA is WRONG !!!!!!!  PLEASE CHECK !!!!!!!!!! ")
    print('row of df_allp_invt_Comb is ', len(df_allp_invt_Comb))
    # 填充Quantity 和 quantity 缺失值，得到 合并库存CombinedBalance 和 备货库存TransBalance
    df_allp_invt_Comb = df_allp_invt_Comb.fillna(
        {'Quantity': 0, 'quantity': 0})
    df_allp_invt_Comb['CombinedBalance'] = df_allp_invt_Comb['Quantity'] + \
        df_allp_invt_Comb['quantity']
    df_allp_invt_Comb['TransBalance'] = df_allp_invt_Comb['quantity']
    # 填充 Product_key 缺失值,删除多余的product列
    df_allp_invt_Comb['Product_key'] = np.where(np.isnan(
        df_allp_invt_Comb['Product_key']), df_allp_invt_Comb['product_key'], df_allp_invt_Comb['Product_key'])
    df_allp_invt_Comb.drop(['product_key'], axis=1, inplace=True)

    # 1.3 抓取当天所有需求的产品主键（去重防止笛卡尔积, 链接 pk 到 df_allp_invt_Comb
    series_pk = df['product_key'].drop_duplicates()
    df_pk = pd.DataFrame(series_pk, columns=['product_key'])
    print('merging demand to df_allp_invt_Comb ... ')
    len_bf_merge = len(df_allp_invt_Comb)
    set_bf_merge = set(df_allp_invt_Comb['Product_key'])
    df_allp_invt_Comb = pd.merge(df_allp_invt_Comb, df_pk[[
                                 'product_key']], how='outer', left_on='Product_key', right_on='product_key')
    # QA
    if len(df_allp_invt_Comb) == len_bf_merge + len(df_pk) - len(set_bf_merge & set(df_pk['product_key'])):
        print("df_allp_invt_Comb QA is OK")
    else:
        print("WARNING 2: df_allp_invt_Comb QA is WRONG !!!!!!!  PLEASE CHECK !!!!!!!!!! ")
    print('row of df_allp_invt_Comb is ', len(df_allp_invt_Comb))
    # 填充CombinedBalance 和 TransBalance缺失值
    df_allp_invt_Comb = df_allp_invt_Comb.fillna(
        {'CombinedBalance': 0, 'TransBalance': 0})
    # 填充 Product_key 缺失值,删除多余的product列
    df_allp_invt_Comb['Product_key'] = np.where(pd.isnull(
        df_allp_invt_Comb['Product_key']), df_allp_invt_Comb['product_key'], df_allp_invt_Comb['Product_key'])
    df_allp_invt_Comb.drop(['product_key'], axis=1, inplace=True)

    # 1.4 链接 department_cd 到 df_allp_invt_Comb
    print('merging department to df_allp_invt_Comb ... ')
    df_allp_invt_Comb = pd.merge(df_allp_invt_Comb, df_department,
                                 how='left', left_on='Product_key', right_on='product_key')
    # 删除多余的product列
    df_allp_invt_Comb.drop(['product_key'], axis=1, inplace=True)
    # QA
    print('row of df_allp_invt_Comb is ', len(df_allp_invt_Comb))
    print('df_allp_invt_Comb is Ready ', len(df_allp_invt_Comb))

    # 三、生成订单
    tqdm.pandas()
    df = df.progress_apply(lambda row: fetchBalance(row, df_allp_invt_Comb), axis=1)
    # deleting the dummy row that was added before to avoid twice run on first row
    df = df.iloc[1:]

    return df, df_allp_invt_Comb


def to_integer(dt_time):

    return 10000*dt_time.year + 100*dt_time.month + dt_time.day


def orderkey(row):

    x = int(str(row['dt_key'])[2:])

    y = row['visit_key']

    a = math.floor(math.log10(y))

    return int(x*10**(1+a)+y)


def sales_orders(df_orders):

    # 找出所有 customer_key
    df_ck = df_orders['customer_key'].drop_duplicates()
    df_ck = df_ck.reset_index(drop=True)

    # 初始化一个空的DataFrame
    df_allc = pd.DataFrame()

    # 对于所有的 customer_key,按顺序取出每一个key
    for i in range(len(df_ck)):

        # get one customer's key
        ck = df_ck[i]

        # fetch all recourds of this customer
        df_oc = deepcopy(df_orders.loc[df_orders['customer_key'] == ck, :])
        df_oc = df_oc.reset_index(drop=True)

        # 初始化flag_ct
        flag_ct = 0

        # 统计 combine_flag <=2 (两个标准差左侧的面积) 的行数
        for j in range(len(df_oc)):

            if df_oc['combine_flag'][j] <= 2:

                flag_ct += 1

        # 判断 combine_flag <=2的行数 是否大于1
        if flag_ct > 1:

            # 获取combine_flag <=2 行数 的 最大order_key
            df_flag_ct = deepcopy(df_oc.loc[df_oc['combine_flag'] <= 2, :])
            max_orderkey = df_flag_ct['order_key'].max()

            # 在该顾客的记录中，将 combine_flag <=2 的记录的order_key 修改为最大order_key,剩余部分不变
            df_oc['order_key'] = np.where(
                df_oc['combine_flag'] <= 2, max_orderkey, df_oc['order_key'])

        # 将 df_op append到 df_allp
        df_allc = df_allc.append(df_oc)

    return df_allc

def InsertSalesData(engine, tablename, refresh_dt, df):

    # 查询当天是否已经有销售数据
    count = pd.read_sql(
        """ SELECT COUNT(*) as ct FROM {0} WHERE date = '{1}' """.format(tablename, refresh_dt), engine)

    # 如果当天已经有销售数据，删除掉
    if count['ct'][0] != 0:
        print("Deleting sales data from {0} on {1}".format(tablename, refresh_dt))
        with engine.begin() as conn:
            conn.execute(""" DELETE FROM {0} WHERE date = '{1}' """.format(tablename, refresh_dt))

    # 插入当天销售数据
    print("Inserting sales data into {0} on {1}".format(tablename, refresh_dt))
    df.to_sql('{0}'.format(tablename),if_exists='append', con=engine, index=False)

    # 查询当天销售数据
    result = engine.execute(
        """SELECT COUNT(*) as ct FROM {0} WHERE date = '{1}' """.format(tablename, refresh_dt)).fetchall()

    return result



# main program
if __name__ == '__main__':

    # df_visits, df_sales_order, rows_sales_order_details,df_allp_invt_Comb = main()
    
    try:
        df_visits, df_sales_order, rows_sales_order_details,df_allp_invt_Comb = main()
    except UnboundLocalError:
        print("Please try later.Or you can delete the last day sales data to test again. ")


time_end=time.time()
print('End at', time.strftime("%H:%M:%S") )
print('totally cost',(time_end-time_start)/60, 'mins')
print('#############################################')