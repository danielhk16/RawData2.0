
'''6.13.1测试 开始分割 ################################################################'''


def orderGenerate(tbl, refresh_dt):


df = df_visits.copy()

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

# 一、库存表准备部分
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
df_allp_invt_Comb = df_allp_invt_Comb.fillna({'Quantity': 0, 'quantity': 0})
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
df_allp_invt_Comb = pd.merge(
    df_allp_invt_Comb, df_pk[['product_key']], how='outer', left_on='Product_key', right_on='product_key')
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
'''6.13.1测试 以上ok分割  ################################################################'''


def fetchBalance(row):

    # ct = 0
    global df_allp_invt_Comb

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


tqdm.pandas()
df = df.progress_apply(lambda row: fetchBalance(row), axis=1)


# 🚧#🚧#🚧#🚧#🚧#🚧#🚧#🚧#🚧#🚧#🚧#🚧
