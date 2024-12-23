'''
테이블에 적합한 형태로 변환
'''
import pandas as pd

def change_tms_df(raw_df:pd.DataFrame):
    tms_df = raw_df[['traveler_id', 'age', 'gender', 'travel_status_res', 'travel_status_acr']]
    tms_df['age'] = tms_df['age'].replace('', 0)
    tms_df.loc[:, 'age'] = tms_df['age'].fillna(0).astype(int)
    tms_df.loc[:, 'age_group'] = tms_df['age'].apply(lambda x : x // 10 * 10)
    tms_df = tms_df.drop('age', axis=1)
    return tms_df, 'traveler_master'

def change_t_df(raw_df):
    t_df = raw_df[['travel_id','traveler_id','mvmn_nm', 'travel_reason']]
    return t_df, 'travel'

def change_ta_df(raw_df):
    row_convert_ls = []

    for _, row in raw_df.iterrows():
        if row['visit_areas'] != row['visit_areas']:
            continue
        else:
            visit_area_ls = row['visit_areas'].split(',')
            for order, visit_area in enumerate(visit_area_ls):
                app_dict = {}
                app_dict['travel_id'] = row['travel_id']
                app_dict['visit_order'] = order
                app_dict['visit_area_nm'] = visit_area
                app_dict['visit_ymd'] = row['visit_ymd']
                row_convert_ls.append(app_dict)
                
    ta_df = pd.DataFrame(row_convert_ls)
    return ta_df, 'visit_area_info'