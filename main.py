import pandas as pd
import os
import json
from typing import Dict
from config import CONFIG

def load_data(file_path: str) -> pd.DataFrame:
    """加载CSV文件，并将列名标准化为小写。"""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"文件不存在: {file_path}")
    try:
        df = pd.read_csv(file_path, encoding='gbk', low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
    
    df.columns = [str(col).lower() for col in df.columns]
    return df

def parse_4g_cells(df: pd.DataFrame, col_name: str, id_col: str) -> pd.DataFrame:
    """解析4G的 related_cell_list 字段。"""
    df = df.copy()
    df.dropna(subset=[col_name], inplace=True)
    df[col_name] = df[col_name].str.strip('{}').str.split(r'\),\(')
    df = df.explode(col_name)
    df[id_col] = df[col_name].apply(lambda x: x.split(',')[0].replace('(', ''))
    return df

def parse_5g_cells(df: pd.DataFrame, col_name: str, id_col: str) -> pd.DataFrame:
    """解析5G的 relatednrcelldu 字段。"""
    df = df.copy()
    df.dropna(subset=[col_name], inplace=True)
    df[col_name] = df[col_name].str.strip('{}').str.split(r'\),\(')
    df = df.explode(col_name)
    # 提取ID并处理 "46000-" -> "460-00-" 的格式
    df[id_col] = df[col_name].apply(lambda x: x.split(',')[0].replace('(', '')).str.replace('46000-', '460-00-')
    return df

def map_rru_to_station_id(df: pd.DataFrame, cm_function_df: pd.DataFrame, is_5g: bool = False) -> pd.DataFrame:
    """将RRU的dn映射到基站ID（enbid或gnb_id），并提取rru_key。"""
    df = df.copy()
    
    # 提取dn的前三段作为匹配键
    df['dn_prefix'] = df['dn'].apply(lambda x: ','.join(x.split(',')[:3]) if isinstance(x, str) else '')
    cm_function_df['dn_prefix'] = cm_function_df['dn'].apply(lambda x: ','.join(x.split(',')[:3]) if isinstance(x, str) else '')
    
    # 提取rru_key（dn的最后一段）
    df['rru_key'] = df['dn'].apply(lambda x: x.split(',')[-1] if isinstance(x, str) else '')
    
    # 关联获取基站ID（根据4G/5G选择正确的字段名）
    if is_5g:
        station_col = 'gnb_id' if 'gnb_id' in cm_function_df.columns else 'gnodeb_id'
        target_col = 'gnb_id'
    else:
        station_col = 'enb_id' if 'enb_id' in cm_function_df.columns else 'enbid'
        target_col = 'enbid'
    
    mapped_df = pd.merge(df, cm_function_df[['dn_prefix', station_col]], on='dn_prefix', how='left')
    
    # 统一字段名
    if station_col != target_col:
        mapped_df.rename(columns={station_col: target_col}, inplace=True)
    
    # 清理4G基站ID中的460-00前缀
    if not is_5g:
        mapped_df[target_col] = mapped_df[target_col].astype(str).str.replace('460-00-', '', regex=False)
    
    return mapped_df

def generate_rru_list(df: pd.DataFrame, enbid_col: str, rru_key_col: str, value_col: str) -> pd.DataFrame:
    """生成RRU能耗清单JSONB字段。"""
    # 确保starttime_date字段存在
    if 'starttime_date' not in df.columns:
        df['starttime_date'] = df['starttime'].dt.date
    
    # 按enbid+日期+rru_key聚合
    df_agg = df.groupby([enbid_col, 'starttime_date', rru_key_col])[value_col].sum().reset_index()
    
    # 生成JSONB格式的RRU清单 - 改用更简单的方法
    result_list = []
    for (enbid, date), group in df_agg.groupby([enbid_col, 'starttime_date']):
        rru_dict = dict(zip(group[rru_key_col], group[value_col]))
        rru_json = json.dumps(rru_dict, ensure_ascii=False)
        result_list.append({enbid_col: enbid, 'starttime_date': date, 'ee_rrumeanpower_list': rru_json})
    
    return pd.DataFrame(result_list)

def process_data(config: Dict):
    """
    通用的天粒度能耗数据处理逻辑。
    """
    if config['type'] in ['4g_station', '5g_station']:
        process_station_data(config)
    else:
        process_cell_data(config)

def process_cell_data(config: Dict):
    """
    小区级天粒度能耗数据处理逻辑。
    """
    input_files = config['input_files']
    id_col = config['id_col']
    
    # 1. 从PM性能数据表取天粒度数据
    pm_cell_df = load_data(input_files['pm_cell'])
    
    pm_cell_df['starttime'] = pd.to_datetime(pm_cell_df['starttime'])
    daily_pm_cell_df = pm_cell_df[
        pm_cell_df['starttime'].dt.date == pd.to_datetime(config['date_filter']).date()
    ].copy()
    daily_pm_cell_df = daily_pm_cell_df[config['pm_cell_cols']]

    # 2. RRU -> 小区ID 映射 和指标聚合
    pm_rru_df = load_data(input_files['pm_rru'])
    cm_rru_cell_df = load_data(input_files['cm_rru_cell'])
    
    rru_cell_mapping_df = pd.merge(
        pm_rru_df[['dn', 'starttime'] + config['rru_agg_cols']],
        cm_rru_cell_df[['dn', config['cell_list_col']]],
        on='dn'
    )
    
    # 根据类型选择不同的解析函数
    if config['type'] == '4g':
        rru_id_df = parse_4g_cells(rru_cell_mapping_df, config['cell_list_col'], id_col)
    elif config['type'] == '5g':
        rru_id_df = parse_5g_cells(rru_cell_mapping_df, config['cell_list_col'], id_col)
    else:
        raise ValueError(f"未知的配置类型: {config['type']}")

    rru_id_df['starttime'] = pd.to_datetime(rru_id_df['starttime'])
    rru_agg_df = rru_id_df.groupby([id_col, pd.Grouper(key='starttime', freq='D')])[config['rru_agg_cols']].sum().reset_index()
    
    # 3. 合并数据
    daily_pm_cell_df['starttime_date'] = daily_pm_cell_df['starttime'].dt.date
    rru_agg_df['starttime_date'] = rru_agg_df['starttime'].dt.date

    merged_df = pd.merge(
        daily_pm_cell_df,
        rru_agg_df,
        on=[id_col, 'starttime_date'],
        how='left'
    )
    
    # 4. 补充工参信息
    dw_cell_info_df = load_data(input_files['dw_cell_info'])

    # 特殊处理：如果5G的工参表使用'cgi'而不是'ncgi'，则进行重命名以统一
    if config['type'] == '5g' and 'cgi' in dw_cell_info_df.columns and id_col not in dw_cell_info_df.columns:
        dw_cell_info_df.rename(columns={'cgi': id_col}, inplace=True)

    final_df = pd.merge(
        merged_df,
        dw_cell_info_df,
        on=id_col,
        how='left'
    )
    
    # 5. 整理输出字段
    if config.get('final_cols_rename_map'):
        final_df.rename(columns=config['final_cols_rename_map'], inplace=True)
    
    # 确保所有期望的列都存在，不存在的用NaN填充
    for col in config['output_cols_order']:
        if col not in final_df.columns:
            final_df[col] = None 
            
    final_df = final_df[config['output_cols_order']]
    
    final_df.to_csv(config['output_file'], index=False)
    print(f"处理完成，结果已保存至 {config['output_file']}")

def process_station_data(config: Dict):
    """
    基站级天粒度能耗数据处理逻辑（5.3/5.4需求）。
    """
    input_files = config['input_files']
    id_col = config['id_col']  # enbid 或 gnodeb_id
    is_5g = config['type'] == '5g_station'
    station_id_col = 'gnb_id' if is_5g else 'enbid'  # 数据表中的实际字段名
    
    # 1. 从NE数据表取天粒度数据
    pm_ne_df = load_data(input_files['pm_ne'])
    pm_ne_df['starttime'] = pd.to_datetime(pm_ne_df['starttime'])
    daily_ne_df = pm_ne_df[
        pm_ne_df['starttime'].dt.date == pd.to_datetime(config['date_filter']).date()
    ].copy()
    daily_ne_df = daily_ne_df[config['ne_cols']]
    # 清理基站ID中的460-00前缀（仅4G需要）
    if not is_5g:
        daily_ne_df[station_id_col] = daily_ne_df[station_id_col].astype(str).str.replace('460-00-', '', regex=False)
    
    # 2. 从BBU数据表取天粒度数据并聚合
    pm_bbu_df = load_data(input_files['pm_bbu'])
    pm_bbu_df['starttime'] = pd.to_datetime(pm_bbu_df['starttime'])
    daily_bbu_df = pm_bbu_df[
        pm_bbu_df['starttime'].dt.date == pd.to_datetime(config['date_filter']).date()
    ].copy()
    daily_bbu_df = daily_bbu_df[config['bbu_cols']]
    # 清理基站ID中的460-00前缀（仅4G需要）
    if not is_5g:
        daily_bbu_df[station_id_col] = daily_bbu_df[station_id_col].astype(str).str.replace('460-00-', '', regex=False)
    # 按基站ID+天聚合BBU能耗
    bbu_agg_df = daily_bbu_df.groupby([station_id_col, daily_bbu_df['starttime'].dt.date])['ee_bbumeanpower'].sum().reset_index()
    bbu_agg_df.columns = [station_id_col, 'starttime_date', 'ee_bbumeanpower']
    
    # 2.1 处理BBU休眠数据（仅5G）
    if is_5g and 'pm_bbu_pack' in input_files:
        pm_bbu_pack_df = load_data(input_files['pm_bbu_pack'])
        pm_bbu_pack_df['starttime'] = pd.to_datetime(pm_bbu_pack_df['starttime'])
        daily_bbu_pack_df = pm_bbu_pack_df[
            pm_bbu_pack_df['starttime'].dt.date == pd.to_datetime(config['date_filter']).date()
        ].copy()
        daily_bbu_pack_df = daily_bbu_pack_df[config['bbu_pack_cols']]
        # 按基站ID+天聚合BBU休眠指标
        bbu_pack_agg_df = daily_bbu_pack_df.groupby([station_id_col, daily_bbu_pack_df['starttime'].dt.date])[
            ['ee_channelshutdowntimebbu', 'ee_lowsvctimebbu', 'ee_symbolshutdowntimebbu']
        ].sum().reset_index()
        bbu_pack_agg_df.columns = [station_id_col, 'starttime_date', 'ee_channelshutdowntimebbu', 'ee_lowsvctimebbu', 'ee_symbolshutdowntimebbu']
        # 合并BBU能耗和休眠数据
        bbu_agg_df = pd.merge(bbu_agg_df, bbu_pack_agg_df, on=[station_id_col, 'starttime_date'], how='left')
    
    # 3. RRU -> 基站ID 映射和聚合
    pm_rru_df = load_data(input_files['pm_rru'])
    cm_function_df = load_data(input_files['cm_function'])
    
    pm_rru_df['starttime'] = pd.to_datetime(pm_rru_df['starttime'])
    daily_rru_df = pm_rru_df[
        pm_rru_df['starttime'].dt.date == pd.to_datetime(config['date_filter']).date()
    ].copy()
    daily_rru_df = daily_rru_df[config['rru_cols']]
    
    # 映射RRU到基站ID
    rru_mapped_df = map_rru_to_station_id(daily_rru_df, cm_function_df, is_5g)
    rru_mapped_df['starttime_date'] = rru_mapped_df['starttime'].dt.date
    
    # 过滤掉映射失败的记录（基站ID为NaN）
    actual_station_col = 'gnb_id' if is_5g else 'enbid'
    rru_mapped_df = rru_mapped_df.dropna(subset=[actual_station_col])
    
    # 生成RRU清单
    rru_list_df = generate_rru_list(rru_mapped_df, actual_station_col, 'rru_key', 'ee_rrumeanpower')
    
    # 按基站ID+天聚合RRU能耗
    rru_agg_df = rru_mapped_df.groupby([actual_station_col, 'starttime_date'])['ee_rrumeanpower'].sum().reset_index()
    
    # 4. 合并NE、BBU、RRU数据
    daily_ne_df['starttime_date'] = daily_ne_df['starttime'].dt.date
    
    # 以NE数据为主表，左关联BBU和RRU数据
    merged_df = pd.merge(daily_ne_df, bbu_agg_df, on=[station_id_col, 'starttime_date'], how='left')
    merged_df = pd.merge(merged_df, rru_agg_df, on=[actual_station_col, 'starttime_date'], how='left')
    merged_df = pd.merge(merged_df, rru_list_df, on=[actual_station_col, 'starttime_date'], how='left', suffixes=('', '_dup'))
    
    # 5. 补充工参信息
    dw_station_info_df = load_data(input_files['dw_station_info'])
    
    # 根据4G/5G选择正确的字段名
    if is_5g:
        # 5G使用gnodeb_id关联gnb_id
        if 'gnodeb_id' in dw_station_info_df.columns:
            dw_station_info_df.rename(columns={'gnodeb_id': station_id_col}, inplace=True)
        station_name_col = 'gnbduname'
        # 5G基站名取cell_name中的基站部分，去掉小区后缀
        if 'cell_name' in dw_station_info_df.columns:
            dw_station_info_df[station_name_col] = dw_station_info_df['cell_name'].str.replace(r'-\d+$', '', regex=True)
    else:
        # 4G使用enodeb_id关联enbid
        if 'enodeb_id' in dw_station_info_df.columns:
            dw_station_info_df.rename(columns={'enodeb_id': station_id_col}, inplace=True)
        station_name_col = 'enodeb_name'
        # 4G基站名取cell_name中的基站部分，去掉小区后缀
        if 'cell_name' in dw_station_info_df.columns:
            dw_station_info_df[station_name_col] = dw_station_info_df['cell_name'].str.replace(r'-\d+$', '', regex=True)
    
    # 确保基站ID字段类型一致（字符串类型）
    dw_station_info_df[station_id_col] = dw_station_info_df[station_id_col].astype(str)
    merged_df[actual_station_col] = merged_df[actual_station_col].astype(str)
    
    # 按基站聚合，取第一条记录的基站名、厂家、地市
    station_info_agg = dw_station_info_df.groupby(station_id_col).agg({
        station_name_col: 'first',
        'vendor_name': 'first', 
        'city_name': 'first'
    }).reset_index()
    
    # 需要将基站ID统一到输出字段名
    if station_id_col != id_col:
        station_info_agg.rename(columns={station_id_col: id_col}, inplace=True)
        merged_df.rename(columns={actual_station_col: id_col}, inplace=True)
    
    final_df = pd.merge(
        merged_df,
        station_info_agg[[id_col, station_name_col, 'vendor_name', 'city_name']],
        on=id_col,
        how='left'
    )
    
    # 6. 整理输出字段
    # 确保所有期望的列都存在，不存在的用NaN填充
    for col in config['output_cols_order']:
        if col not in final_df.columns:
            final_df[col] = None
            
    final_df = final_df[config['output_cols_order']]
    
    final_df.to_csv(config['output_file'], index=False)
    print(f"处理完成，结果已保存至 {config['output_file']}")

def main():
    """程序主入口"""
    for job_name, config in CONFIG.items():
        print(f"\n开始处理任务: {job_name}")
        try:
            process_data(config)
        except Exception as e:
            print(f"处理任务 {job_name} 时发生错误: {e}")

if __name__ == "__main__":
    main()
