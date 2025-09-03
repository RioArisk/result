import pandas as pd
import os
import json
from typing import Dict, Any
from abc import ABC, abstractmethod
from config import CONFIG

class DataProcessor(ABC):
    """数据处理器的抽象基类。"""
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.type = config.get('type', 'unknown')
        self.date_filter = pd.to_datetime(config['date_filter']).date()
        self.input_files = config['input_files']
        self.output_file = config['output_file']
        self.id_col = config['id_col']
        self.output_cols_order = config['output_cols_order']

    def load_data(self, file_key: str) -> pd.DataFrame:
        """从配置文件中按键加载CSV数据。"""
        file_path = self.input_files[file_key]
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"文件不存在: {file_path}")

        try:
            df = pd.read_csv(file_path, encoding='gbk', low_memory=False)
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)

        df.columns = [str(col).lower() for col in df.columns]
        return df

    def _get_daily_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """根据日期过滤数据。"""
        df['starttime'] = pd.to_datetime(df['starttime'])
        return df[df['starttime'].dt.date == self.date_filter].copy()

    def save_output(self, df: pd.DataFrame):
        """保存处理结果到CSV文件。"""
        # 确保所有期望的列都存在，不存在的用NaN填充
        for col in self.output_cols_order:
            if col not in df.columns:
                df[col] = None

        df = df[self.output_cols_order]

        # 创建输出目录（如果不存在）
        output_dir = os.path.dirname(self.output_file)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        df.to_csv(self.output_file, index=False)
        print(f"处理完成，结果已保存至 {self.output_file}")

    @abstractmethod
    def process(self):
        """处理数据的主逻辑，由子类实现。"""
        pass

class CellDataProcessor(DataProcessor):
    """小区级能耗数据处理器。"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.cell_list_col = config['cell_list_col']
        self.rru_agg_cols = config['rru_agg_cols']
        self.pm_cell_cols = config['pm_cell_cols']
        self.is_5g = self.type == '5g'

    def _parse_cells(self, df: pd.DataFrame) -> pd.DataFrame:
        """根据4G/5G类型解析 related_cell 字段。"""
        df = df.copy()
        df.dropna(subset=[self.cell_list_col], inplace=True)
        df[self.cell_list_col] = df[self.cell_list_col].str.strip('{}').str.split(r'\),\(')
        df = df.explode(self.cell_list_col)

        if self.is_5g:
            # 5G: 提取ID并处理 "46000-" -> "460-00-" 的格式
            df[self.id_col] = df[self.cell_list_col].apply(lambda x: x.split(',')[0].replace('(', '')).str.replace('46000-', '460-00-')
        else:
            # 4G
            df[self.id_col] = df[self.cell_list_col].apply(lambda x: x.split(',')[0].replace('(', ''))
        return df

    def process(self):
        # 1. 加载并处理PM性能数据
        pm_cell_df = self.load_data('pm_cell')
        daily_pm_cell_df = self._get_daily_data(pm_cell_df)
        daily_pm_cell_df = daily_pm_cell_df[self.pm_cell_cols]

        # 2. RRU -> 小区ID 映射 和指标聚合
        pm_rru_df = self.load_data('pm_rru')
        cm_rru_cell_df = self.load_data('cm_rru_cell')

        rru_cell_mapping_df = pd.merge(
            pm_rru_df[['dn', 'starttime'] + self.rru_agg_cols],
            cm_rru_cell_df[['dn', self.cell_list_col]],
            on='dn'
        )

        rru_id_df = self._parse_cells(rru_cell_mapping_df)

        rru_id_df['starttime'] = pd.to_datetime(rru_id_df['starttime'])
        rru_agg_df = rru_id_df.groupby([self.id_col, pd.Grouper(key='starttime', freq='D')])[self.rru_agg_cols].sum().reset_index()

        # 3. 合并数据
        daily_pm_cell_df['starttime_date'] = daily_pm_cell_df['starttime'].dt.date
        rru_agg_df['starttime_date'] = rru_agg_df['starttime'].dt.date

        merged_df = pd.merge(
            daily_pm_cell_df,
            rru_agg_df,
            on=[self.id_col, 'starttime_date'],
            how='left'
        )

        # 4. 补充工参信息
        dw_cell_info_df = self.load_data('dw_cell_info')
        if self.is_5g and 'cgi' in dw_cell_info_df.columns and self.id_col not in dw_cell_info_df.columns:
            dw_cell_info_df.rename(columns={'cgi': self.id_col}, inplace=True)

        final_df = pd.merge(
            merged_df,
            dw_cell_info_df,
            on=self.id_col,
            how='left'
        )

        # 5. 整理输出字段
        if self.config.get('final_cols_rename_map'):
            final_df.rename(columns=self.config['final_cols_rename_map'], inplace=True)
            
        self.save_output(final_df)

class StationDataProcessor(DataProcessor):
    """基站级能耗数据处理器。"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.is_5g = self.type == '5g_station'
        self.station_id_col = 'gnb_id' if self.is_5g else 'enbid'

    def _map_rru_to_station_id(self, df: pd.DataFrame, cm_function_df: pd.DataFrame) -> pd.DataFrame:
        """将RRU的dn映射到基站ID，并提取rru_key。"""
        df = df.copy()
        df['dn_prefix'] = df['dn'].apply(lambda x: ','.join(x.split(',')[:3]) if isinstance(x, str) else '')
        cm_function_df['dn_prefix'] = cm_function_df['dn'].apply(lambda x: ','.join(x.split(',')[:3]) if isinstance(x, str) else '')
        df['rru_key'] = df['dn'].apply(lambda x: x.split(',')[-1] if isinstance(x, str) else '')

        station_col = 'gnb_id' if self.is_5g and 'gnb_id' in cm_function_df.columns else 'gnodeb_id' if self.is_5g else 'enb_id' if 'enb_id' in cm_function_df.columns else 'enbid'

        mapped_df = pd.merge(df, cm_function_df[['dn_prefix', station_col]], on='dn_prefix', how='left')

        if station_col != self.station_id_col:
            mapped_df.rename(columns={station_col: self.station_id_col}, inplace=True)

        if not self.is_5g:
            mapped_df[self.station_id_col] = mapped_df[self.station_id_col].astype(str).str.replace('460-00-', '', regex=False)

        return mapped_df

    def _generate_rru_list(self, df: pd.DataFrame) -> pd.DataFrame:
        """生成RRU能耗清单JSONB字段。"""
        if 'starttime_date' not in df.columns:
            df['starttime_date'] = df['starttime'].dt.date

        df_agg = df.groupby([self.station_id_col, 'starttime_date', 'rru_key'])['ee_rrumeanpower'].sum().reset_index()

        result_list = []
        for (enbid, date), group in df_agg.groupby([self.station_id_col, 'starttime_date']):
            rru_dict = dict(zip(group['rru_key'], group['ee_rrumeanpower']))
            rru_json = json.dumps(rru_dict, ensure_ascii=False)
            result_list.append({self.station_id_col: enbid, 'starttime_date': date, 'ee_rrumeanpower_list': rru_json})

        return pd.DataFrame(result_list)

    def process(self):
        # 1. 处理NE数据
        pm_ne_df = self.load_data('pm_ne')
        daily_ne_df = self._get_daily_data(pm_ne_df)
        daily_ne_df = daily_ne_df[self.config['ne_cols']]
        if not self.is_5g:
            daily_ne_df[self.station_id_col] = daily_ne_df[self.station_id_col].astype(str).str.replace('460-00-', '', regex=False)

        # 2. 处理BBU数据
        pm_bbu_df = self.load_data('pm_bbu')
        daily_bbu_df = self._get_daily_data(pm_bbu_df)
        daily_bbu_df = daily_bbu_df[self.config['bbu_cols']]
        if not self.is_5g:
            daily_bbu_df[self.station_id_col] = daily_bbu_df[self.station_id_col].astype(str).str.replace('460-00-', '', regex=False)

        bbu_agg_df = daily_bbu_df.groupby([self.station_id_col, daily_bbu_df['starttime'].dt.date])['ee_bbumeanpower'].sum().reset_index()
        bbu_agg_df.columns = [self.station_id_col, 'starttime_date', 'ee_bbumeanpower']

        if self.is_5g and 'pm_bbu_pack' in self.input_files:
            pm_bbu_pack_df = self.load_data('pm_bbu_pack')
            daily_bbu_pack_df = self._get_daily_data(pm_bbu_pack_df)
            daily_bbu_pack_df = daily_bbu_pack_df[self.config['bbu_pack_cols']]
            bbu_pack_agg_df = daily_bbu_pack_df.groupby([self.station_id_col, daily_bbu_pack_df['starttime'].dt.date])[
                ['ee_channelshutdowntimebbu', 'ee_lowsvctimebbu', 'ee_symbolshutdowntimebbu']
            ].sum().reset_index()
            bbu_pack_agg_df.columns = [self.station_id_col, 'starttime_date', 'ee_channelshutdowntimebbu', 'ee_lowsvctimebbu', 'ee_symbolshutdowntimebbu']
            bbu_agg_df = pd.merge(bbu_agg_df, bbu_pack_agg_df, on=[self.station_id_col, 'starttime_date'], how='left')

        # 3. 处理RRU数据
        pm_rru_df = self.load_data('pm_rru')
        cm_function_df = self.load_data('cm_function')
        daily_rru_df = self._get_daily_data(pm_rru_df)
        daily_rru_df = daily_rru_df[self.config['rru_cols']]

        rru_mapped_df = self._map_rru_to_station_id(daily_rru_df, cm_function_df)
        rru_mapped_df['starttime_date'] = rru_mapped_df['starttime'].dt.date
        rru_mapped_df = rru_mapped_df.dropna(subset=[self.station_id_col])

        rru_list_df = self._generate_rru_list(rru_mapped_df)
        rru_agg_df = rru_mapped_df.groupby([self.station_id_col, 'starttime_date'])['ee_rrumeanpower'].sum().reset_index()

        # 4. 合并数据
        daily_ne_df['starttime_date'] = daily_ne_df['starttime'].dt.date
        merged_df = pd.merge(daily_ne_df, bbu_agg_df, on=[self.station_id_col, 'starttime_date'], how='left')
        merged_df = pd.merge(merged_df, rru_agg_df, on=[self.station_id_col, 'starttime_date'], how='left')
        merged_df = pd.merge(merged_df, rru_list_df, on=[self.station_id_col, 'starttime_date'], how='left', suffixes=('', '_dup'))

        # 5. 补充工参信息
        dw_station_info_df = self.load_data('dw_station_info')

        if self.is_5g:
            if 'gnodeb_id' in dw_station_info_df.columns:
                dw_station_info_df.rename(columns={'gnodeb_id': self.station_id_col}, inplace=True)
            station_name_col = 'gnbduname'
        else:
            if 'enodeb_id' in dw_station_info_df.columns:
                dw_station_info_df.rename(columns={'enodeb_id': self.station_id_col}, inplace=True)
            station_name_col = 'enodeb_name'

        if 'cell_name' in dw_station_info_df.columns:
            dw_station_info_df[station_name_col] = dw_station_info_df['cell_name'].str.replace(r'-\d+$', '', regex=True)

        dw_station_info_df[self.station_id_col] = dw_station_info_df[self.station_id_col].astype(str)
        merged_df[self.station_id_col] = merged_df[self.station_id_col].astype(str)

        station_info_agg = dw_station_info_df.groupby(self.station_id_col).agg({
            station_name_col: 'first', 'vendor_name': 'first', 'city_name': 'first'
        }).reset_index()

        if self.station_id_col != self.id_col:
            station_info_agg.rename(columns={self.station_id_col: self.id_col}, inplace=True)
            merged_df.rename(columns={self.station_id_col: self.id_col}, inplace=True)

        final_df = pd.merge(merged_df, station_info_agg, on=self.id_col, how='left')

        self.save_output(final_df)

def get_processor(config: Dict[str, Any]) -> DataProcessor:
    """根据配置类型返回相应的数据处理器实例。"""
    processor_type = config.get('type', 'unknown')
    if processor_type in ['4g', '5g']:
        return CellDataProcessor(config)
    elif processor_type in ['4g_station', '5g_station']:
        return StationDataProcessor(config)
    else:
        raise ValueError(f"未知的处理器类型: {processor_type}")

def main():
    """程序主入口"""
    # 确保输出目录存在
    if not os.path.exists('output'):
        os.makedirs('output')

    for job_name, config in CONFIG.items():
        print(f"\n开始处理任务: {job_name}")
        try:
            processor = get_processor(config)
            processor.process()
        except FileNotFoundError as e:
            print(f"处理任务 {job_name} 时发生文件未找到错误: {e}")
        except Exception as e:
            print(f"处理任务 {job_name} 时发生未知错误: {e}")

if __name__ == "__main__":
    main()
