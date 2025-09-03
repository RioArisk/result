from copy import deepcopy

def merge_configs(base, override):
    """递归合并两个字典。"""
    result = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and key in result and isinstance(result[key], dict):
            result[key] = merge_configs(result[key], value)
        else:
            result[key] = value
    return result

# --- 基础模板 ---
BASE_CONFIG = {
    'input_files': {
        'dw_cell_info': 'data/输入参数文件/dw_necur_lte_cell_hb.csv',
        'dw_station_info': 'data/输入参数文件/dw_necur_lte_cell_hb.csv' # 假设基站和 小区工参是同一个文件
    },
    'date_filter': '2025-01-01' # 默认日期，每个任务应覆盖
}

# --- 技术类型模板 (4G/5G) ---
TECH_4G_BASE = {
    'input_files': {
        'pm_rru': 'data/中间表/8-14/pm_lte_inventoryunitrru_2.csv',
        'cm_rru_cell': 'data/输入参数文件/cm_inventory_unit_rru_lte.csv',
        'cm_function': 'data/输入参数文件/cm_function_lte.csv',
    }
}

TECH_5G_BASE = {
    'input_files': {
        'pm_rru': 'data/中间表/8-14/pm_nr_inventoryunitrru_2.csv',
        'cm_rru_cell': 'data/输入参数文件/cm_inventory_unit_rru_nr.csv',
        'cm_function': 'data/输入参数文件/cm_function_nr.csv',
        'dw_cell_info': 'data/输入参数文件/dw_necur_nr_cell_hb.csv',
        'dw_station_info': 'data/输入参数文件/dw_necur_nr_cell_hb.csv',
    }
}

# --- 处理器类型模板 (Cell/Station) ---
PROCESSOR_CELL_BASE = {
    'type': 'cell', # 会被 '4g' 或 '5g' 覆盖
    'final_cols_rename_map': {},
}

PROCESSOR_STATION_BASE = {
    'type': 'station', # 会被 '4g_station' 或 '5g_station' 覆盖
}

# --- 具体任务配置 ---

# 4G 小区 (Cell)
config_4g_cell = {
    'type': '4g',
    'input_files': {
        'pm_cell': 'data/输入参数文件/pm_eutrancell_2.csv',
    },
    'output_file': 'output/pm_erurancell_energy_consumption_2.csv',
    'date_filter': '2025-08-19',
    'id_col': 'cgi',
    'cell_list_col': 'related_cell_list',
    'rru_agg_cols': ['ee_deepsleeptime'],
    'pm_cell_cols': [
        'starttime', 'nid', 'cgi', 'ee_carriershutdowntime',
        'ee_channelshutdowntime', 'ee_slotshutdowntime'
    ],
    'final_cols_rename_map': {'ee_deepsleeptime': 'ee_deepsleeptime'},
    'output_cols_order': [
        'starttime', 'nid', 'cgi', 'cell_name', 'vendor_name', 'city_name',
        'ee_carriershutdowntime', 'ee_channelshutdowntime', 'ee_slotshutdowntime',
        'ee_deepsleeptime'
    ]
}

# 5G 小区 (Cell)
config_5g_cell = {
    'type': '5g',
    'input_files': {
        'pm_cell': 'data/输入参数文件/pm_nrcelldu_phy_2.csv',
    },
    'output_file': 'output/pm_nrcell_energy_consumption_2.csv',
    'date_filter': '2025-08-21',
    'id_col': 'ncgi',
    'cell_list_col': 'related_nr_cell_du',
    'rru_agg_cols': [
        'ee_deepsleeptimerru', 'ee_shallowsleeptimerru', 'ee_channelshutdowntimerru',
        'ee_symbolshutdowntimerru', 'ee_supersleeptimerru'
    ],
    'pm_cell_cols': [
        'starttime', 'nid', 'ncgi', 'ee_carriershutdowntime',
        'ee_channelshutdowntime', 'ee_symbolshutdowntime', 'ee_dlpoweropttime'
    ],
    'output_cols_order': [
        'starttime', 'nid', 'ncgi', 'cell_name', 'vendor_name', 'city_name',
        'ee_carriershutdowntime', 'ee_channelshutdowntime', 'ee_symbolshutdowntime',
        'ee_dlpoweropttime', 'ee_deepsleeptimerru', 'ee_shallowsleeptimerru',
        'ee_channelshutdowntimerru', 'ee_symbolshutdowntimerru', 'ee_supersleeptimerru'
    ]
}

# 4G 基站 (Station)
config_4g_station = {
    'type': '4g_station',
    'input_files': {
        'pm_ne': 'data/中间表/8-14/pm_lte_managedelement_2.csv',
        'pm_bbu': 'data/中间表/8-14/pm_lte_inventoryunitshelf_2.csv',
    },
    'output_file': 'output/pm_eutran_energy_consumption_2.csv',
    'date_filter': '2025-04-25',
    'id_col': 'enbid',
    'ne_cols': ['starttime', 'nid', 'enbid', 'ee_nemeanpower', 'ee_ltenemeanpower', 'ee_nbnemeanpower', 'ee_gsmnemeanpower'],
    'bbu_cols': ['starttime', 'enbid', 'ee_bbumeanpower'],
    'rru_cols': ['starttime', 'dn', 'ee_rrumeanpower', 'ee_deepsleeptime', 'phy_rrumaxtxpower', 'phy_rrumeantxpower'],
    'function_cols': ['dn', 'enb_id'],
    'output_cols_order': [
        'starttime', 'enbid', 'nid', 'enodeb_name', 'vendor_name', 'city_name',
        'ee_gsmnemeanpower', 'ee_ltenemeanpower', 'ee_nbnemeanpower', 'ee_nemeanpower',
        'ee_bbumeanpower', 'ee_rrumeanpower', 'ee_rrumeanpower_list'
    ]
}

# 5G 基站 (Station)
config_5g_station = {
    'type': '5g_station',
    'input_files': {
        'pm_ne': 'data/中间表/8-14/pm_nr_managedelement_2.csv',
        'pm_bbu': 'data/中间表/8-14/pm_nr_inventoryunitshelf_2.csv',
        'pm_bbu_pack': 'data/中间表/8-14/pm_nr_inventoryunitpack_2.csv',
    },
    'output_file': 'output/pm_nr_energy_consumption_2.csv',
    'date_filter': '2025-04-25',
    'id_col': 'gnodeb_id',
    'ne_cols': ['starttime', 'nid', 'gnb_id', 'ee_nemeanpower', 'ee_nrnemeanpower'],
    'bbu_cols': ['starttime', 'gnb_id', 'ee_bbumeanpower'],
    'bbu_pack_cols': ['starttime', 'gnb_id', 'ee_channelshutdowntimebbu', 'ee_lowsvctimebbu', 'ee_symbolshutdowntimebbu'],
    'rru_cols': ['starttime', 'dn', 'ee_rrumeanpower'],
    'function_cols': ['dn', 'gnb_id'],
    'output_cols_order': [
        'starttime', 'gnodeb_id', 'nid', 'gnbduname', 'vendor_name', 'city_name',
        'ee_nemeanpower', 'ee_nrnemeanpower', 'ee_bbumeanpower',
        'ee_channelshutdowntimebbu', 'ee_lowsvctimebbu', 'ee_symbolshutdowntimebbu',
        'ee_rrumeanpower', 'ee_rrumeanpower_list'
    ]
}


# --- 最终导出的CONFIG ---
CONFIG = {
    '4g_daily': merge_configs(
        merge_configs(BASE_CONFIG, TECH_4G_BASE),
        merge_configs(PROCESSOR_CELL_BASE, config_4g_cell)
    ),
    '5g_daily': merge_configs(
        merge_configs(BASE_CONFIG, TECH_5G_BASE),
        merge_configs(PROCESSOR_CELL_BASE, config_5g_cell)
    ),
    '4g_station_daily': merge_configs(
        merge_configs(BASE_CONFIG, TECH_4G_BASE),
        merge_configs(PROCESSOR_STATION_BASE, config_4g_station)
    ),
    '5g_station_daily': merge_configs(
        merge_configs(BASE_CONFIG, TECH_5G_BASE),
        merge_configs(PROCESSOR_STATION_BASE, config_5g_station)
    )
}
