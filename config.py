CONFIG = {
    '4g_daily': {
        'type': '4g',
        'input_files': {
            'pm_cell': 'data/输入参数文件/pm_eutrancell_2.csv',
            'pm_rru': 'data/中间表/8-14/pm_lte_inventoryunitrru_2.csv',
            'cm_rru_cell': 'data/输入参数文件/cm_inventory_unit_rru_lte.csv',
            'dw_cell_info': 'data/输入参数文件/dw_necur_lte_cell_hb.csv'
        },
        'output_file': 'output/pm_erurancell_energy_consumption_2.csv',
        'date_filter': '2025-08-19', # 根据实际数据日期调整
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
    },
    '5g_daily': {
        'type': '5g',
        'input_files': {
            # 假设 pm_nrcellcu_0.csv 是需求文档中的 pm_nrcelldu_phy_0
            'pm_cell': 'data/输入参数文件/pm_nrcelldu_phy_2.csv', 
            'pm_rru': 'data/中间表/8-14/pm_nr_inventoryunitrru_2.csv',
            'cm_rru_cell': 'data/输入参数文件/cm_inventory_unit_rru_nr.csv',
            'dw_cell_info': 'data/输入参数文件/dw_necur_nr_cell_hb.csv'
        },
        'output_file': 'output/pm_nrcell_energy_consumption_2.csv',
        'date_filter': '2025-08-21', # 遵从需求文档，后续可能需要根据实际数据调整
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
        'final_cols_rename_map': {}, # 无需重命名
        'output_cols_order': [
            'starttime', 'nid', 'ncgi', 'cell_name', 'vendor_name', 'city_name',
            'ee_carriershutdowntime', 'ee_channelshutdowntime', 'ee_symbolshutdowntime',
            'ee_dlpoweropttime', 'ee_deepsleeptimerru', 'ee_shallowsleeptimerru',
            'ee_channelshutdowntimerru', 'ee_symbolshutdowntimerru', 'ee_supersleeptimerru'
        ]
    },
    '4g_station_daily': {
        'type': '4g_station',
        'input_files': {
            'pm_ne': 'data/中间表/8-14/pm_lte_managedelement_2.csv',
            'pm_bbu': 'data/中间表/8-14/pm_lte_inventoryunitshelf_2.csv', 
            'pm_rru': 'data/中间表/8-14/pm_lte_inventoryunitrru_2.csv',
            'cm_function': 'data/输入参数文件/cm_function_lte.csv',
            'dw_station_info': 'data/输入参数文件/dw_necur_lte_cell_hb.csv'
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
    },
    '5g_station_daily': {
        'type': '5g_station',
        'input_files': {
            'pm_ne': 'data/中间表/8-14/pm_nr_managedelement_2.csv',
            'pm_bbu': 'data/中间表/8-14/pm_nr_inventoryunitshelf_2.csv',
            'pm_bbu_pack': 'data/中间表/8-14/pm_nr_inventoryunitpack_2.csv',
            'pm_rru': 'data/中间表/8-14/pm_nr_inventoryunitrru_2.csv',
            'cm_function': 'data/输入参数文件/cm_function_nr.csv',
            'dw_station_info': 'data/输入参数文件/dw_necur_nr_cell_hb.csv'
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
}
