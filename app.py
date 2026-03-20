import streamlit as st
import pandas as pd
import numpy as np
import re

# ================= 页面全局配置 =================
st.set_page_config(page_title="玖月美妆 - 全渠道库存透视中心", layout="wide")

# ================= 辅助函数：抓取所有可用仓库 =================
@st.cache_data
def get_all_warehouses():
    try:
        df = pd.read_csv("全渠道库存V3.csv", usecols=['仓库名称'])
        return df['仓库名称'].dropna().unique().tolist()
    except:
        return ['仓库-A', '仓库-I', '仓库-H', '仓库-F', '仓库-L']

# ================= 核心数据处理逻辑 =================
@st.cache_data
def load_and_process_data(selected_warehouses):
    master_path = "全渠道库存V3.csv"
    dy_inv_path = "抖音在架库存V1.csv"
    dy_combo_path = "抖音-虚拟套组v1.csv"
    tm_inv_path = "天猫在售库存.csv"
    tm_combo_path = "天猫虚拟套组.csv"
    xhs_inv_path = "小红书在售库存.csv"
    xhs_combo_path = "小红书虚拟套组.csv"
    sales_path = "全渠道7天销量.csv"

    # 容错读取，避免文件缺失报错
    try: df_master = pd.read_csv(master_path)
    except: df_master = pd.DataFrame(columns=['商品编码', '商品名称', '仓库名称', '库存状态', '渠道', '可用数量', '在途数量', '换货在途数量'])
    
    try: df_dy_inv = pd.read_csv(dy_inv_path)
    except: df_dy_inv = pd.DataFrame(columns=['商家编码', '现货可售'])
    try: df_dy_combo = pd.read_csv(dy_combo_path)
    except: df_dy_combo = pd.DataFrame(columns=['套餐编码', '商品编码', '明细数量'])
    
    try: df_tm_inv = pd.read_csv(tm_inv_path)
    except: df_tm_inv = pd.DataFrame(columns=['商品编码', '可售库存'])
    try: df_tm_combo = pd.read_csv(tm_combo_path)
    except: df_tm_combo = pd.DataFrame(columns=['套餐编码', '商品编码', '明细数量'])
    
    try: df_xhs_inv = pd.read_csv(xhs_inv_path)
    except: df_xhs_inv = pd.DataFrame(columns=['商家编码', '库存'])
    try: df_xhs_combo = pd.read_csv(xhs_combo_path)
    except: df_xhs_combo = pd.DataFrame(columns=['套餐编码', '商品编码', '明细数量'])

    try: df_sales = pd.read_csv(sales_path)
    except: df_sales = pd.DataFrame(columns=['渠道', '前端销售Code', '7天销量'])

    # ================= 1. 清洗总库存表 =================
    valid_status = ['可售']
    valid_channels = ['TM', 'DYXD', '天猫', 'RED', 'KWAI']
    
    df_master_filtered = df_master[
        (df_master['库存状态'].isin(valid_status)) &
        (df_master['仓库名称'].isin(selected_warehouses)) &
        (df_master['渠道'].astype(str).str.upper().isin(valid_channels))
    ].copy()
    
    for col in ['可用数量', '在途数量', '换货在途数量']:
        if col in df_master_filtered.columns:
            df_master_filtered[col] = pd.to_numeric(df_master_filtered[col], errors='coerce').fillna(0)
        else:
            df_master_filtered[col] = 0
            
    df_master_filtered['TTL'] = df_master_filtered['可用数量'] + df_master_filtered['在途数量'] + df_master_filtered['换货在途数量']
    df_master_agg = df_master_filtered.groupby(['商品编码', '商品名称'], as_index=False)[['可用数量', '在途数量', '换货在途数量', 'TTL']].sum()

    # ================= 2. 整合全渠道套组 =================
    combo_all = pd.concat([
        df_dy_combo[['套餐编码', '商品编码', '明细数量']],
        df_tm_combo[['套餐编码', '商品编码', '明细数量']],
        df_xhs_combo[['套餐编码', '商品编码', '明细数量']]
    ]).drop_duplicates()
    combo_all['明细数量'] = pd.to_numeric(combo_all['明细数量'], errors='coerce').fillna(0)
    combo_all['套餐编码'] = combo_all['套餐编码'].astype(str).str.strip()
    combo_all['商品编码'] = combo_all['商品编码'].astype(str).str.strip()

    # ================= 3. 通用渠道拆解引擎 (适用于库存和销量) =================
    def process_channel(df_input, df_combo, code_col, qty_col):
        if df_input.empty: return pd.DataFrame(columns=['bottom_code', 'consumed_qty'])
        df_proc = df_input[[code_col, qty_col]].copy()
        df_proc.rename(columns={code_col: 'merchant_code', qty_col: 'qty'}, inplace=True)
        df_proc['qty'] = pd.to_numeric(df_proc['qty'], errors='coerce').fillna(0)
        
        if df_combo.empty:
            combo = pd.DataFrame(columns=['套餐编码', '商品编码', '明细数量'])
        else:
            combo = df_combo[['套餐编码', '商品编码', '明细数量']].copy()
            combo['明细数量'] = pd.to_numeric(combo['明细数量'], errors='coerce').fillna(0)
        
        df_proc['merchant_code'] = df_proc['merchant_code'].astype(str).str.strip()
        df_proc['is_bottom'] = df_proc['merchant_code'].str.startswith('H', na=False)
        
        bottom_df = df_proc[df_proc['is_bottom']].copy()
        bottom_df['bottom_code'] = bottom_df['merchant_code']
        bottom_df['consumed_qty'] = bottom_df['qty']
        
        virtual_df = df_proc[~df_proc['is_bottom']].copy()
        if not combo.empty:
            merged = pd.merge(virtual_df, combo, left_on='merchant_code', right_on='套餐编码', how='left')
            # 💡 核心逻辑：这里确保了消耗量 = 套组前端数量 × 明细数量
            merged['consumed_qty'] = merged['qty'] * merged['明细数量']
            merged['bottom_code'] = merged['商品编码']
        else:
            merged = virtual_df.copy()
            merged['bottom_code'] = merged['merchant_code']
            merged['consumed_qty'] = merged['qty']
            
        unmatched = merged['bottom_code'].isna()
        merged.loc[unmatched, 'bottom_code'] = merged.loc[unmatched, 'merchant_code']
        merged.loc[unmatched, 'consumed_qty'] = merged.loc[unmatched, 'qty']
        
        final_df = pd.concat([bottom_df[['bottom_code', 'consumed_qty']], merged[['bottom_code', 'consumed_qty']]])
        return final_df.groupby('bottom_code', as_index=False)['consumed_qty'].sum()

    # == 计算底层占用库存 ==
    dy_res = process_channel(df_dy_inv, df_dy_combo, '商家编码', '现货可售').rename(columns={'consumed_qty': '抖音占用'})
    tm_res = process_channel(df_tm_inv, df_tm_combo, '商品编码', '可售库存').rename(columns={'consumed_qty': '天猫占用'})
    xhs_res = process_channel(df_xhs_inv, df_xhs_combo, '商家编码', '库存').rename(columns={'consumed_qty': '小红书占用'})

    # == 核心新功能：计算底层的7天真实销量 ==
    dy_sales_res = process_channel(df_sales[df_sales['渠道'] == 'DYXD'], df_dy_combo, '前端销售Code', '7天销量').rename(columns={'consumed_qty': 'DY7天销量'})
    tm_sales_res = process_channel(df_sales[df_sales['渠道'] == 'TM'], df_tm_combo, '前端销售Code', '7天销量').rename(columns={'consumed_qty': 'TM7天销量'})
    xhs_sales_res = process_channel(df_sales[df_sales['渠道'] == 'RED'], df_xhs_combo, '前端销售Code', '7天销量').rename(columns={'consumed_qty': 'RED7天销量'})

    # ================= 4. 大盘融合比对 =================
    df_final = df_master_agg.copy()
    if df_final.empty:
        return df_final, combo_all
        
    df_final['商品编码'] = df_final['商品编码'].astype(str).str.strip()
    
    # 合并库存占用
    for res_df in [dy_res, tm_res, xhs_res, dy_sales_res, tm_sales_res, xhs_sales_res]:
        if not res_df.empty: res_df['bottom_code'] = res_df['bottom_code'].astype(str).str.strip()

    df_final = pd.merge(df_final, dy_res, left_on='商品编码', right_on='bottom_code', how='left').drop(columns=['bottom_code'], errors='ignore')
    df_final = pd.merge(df_final, tm_res, left_on='商品编码', right_on='bottom_code', how='left').drop(columns=['bottom_code'], errors='ignore')
    df_final = pd.merge(df_final, xhs_res, left_on='商品编码', right_on='bottom_code', how='left').drop(columns=['bottom_code'], errors='ignore')

    # 合并销量表现
    df_final = pd.merge(df_final, dy_sales_res, left_on='商品编码', right_on='bottom_code', how='left').drop(columns=['bottom_code'], errors='ignore')
    df_final = pd.merge(df_final, tm_sales_res, left_on='商品编码', right_on='bottom_code', how='left').drop(columns=['bottom_code'], errors='ignore')
    df_final = pd.merge(df_final, xhs_sales_res, left_on='商品编码', right_on='bottom_code', how='left').drop(columns=['bottom_code'], errors='ignore')

    # 计算各项占用的基础信息
    for col in ['抖音占用', '天猫占用', '小红书占用']:
        if col not in df_final.columns: df_final[col] = 0
        df_final[col] = df_final[col].fillna(0).astype(int)

    df_final['全渠道总占用'] = df_final['抖音占用'] + df_final['天猫占用'] + df_final['小红书占用']
    df_final['剩余可分配库存'] = df_final['TTL'] - df_final['全渠道总占用']

    # 计算各渠道的准确日均销量
    for col in ['DY7天销量', 'TM7天销量', 'RED7天销量']:
        if col not in df_final.columns: df_final[col] = 0
        
    df_final['DY日均销量'] = (pd.to_numeric(df_final['DY7天销量'], errors='coerce').fillna(0) / 7.0).round(2)
    df_final['TM日均销量'] = (pd.to_numeric(df_final['TM7天销量'], errors='coerce').fillna(0) / 7.0).round(2)
    df_final['RED日均销量'] = (pd.to_numeric(df_final['RED7天销量'], errors='coerce').fillna(0) / 7.0).round(2)
    df_final['日均销量'] = df_final['DY日均销量'] + df_final['TM日均销量'] + df_final['RED日均销量']
    
    def clean_product_name(name):
        name_clean = str(name).replace(' ', '')
        if '-' in name_clean:
            parts = name_clean.split('-')
            if len(parts) >= 3: name_clean = parts[1]
            elif len(parts) == 2: name_clean = parts[1]
        name_clean = name_clean.replace('玖月', '')
        name_clean = re.sub(r'(样|非|会员|正装|赠品|小样)$', '', name_clean)
        return name_clean
        
    df_final['匹配核名称'] = df_final['商品名称'].apply(clean_product_name)
    df_final = df_final[(df_final['TTL'] > 0) | (df_final['全渠道总占用'] > 0)]
    df_final = df_final.sort_values(by='剩余可分配库存', ascending=True)

    return df_final, combo_all

# ================= 动态列视图助手（处理多级表头与隐藏列） =================
def format_display_df(df, show_dy, show_tm, show_xhs):
    if df.empty: return df
    
    # 💡 核心修改：在表头映射字典里加入对 明细数量 的支持
    header_mapping = {
        '商品编码': '📦 基础库存概览', '商品名称': '📦 基础库存概览',
        '明细数量': '📦 基础库存概览',  # 新增
        '可用数量': '📦 基础库存概览', '在途数量': '📦 基础库存概览',
        '换货在途数量': '📦 基础库存概览', 'TTL': '📦 基础库存概览',
        
        '全渠道总占用': '📊 核心调配指标', '剩余可分配库存': '📊 核心调配指标',
        '库存状态诊断': '📊 核心调配指标', '日均销量': '📊 核心调配指标',
        
        '抖音占用': '🎵 抖音渠道', 'DY日均销量': '🎵 抖音渠道', 'DY预计周转天数': '🎵 抖音渠道', 'DY预计补货数': '🎵 抖音渠道',
        '天猫占用': '🐱 天猫渠道', 'TM日均销量': '🐱 天猫渠道', 'TM预计周转天数': '🐱 天猫渠道', 'TM预计补货数': '🐱 天猫渠道',
        '小红书占用': '📕 小红书渠道', 'RED日均销量': '📕 小红书渠道', 'RED预计周转天数': '📕 小红书渠道', 'RED预计补货数': '📕 小红书渠道'
    }

    # 💡 核心修改：动态识别并插入 明细数量 到 商品名称 后面
    base_cols = ['商品编码', '商品名称']
    if '明细数量' in df.columns:
        base_cols.append('明细数量')
        
    base_cols.extend(['可用数量', '在途数量', '换货在途数量', 'TTL', 
                 '全渠道总占用', '剩余可分配库存', '库存状态诊断', '日均销量'])
                 
    dy_cols = ['抖音占用'] + (['DY日均销量', 'DY预计周转天数', 'DY预计补货数'] if show_dy else [])
    tm_cols = ['天猫占用'] + (['TM日均销量', 'TM预计周转天数', 'TM预计补货数'] if show_tm else [])
    xhs_cols = ['小红书占用'] + (['RED日均销量', 'RED预计周转天数', 'RED预计补货数'] if show_xhs else [])

    final_cols = base_cols + dy_cols + tm_cols + xhs_cols
    
    df_out = df[final_cols].copy()
    
    multi_columns = [(header_mapping[col], col) for col in final_cols]
    df_out.columns = pd.MultiIndex.from_tuples(multi_columns)
    return df_out

# ================= UI 界面渲染 =================
st.title("📦 玖月美妆 - 全渠道库存透视中心")

# ================= 模块：控制台（动态参数） =================
with st.expander("⚙️ 展开控制台：配置发货仓库与商品告急阈值", expanded=True):
    col1, col2 = st.columns([1.5, 1])
    
    with col1:
        st.markdown("##### 📍 发货仓库多选")
        all_wh = get_all_warehouses()
        default_wh = [wh for wh in ['仓库-A', '仓库-I', '仓库-H', '仓库-F', '仓库-L'] if wh in all_wh]
        selected_wh = st.multiselect("请选择需要统计盘点的大仓（支持多选）：", options=all_wh, default=default_wh)
    
    with col2:
        st.markdown("##### 🚨 全局默认告急线")
        global_limit = st.number_input("当剩余库存低于此数值时报警：", value=50, step=10)

    st.markdown("---")
    st.markdown("##### 🎯 单品自定义特殊告急线")
    st.caption("如果某些品需要独立预警（如爆款设为200，清仓设为10），请在下方表格添加 (日均销量已升级为真实表抓取)：")
    
    if 'custom_configs' not in st.session_state:
        st.session_state.custom_configs = pd.DataFrame([{"商品编码": "", "特殊告急线": None}])
        
    edited_df = st.data_editor(
        st.session_state.custom_configs,
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        column_config={
            "商品编码": st.column_config.TextColumn("📝 输入商品编码 (如 H123)"),
            "特殊告急线": st.column_config.NumberColumn("🎯 特殊告急线", min_value=0, step=1)
        }
    )
    
    valid_configs = edited_df.dropna(subset=['商品编码'])
    valid_configs = valid_configs[valid_configs['商品编码'].str.strip() != ""]
    custom_limits_dict = dict(zip(valid_configs['商品编码'], valid_configs['特殊告急线']))

st.markdown("---")

st.markdown("#### 👁️ 渠道明细视图控制 (展开以查看渠道日销、周转天数与建议补货数)")
c1, c2, c3 = st.columns(3)
show_dy = c1.toggle("🎵 展开抖音明细指标", value=False)
show_tm = c2.toggle("🐱 展开天猫明细指标", value=False)
show_xhs = c3.toggle("📕 展开小红书明细指标", value=False)

st.markdown("#### ⚙️ 全局补货预测参数")
global_cycle = st.number_input("🎯 目标安全备货周期 (填写天数后敲击回车，系统将自动重算各渠道补货数):", value=7, min_value=1, step=1)
st.markdown("---")

# 开始加载数据（传入你勾选的仓库）
with st.spinner('⏳ 正在根据最新配置光速拆解全渠道库存与销量表现...'):
    df_final, combo_all = load_and_process_data(tuple(selected_wh))

# ================= 数据增强与预测计算 =================
if not df_final.empty:
    def apply_dynamic_status(row):
        rem = row['剩余可分配库存']
        occ = row['全渠道总占用']
        limit = custom_limits_dict.get(row['商品编码'])
        if pd.isna(limit): limit = global_limit
        
        if rem < 0: return '🚨 超卖!'
        elif rem <= limit and occ > 0: return '⚠️ 库存告急'
        else: return '✅ 正常'

    df_final['库存状态诊断'] = df_final.apply(apply_dynamic_status, axis=1)

    # 周转与补货数计算 (基于真实全渠道7天销量算出的日销)
    # -- 抖音 --
    df_final['DY预计周转天数'] = np.where(df_final['DY日均销量'] > 0, df_final['抖音占用'] / df_final['DY日均销量'], 999.9)
    df_final['DY预计周转天数'] = df_final['DY预计周转天数'].round(1)
    df_final['DY预计补货数'] = np.where(df_final['DY日均销量'] > 0, np.maximum(0, df_final['DY日均销量'] * global_cycle - df_final['抖音占用']), 0).astype(int)

    # -- 天猫 --
    df_final['TM预计周转天数'] = np.where(df_final['TM日均销量'] > 0, df_final['天猫占用'] / df_final['TM日均销量'], 999.9)
    df_final['TM预计周转天数'] = df_final['TM预计周转天数'].round(1)
    df_final['TM预计补货数'] = np.where(df_final['TM日均销量'] > 0, np.maximum(0, df_final['TM日均销量'] * global_cycle - df_final['天猫占用']), 0).astype(int)

    # -- 小红书 --
    df_final['RED预计周转天数'] = np.where(df_final['RED日均销量'] > 0, df_final['小红书占用'] / df_final['RED日均销量'], 999.9)
    df_final['RED预计周转天数'] = df_final['RED预计周转天数'].round(1)
    df_final['RED预计补货数'] = np.where(df_final['RED日均销量'] > 0, np.maximum(0, df_final['RED日均销量'] * global_cycle - df_final['小红书占用']), 0).astype(int)

# ================= 模块一：预警大屏 =================
st.header("🚨 预警大屏 (在售且超卖/库存告急的商品)")

if not df_final.empty:
    alert_df = df_final[df_final['库存状态诊断'].isin(['🚨 超卖!', '⚠️ 库存告急'])].copy().reset_index(drop=True)

    if not alert_df.empty:
        st.error(f"⚠️ 发现 {len(alert_df)} 个正在售卖的底层商品存在超卖或库存告急风险，请立即协调上下架或补货！")
        st.markdown("#### 🖱️ **【智能调度模式】**：点击下方表格中 **🚨 超卖商品的任意位置**，即可自动寻回它的同款替补库存！")

        formatted_alert_df = format_display_df(alert_df, show_dy, show_tm, show_xhs)
        
        selection_event = st.dataframe(
            formatted_alert_df,
            use_container_width=True,
            hide_index=True,
            selection_mode="single-row",
            on_select="rerun"
        )
        
        selected_rows = selection_event.selection.rows
        if selected_rows:
            row_idx = selected_rows[0]
            selected_row = alert_df.iloc[row_idx]
            
            if selected_row['库存状态诊断'] == '🚨 超卖!':
                target_clean_name = selected_row['匹配核名称']
                target_code = selected_row['商品编码']
                
                alt_df = df_final[(df_final['匹配核名称'] == target_clean_name) & (df_final['商品编码'] != target_code)]
                
                with st.container(border=True):
                    st.markdown(f"### 🎯 发现【{target_code}】的救援方案！")
                    if not alt_df.empty:
                        st.success(f"✨ 核心匹配词：**{target_clean_name}**。已为您跨区抓取到以下 **{len(alt_df)}** 个同款替补：")
                        formatted_alt_df = format_display_df(alt_df, show_dy, show_tm, show_xhs)
                        st.dataframe(formatted_alt_df, use_container_width=True, hide_index=True)
                    else:
                        st.warning(f"😔 系统搜遍了全库，没有找到包含核心词 **[{target_clean_name}]** 的同款替补商品。")
            else:
                st.info("💡 当前选中的商品为【库存告急】状态，尚未引发超卖，系统暂时无需强制推荐替补方案。")
    else:
        st.success("🎉 太棒了！当前所有在售底层SKU库存极其健康！")

st.markdown("---")

# ================= 模块二：双向联动查询中心 =================
st.header("🔍 双向联动查询中心")

tab1, tab2 = st.tabs(["🧩 按【虚拟套组】查询 (查组合)", "🧬 按【底层 Code】查询 (查单品)"])

with tab1:
    st.markdown("##### 🕵️‍♂️ 请选择一个虚拟套组编码 (演示环境仅开放前 10 个数据)：")
    combo_list = combo_all['套餐编码'].dropna().unique().tolist()[:10]
    selected_combo = st.selectbox("👉 选择套组：", [""] + combo_list, key="combo_select", help="为保护商业隐私，此处仅截取前10条脱敏数据供系统功能演示。")
    
    if selected_combo and not df_final.empty:
        combo_details = combo_all[combo_all['套餐编码'] == selected_combo].copy()
        merged_info = pd.merge(combo_details, df_final, on='商品编码', how='left')
        
        st.info(f"💡 该组合包共需要消耗 **{len(merged_info)}** 种底层物料，它们的明细及当前全局库存与预计周转状况如下：")
        formatted_combo_df = format_display_df(merged_info, show_dy, show_tm, show_xhs)
        st.dataframe(formatted_combo_df, use_container_width=True, hide_index=True)

with tab2:
    st.markdown("##### 🕵️‍♂️ 请选择一个底层商品编码 (演示环境仅开放前 10 个数据)：")
    if not df_final.empty:
        bottom_list = df_final['商品编码'].dropna().unique().tolist()[:10]
        selected_bottom = st.selectbox("👉 选择底层 Code：", [""] + bottom_list, key="bottom_select", help="为保护商业隐私，此处仅截取前10条脱敏数据供系统功能演示。")
        
        if selected_bottom:
            bottom_details = df_final[df_final['商品编码'] == selected_bottom]
            
            if not bottom_details.empty:
                st.write("---")
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("📦 总可用大盘 (TTL)", int(bottom_details['TTL'].iloc[0]))
                col2.metric("🛒 全渠道总占用", int(bottom_details['全渠道总占用'].iloc[0]))
                
                rem_inv = int(bottom_details['剩余可分配库存'].iloc[0])
                limit_val = custom_limits_dict.get(selected_bottom)
                sk_limit = limit_val if pd.notna(limit_val) else global_limit
                
                col3.metric("✨ 最终剩余可支配", rem_inv, delta=f"告急线: {sk_limit}", delta_color="inverse" if rem_inv <= sk_limit else "normal")
                
                st.write("📊 **全渠道库存详情、预测与分布：**")
                
                formatted_bottom_df = format_display_df(bottom_details, show_dy, show_tm, show_xhs)
                st.dataframe(formatted_bottom_df, use_container_width=True, hide_index=True)

# ================= 模块三：模糊名称检索补充工具 =================
st.markdown("---")
st.header("💡 补充查询工具")

with st.expander("➕ 点击展开：按【商品名称】进行模糊检索"):
    st.markdown("💡 系统会自动忽略品牌名（玖月）和末尾的修饰词汇。")
    search_kw = st.text_input("✍️ 请输入商品名称关键字（如：睫毛夹、粉底）：")
    
    if search_kw and not df_final.empty:
        kw_clean = search_kw.replace('玖月', '').replace(' ', '')
        kw_clean = re.sub(r'(样|非|会员|正装|赠品|小样)$', '', kw_clean)
        
        if kw_clean == "":
            matched_df = df_final[df_final['商品名称'].str.contains(search_kw, case=False, na=False)]
        else:
            mask = df_final['匹配核名称'].str.contains(kw_clean, case=False, na=False) | \
                   df_final['商品名称'].str.contains(search_kw, case=False, na=False)
            matched_df = df_final[mask]
        
        if not matched_df.empty:
            st.success(f"🎯 系统为您抓取到 **{len(matched_df)}** 个符合关键字“**{search_kw}**”的商品：")
            formatted_matched_df = format_display_df(matched_df, show_dy, show_tm, show_xhs)
            st.dataframe(formatted_matched_df, use_container_width=True, hide_index=True)
        else:
            st.warning(f"😔 系统搜遍了全库，未找到包含关键字“**{search_kw}**”的商品。")