import streamlit as st
import pandas as pd
import re  # 💡 引入正则表达式，用于更智能的文本清洗

# ================= 页面全局配置 =================
st.set_page_config(page_title="玖月美妆 - 全渠道库存透视中心", layout="wide")

# ================= 辅助函数：抓取所有可用仓库 =================
@st.cache_data
def get_all_warehouses():
    try:
        # 自动读取总表里的所有仓库名称供你筛选
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

    df_master = pd.read_csv(master_path)
    df_dy_inv = pd.read_csv(dy_inv_path)
    df_dy_combo = pd.read_csv(dy_combo_path)
    df_tm_inv = pd.read_csv(tm_inv_path)
    df_tm_combo = pd.read_csv(tm_combo_path)
    df_xhs_inv = pd.read_csv(xhs_inv_path)
    df_xhs_combo = pd.read_csv(xhs_combo_path)

    # ================= 1. 清洗总库存表 =================
    valid_status = ['可售']
    valid_channels = ['TM', 'DYXD', '天猫', 'RED', 'KWAI']
    
    # 💡【核心修改1】：仓库筛选条件变成了你在前端勾选的动态元组
    df_master_filtered = df_master[
        (df_master['库存状态'].isin(valid_status)) &
        (df_master['仓库名称'].isin(selected_warehouses)) &
        (df_master['渠道'].astype(str).str.upper().isin(valid_channels))
    ].copy()
    
    df_master_filtered['可用数量'] = pd.to_numeric(df_master_filtered['可用数量'], errors='coerce').fillna(0)
    df_master_filtered['在途数量'] = pd.to_numeric(df_master_filtered['在途数量'], errors='coerce').fillna(0)
    df_master_filtered['换货在途数量'] = pd.to_numeric(df_master_filtered['换货在途数量'], errors='coerce').fillna(0)
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

    # ================= 3. 渠道拆解引擎 =================
    def process_channel(df_inv, df_combo, inv_code_col, inv_qty_col):
        inv = df_inv[[inv_code_col, inv_qty_col]].copy()
        inv.rename(columns={inv_code_col: 'merchant_code', inv_qty_col: 'qty'}, inplace=True)
        inv['qty'] = pd.to_numeric(inv['qty'], errors='coerce').fillna(0)
        
        combo = df_combo[['套餐编码', '商品编码', '明细数量']].copy()
        combo['明细数量'] = pd.to_numeric(combo['明细数量'], errors='coerce').fillna(0)
        
        inv['merchant_code'] = inv['merchant_code'].astype(str).str.strip()
        
        inv['is_bottom'] = inv['merchant_code'].str.startswith('H', na=False)
        
        bottom_inv = inv[inv['is_bottom']].copy()
        bottom_inv['bottom_code'] = bottom_inv['merchant_code']
        bottom_inv['consumed_qty'] = bottom_inv['qty']
        
        virtual_inv = inv[~inv['is_bottom']].copy()
        merged = pd.merge(virtual_inv, combo, left_on='merchant_code', right_on='套餐编码', how='left')
        merged['consumed_qty'] = merged['qty'] * merged['明细数量']
        merged['bottom_code'] = merged['商品编码']
        
        unmatched = merged['bottom_code'].isna()
        merged.loc[unmatched, 'bottom_code'] = merged.loc[unmatched, 'merchant_code']
        merged.loc[unmatched, 'consumed_qty'] = merged.loc[unmatched, 'qty']
        
        final_inv = pd.concat([bottom_inv[['bottom_code', 'consumed_qty']], merged[['bottom_code', 'consumed_qty']]])
        return final_inv.groupby('bottom_code', as_index=False)['consumed_qty'].sum()

    dy_res = process_channel(df_dy_inv, df_dy_combo, '商家编码', '现货可售').rename(columns={'consumed_qty': '抖音占用'})
    tm_res = process_channel(df_tm_inv, df_tm_combo, '商品编码', '可售库存').rename(columns={'consumed_qty': '天猫占用'})
    xhs_res = process_channel(df_xhs_inv, df_xhs_combo, '商家编码', '库存').rename(columns={'consumed_qty': '小红书占用'})

    # ================= 4. 大盘融合比对 =================
    df_final = df_master_agg.copy()
    
    df_final['商品编码'] = df_final['商品编码'].astype(str).str.strip()
    dy_res['bottom_code'] = dy_res['bottom_code'].astype(str).str.strip()
    tm_res['bottom_code'] = tm_res['bottom_code'].astype(str).str.strip()
    xhs_res['bottom_code'] = xhs_res['bottom_code'].astype(str).str.strip()

    df_final = pd.merge(df_final, dy_res, left_on='商品编码', right_on='bottom_code', how='left').drop(columns=['bottom_code'], errors='ignore')
    df_final = pd.merge(df_final, tm_res, left_on='商品编码', right_on='bottom_code', how='left').drop(columns=['bottom_code'], errors='ignore')
    df_final = pd.merge(df_final, xhs_res, left_on='商品编码', right_on='bottom_code', how='left').drop(columns=['bottom_code'], errors='ignore')

    for col in ['抖音占用', '天猫占用', '小红书占用']:
        df_final[col] = df_final[col].fillna(0).astype(int)

    df_final['全渠道总占用'] = df_final['抖音占用'] + df_final['天猫占用'] + df_final['小红书占用']
    df_final['剩余可分配库存'] = df_final['TTL'] - df_final['全渠道总占用']
    
    # 💡【核心算法升级：智能识别剔除尾缀】
    def clean_product_name(name):
        name_clean = str(name).replace(' ', '')
        if '-' in name_clean:
            parts = name_clean.split('-')
            if len(parts) >= 3:
                name_clean = parts[1]
            elif len(parts) == 2:
                name_clean = parts[1]
        name_clean = name_clean.replace('玖月', '')
        # 【神级清洗】：自动剔除名字结尾的 样、非、会员、正装、赠品、小样
        name_clean = re.sub(r'(样|非|会员|正装|赠品|小样)$', '', name_clean)
        return name_clean

    df_final['匹配核名称'] = df_final['商品名称'].apply(clean_product_name)
    
    df_final = df_final[(df_final['TTL'] > 0) | (df_final['全渠道总占用'] > 0)]
    df_final = df_final.sort_values(by='剩余可分配库存', ascending=True)

    return df_final, combo_all


# ================= UI 界面渲染 =================
st.title("📦 玖月美妆 - 全渠道库存透视中心")

# ================= 新增模块：控制台（动态参数） =================
with st.expander("⚙️ 展开控制台：配置仓库与自定义告急线", expanded=True):
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
    st.caption("如果某些品需要独立预警（如爆款设为200，清仓设为10），请在下方表格添加：")
    
    # 构建一个可交互填写的表单
    if 'custom_limits' not in st.session_state:
        st.session_state.custom_limits = pd.DataFrame([{"商品编码": "", "特殊告急线": None}])
        
    edited_df = st.data_editor(
        st.session_state.custom_limits,
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        column_config={
            "商品编码": st.column_config.TextColumn("📝 输入商品编码 (如 H123)"),
            "特殊告急线": st.column_config.NumberColumn("🎯 特殊告急线 (输入数字)", min_value=0, step=1)
        }
    )
    # 将你填写的表格转化为字典字典备用，清空空行
    valid_customs = edited_df.dropna(subset=['商品编码', '特殊告急线'])
    custom_limits_dict = dict(zip(valid_customs['商品编码'], valid_customs['特殊告急线']))

st.markdown("---")

# 开始加载数据（传入你勾选的仓库）
with st.spinner('⏳ 正在根据最新配置光速拆解全渠道库存...'):
    # 必须要转成 tuple 才能被 Streamlit 完美缓存
    df_final, combo_all = load_and_process_data(tuple(selected_wh))

# 💡 动态赋予库存状态（根据你的自定义字典）
def apply_dynamic_status(row):
    rem = row['剩余可分配库存']
    occ = row['全渠道总占用']
    # 优先读取自定义线，没有自定义的就用上面的全局默认线
    limit = custom_limits_dict.get(row['商品编码'], global_limit)
    
    if rem < 0: 
        return '🚨 超卖!'
    elif rem <= limit and occ > 0: 
        return '⚠️ 库存告急'
    else: 
        return '✅ 正常'

df_final['库存状态诊断'] = df_final.apply(apply_dynamic_status, axis=1)

# ================= 模块一：预警大屏 (保留点击联动) =================
st.header("🚨 预警大屏 (在售且超卖/库存告急的商品)")

alert_df = df_final[df_final['库存状态诊断'].isin(['🚨 超卖!', '⚠️ 库存告急'])].copy().reset_index(drop=True)

if not alert_df.empty:
    st.error(f"⚠️ 发现 {len(alert_df)} 个正在售卖的底层商品存在超卖或库存告急风险，请立即协调上下架或补货！")
    st.markdown("#### 🖱️ **【智能调度模式】**：点击下方表格中 **🚨 超卖商品的任意位置**，即可自动寻回它的同款替补库存！")

    selection_event = st.dataframe(
        alert_df[['商品编码', '商品名称', '可用数量', '在途数量', '换货在途数量', 'TTL', '全渠道总占用', '剩余可分配库存', '库存状态诊断', '抖音占用', '天猫占用', '小红书占用']],
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
            target_name = selected_row['商品名称']
            
            alt_df = df_final[(df_final['匹配核名称'] == target_clean_name) & (df_final['商品编码'] != target_code)]
            
            with st.container(border=True):
                st.markdown(f"### 🎯 发现【{target_code}】的救援方案！")
                if not alt_df.empty:
                    st.success(f"✨ 核心匹配词：**{target_clean_name}**。已为您跨区抓取到以下 **{len(alt_df)}** 个同款替补：")
                    display_cols = ['商品编码', '商品名称', '可用数量', '在途数量', '换货在途数量', 'TTL', '全渠道总占用', '剩余可分配库存', '库存状态诊断']
                    st.dataframe(alt_df[display_cols], use_container_width=True, hide_index=True)
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
    st.markdown("##### 🕵️‍♂️ 输入或选择一个虚拟套组编码 (如 JYTM1223, JYMZ0009)")
    combo_list = combo_all['套餐编码'].dropna().unique().tolist()
    selected_combo = st.selectbox("请选择或输入套组编码:", [""] + combo_list, key="combo_select")
    
    if selected_combo:
        combo_details = combo_all[combo_all['套餐编码'] == selected_combo].copy()
        merged_info = pd.merge(combo_details, df_final, on='商品编码', how='left')
        
        st.info(f"💡 该组合包共需要消耗 **{len(merged_info)}** 种底层物料，它们的当前全局库存状况如下：")
        
        display_cols = ['商品编码', '商品名称', '明细数量', '可用数量', '在途数量', '换货在途数量', 'TTL', '全渠道总占用', '剩余可分配库存', '库存状态诊断', '抖音占用', '天猫占用', '小红书占用']
        st.dataframe(merged_info[display_cols], use_container_width=True, hide_index=True)

with tab2:
    st.markdown("##### 🕵️‍♂️ 输入或选择一个底层商品编码 (通常为 H 开头)")
    bottom_list = df_final['商品编码'].dropna().unique().tolist()
    selected_bottom = st.selectbox("请选择或输入底层 Code:", [""] + bottom_list, key="bottom_select")
    
    if selected_bottom:
        bottom_details = df_final[df_final['商品编码'] == selected_bottom]
        
        if not bottom_details.empty:
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("📦 总可用大盘 (TTL)", int(bottom_details['TTL'].iloc[0]))
            col2.metric("🛒 全渠道总占用", int(bottom_details['全渠道总占用'].iloc[0]))
            
            rem_inv = int(bottom_details['剩余可分配库存'].iloc[0])
            # 读取当前选择SKU的专用阈值，用于显示差值
            sk_limit = custom_limits_dict.get(selected_bottom, global_limit)
            col3.metric("✨ 最终剩余可支配", rem_inv, delta=f"告急线: {sk_limit}", delta_color="inverse" if rem_inv <= sk_limit else "normal")
            
            st.write("---")
            st.write("📊 **全渠道库存详情及分布：**")
            
            display_cols_bottom = ['商品编码', '商品名称', '可用数量', '在途数量', '换货在途数量', 'TTL', '全渠道总占用', '剩余可分配库存', '库存状态诊断', '抖音占用', '天猫占用', '小红书占用']
            st.dataframe(
                bottom_details[display_cols_bottom],
                use_container_width=True,
                hide_index=True
            )

# ================= 模块三：新增的模糊名称检索补充工具 =================
st.markdown("---")
st.header("💡 补充查询工具")

with st.expander("➕ 点击展开：按【商品名称】进行模糊检索"):
    st.markdown("💡 系统会自动忽略品牌名（玖月）和末尾的修饰词汇。")
    search_kw = st.text_input("✍️ 请输入商品名称关键字（如：睫毛夹、粉底）：")
    
    if search_kw:
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
            display_cols = ['商品编码', '商品名称', '可用数量', '在途数量', '换货在途数量', 'TTL', '全渠道总占用', '剩余可分配库存', '库存状态诊断', '抖音占用', '天猫占用', '小红书占用']
            st.dataframe(matched_df[display_cols], use_container_width=True, hide_index=True)
        else:
            st.warning(f"😔 系统搜遍了全库，未找到包含关键字“**{search_kw}**”的商品。")