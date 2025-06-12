# -*- coding: utf-8 -*-
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from queue import Queue


def filter_data_helper(data, sku_filter, conditions, entity_level=None, is_sp_pos=False, is_sp_word=False, is_sp_invalid=False):
    """Filter data based on given conditions, SKU filter, and entity level
    
    Args:
        data (pd.DataFrame): DataFrame containing advertising data
        sku_filter (list): Optional list of SKUs to filter specific ad combinations
        conditions (pd.Series): Boolean conditions for filtering data
        entity_level (str, optional): Entity level like "商品广告" or "商品定向"
        is_sp_pos (bool): Whether this is a sp_pos function call
        is_sp_word (bool): Whether this is a sp_word function call
        is_sp_invalid (bool): Whether this is a sp_invalid function call
        
    Returns:
        pd.DataFrame: Filtered data meeting all conditions
    """
    try:
        if is_sp_pos:
            base_conditions = (
                (data["实体层级"] == entity_level) &
                conditions
            )
        elif is_sp_word:
            base_conditions = conditions
        elif is_sp_invalid:
            base_conditions = (
                (data["广告活动状态（仅供参考）"] == "已启用") &
                (data["实体层级"] == entity_level) &
                conditions
            )
        else:
            base_conditions = (
                (data["广告活动状态（仅供参考）"] == "已启用") &
                (data["广告组状态（仅供参考）"] == "已启用") &
                (data["状态"] == "已启用") &
                (data["实体层级"] == entity_level) &
                conditions
            )

        if sku_filter:
            return data[base_conditions & data['广告组合名称（仅供参考）'].isin(sku_filter)]
        return data[base_conditions]
    except KeyError as e:
        raise KeyError("Column name error: {0}".format(e))


def worker(data, sku_sub_list, conditions, entity_level, result_queue, is_sp_pos, is_sp_word, is_sp_invalid):
    """Worker thread function to process data subset and put results in queue
    
    Args:
        data (pd.DataFrame): DataFrame containing advertising data
        sku_sub_list (list): Subset of SKUs to process
        conditions (pd.Series): Boolean conditions for filtering
        entity_level (str): Entity level for filtering
        result_queue (Queue): Queue to store results
        is_sp_pos (bool): Whether this is a sp_pos function call
        is_sp_word (bool): Whether this is a sp_word function call
        is_sp_invalid (bool): Whether this is a sp_invalid function call
    """
    result = filter_data_helper(data, sku_sub_list, conditions, entity_level, is_sp_pos, is_sp_word, is_sp_invalid)
    result_queue.put(result)


def apply_filters(data, conditions, sku_str, entity_level=None, is_sp_pos=False, is_sp_word=False, is_sp_invalid=False):
    """Apply filters and process data
    
    Args:
        data (pd.DataFrame): DataFrame containing advertising data
        conditions (pd.Series): Boolean conditions for filtering
        sku_str (str): Comma-separated SKU values from frontend form
        entity_level (str, optional): Entity level for filtering
        is_sp_pos (bool): Whether this is a sp_pos function call
        is_sp_word (bool): Whether this is a sp_word function call
        is_sp_invalid (bool): Whether this is a sp_invalid function call
        
    Returns:
        pd.DataFrame: Filtered data meeting all conditions
    """
    sku_list = [sku.strip() for sku in sku_str.split(",")] if sku_str else None

    if sku_list:
        num_threads = min(len(sku_list), 8)  # Use up to 8 threads
        chunked_sku_lists = [sku_list[i::num_threads] for i in range(num_threads)]

        result_queue = Queue()
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            for sku_sub_list in chunked_sku_lists:
                executor.submit(worker, data, sku_sub_list, conditions, entity_level, result_queue, is_sp_pos, is_sp_word, is_sp_invalid)

        results = []
        while not result_queue.empty():
            results.append(result_queue.get())

        return pd.concat(results) if results else pd.DataFrame()
    
    return filter_data_helper(data, None, conditions, entity_level, is_sp_pos, is_sp_word, is_sp_invalid)


def sp_product(data, click, order, acos, conversion, sku_str):
    """Filter product ads that need to be paused based on specified conditions
    
    Args:
        data (pd.DataFrame): DataFrame containing advertising data
        click (int): Click threshold
        order (int): Order threshold
        acos (float): ACOS threshold
        conversion (float): Conversion rate threshold
        sku_str (str): Comma-separated SKU values
        
    Returns:
        pd.DataFrame: Filtered data with updated status
    """
    condition1 = (data["点击量"] > click) & (data["订单数量"] == 0)
    condition2 = (data["订单数量"] < order) & (data["ACOS"] > acos) & (data["转化率"] < conversion)
    conditions = condition1 | condition2
    entity_level = "商品广告"

    to_pause = apply_filters(data, conditions, sku_str, entity_level)

    if not to_pause.empty:
        data.loc[to_pause.index, "操作"] = "Update"
        data.loc[to_pause.index, "状态"] = "已暂停"
        return data.loc[to_pause.index]
    return None


def sp_ad(data, spend, order, acos, conversion, sku_str):
    """Filter and adjust ads based on spending and performance metrics
    
    Args:
        data (pd.DataFrame): DataFrame containing advertising data
        spend (float): Spending threshold
        order (int): Order threshold
        acos (float): ACOS threshold
        conversion (float): Conversion rate threshold
        sku_str (str): Comma-separated SKU values
        
    Returns:
        pd.DataFrame: Filtered data with updated bids
    """
    condition1 = (data["花费"] > spend) & (data["订单数量"] == 0)
    condition2 = (data["转化率"] < conversion) & (data["花费"] > spend) & (data["ACOS"] > acos)
    condition3 = (data["转化率"] > conversion) & (data["ACOS"] < 0.30)
    conditions = condition1 | condition2 | condition3
    entity_level = "商品定向"

    to_pause = apply_filters(data, conditions, sku_str, entity_level)

    if not to_pause.empty:
        for index, row in to_pause.iterrows():
            if condition1.loc[index]:
                data.loc[index, "操作"] = "Update"
                data.loc[index, "状态"] = "已暂停"
            elif condition2.loc[index]:
                data.loc[index, "操作"] = "Update"
                data.loc[index, '竞价'] = row['竞价'] - 0.02
            elif condition3.loc[index]:
                data.loc[index, "操作"] = "Update"
                data.loc[index, '竞价'] = row['竞价'] + 0.03

        return data.loc[to_pause.index]
    return None


def sp_pos(data, spend, order, acos, conversion, sku_str):
    """Adjust bid positions based on spending and performance metrics
    
    Args:
        data (pd.DataFrame): DataFrame containing advertising data
        spend (float): Spending threshold
        order (int): Order threshold
        acos (float): ACOS threshold
        conversion (float): Conversion rate threshold
        sku_str (str): Comma-separated SKU values
        
    Returns:
        pd.DataFrame: Filtered data with updated bid percentages
    """
    condition1 = (data["花费"] > spend) & (data["转化率"] < conversion) & (data["ACOS"] > acos)
    condition2 = (data["花费"] > spend) & (data["转化率"] > conversion) & (data["ACOS"] < 0.25)
    conditions = condition1 | condition2
    entity_level = "竞价调整"

    to_pause = apply_filters(data, conditions, sku_str, entity_level, is_sp_pos=True)

    if not to_pause.empty:
        for index, row in to_pause.iterrows():
            if condition1.loc[index]:
                data.loc[index, "操作"] = "Update"
                data.loc[index, "百分比"] = max(row['百分比'] - 5.00, 0)
            elif condition2.loc[index]:
                data.loc[index, "操作"] = "Update"
                data.loc[index, '百分比'] = row['百分比'] + 10.00

        return data.loc[to_pause.index]
    return None


def sp_word(data, click, click_rate, order, conversion, sku_str):
    """Filter search terms based on performance metrics
    
    Args:
        data (pd.DataFrame): DataFrame containing advertising data
        click (int): Click threshold
        click_rate (float): Click rate threshold
        order (int): Order threshold
        conversion (float): Conversion rate threshold
        sku_str (str): Comma-separated SKU values
        
    Returns:
        pd.DataFrame: Filtered search terms
    """
    conditions = (
        (data["点击量"] > click) & 
        (data["点击率"] > click_rate) & 
        (data["订单数量"] > order) & 
        (data["转化率"] > conversion) & 
        (data["ACOS"] < 0.25)
    )

    to_pause = apply_filters(data, conditions, sku_str, is_sp_word=True)
    return data.loc[to_pause.index] if not to_pause.empty else None


def sp_keyword(data, click, click_rate, order, conversion, sku_str):
    """Filter keywords based on performance metrics
    
    Args:
        data (pd.DataFrame): DataFrame containing advertising data
        click (int): Click threshold
        click_rate (float): Click rate threshold
        order (int): Order threshold
        conversion (float): Conversion rate threshold
        sku_str (str): Comma-separated SKU values
        
    Returns:
        pd.DataFrame: Filtered keywords
    """
    conditions = (
        (data["点击量"] > click) & 
        (data["点击率"] > click_rate) & 
        (data["订单数量"] > order) & 
        (data["转化率"] > conversion) & 
        (data["ACOS"] < 0.30)
    )
    entity_level = "关键词"

    to_pause = apply_filters(data, conditions, sku_str, entity_level)
    return data.loc[to_pause.index] if not to_pause.empty else None


def sp_invalid(data, click, sku_str):
    """Filter invalid ads and adjust bids for related ads
    
    Args:
        data (pd.DataFrame): DataFrame containing advertising data
        click (int): Click threshold
        sku_str (str): Comma-separated SKU values
        
    Returns:
        pd.DataFrame: Filtered data with updated bids
    """
    # Filter for campaign level
    campaign_data = data[data["实体层级"] == "广告活动"].copy()
    
    # Calculate days since start
    campaign_data["开始日期"] = pd.to_datetime(campaign_data["开始日期"])
    days_since_start = (datetime.now() - campaign_data["开始日期"]).dt.days
    
    # Define conditions based on campaign age
    condition1 = (days_since_start <= 7) & (campaign_data["点击量"] < 5)
    condition2 = (days_since_start > 7) & (campaign_data["点击量"] < 10)
    conditions = condition1 | condition2
    
    # Get invalid campaigns
    invalid_campaigns = apply_filters(campaign_data, conditions, sku_str, is_sp_invalid=True)
    
    if not invalid_campaigns.empty:
        # Get campaign names
        campaign_names = invalid_campaigns["广告活动名称"].unique()
        
        # Filter related keywords and targets
        related_data = data[
            (data["广告活动名称"].isin(campaign_names)) &
            (data["实体层级"].isin(["关键词", "商品定向"]))
        ].copy()
        
        # Adjust bids
        related_data.loc[related_data["实体层级"] == "关键词", "竞价"] *= 0.8
        related_data.loc[related_data["实体层级"] == "商品定向", "竞价"] *= 0.8
        
        return related_data
    return None


def sp_descent(old_data, new_data, spend, sku_str):
    """Analyze spending trends between old and new data
    
    Args:
        old_data (pd.DataFrame): Previous period data
        new_data (pd.DataFrame): Current period data
        spend (float): Spending threshold
        sku_str (str): Comma-separated SKU values
        
    Returns:
        pd.DataFrame: Data showing spending trends
    """
    # Merge old and new data on campaign names
    merged_data = pd.merge(
        old_data[["广告活动名称", "花费"]],
        new_data[["广告活动名称", "花费"]],
        on="广告活动名称",
        suffixes=("_old", "_new")
    )
    
    # Calculate spending change
    merged_data["花费变化"] = merged_data["花费_new"] - merged_data["花费_old"]
    
    # Filter based on conditions
    conditions = (
        (merged_data["花费_old"] > spend) &
        (merged_data["花费变化"] < 0)
    )
    
    if sku_str:
        sku_list = [sku.strip() for sku in sku_str.split(",")]
        conditions &= merged_data["广告活动名称"].isin(sku_list)
    
    return merged_data[conditions]