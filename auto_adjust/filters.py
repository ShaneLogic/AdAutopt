import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from queue import Queue


def filter_data_helper(data, sku_filter, conditions, entity_level=None, is_sp_pos=False, is_sp_word=False, is_sp_invalid=False):
    """
    根据给定的条件、SKU筛选器和实体层级筛选数据。

    :param data: 包含广告相关数据的pandas.DataFrame对象。
    :param sku_filter: 可选的SKU列表，用于筛选特定的广告组合。
    :param conditions: 用于筛选数据的条件组合。
    :param entity_level: 实体层级，如"商品广告"或"商品定向"。
    :param is_sp_pos: 布尔值，表示是否为sp_pos函数调用。
    :param is_sp_word: 布尔值，表示是否为sp_word函数调用。
    :return: 筛选出满足条件的数据的DataFrame。
    """
    try:
        if is_sp_pos or is_sp_word:
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
        else:
            return data[base_conditions]
    except KeyError as e:
        raise KeyError(f"列名错误: {e}")


def worker(data, sku_sub_list, conditions, entity_level, result_queue, is_sp_pos, is_sp_word, is_sp_invalid):
    """
    工作线程函数，处理数据子集并将结果放入队列。

    :param data: 包含广告相关数据的pandas.DataFrame对象。
    :param sku_sub_list: SKU子列表。
    :param conditions: 用于筛选数据的条件组合。
    :param entity_level: 实体层级。
    :param result_queue: 结果队列。
    :param is_sp_pos: 布尔值，表示是否为sp_pos函数调用。
    :param is_sp_word: 布尔值，表示是否为sp_word函数调用。
    """
    result = filter_data_helper(data, sku_sub_list, conditions, entity_level, is_sp_pos, is_sp_word, is_sp_invalid)
    result_queue.put(result)


def apply_filters(data, conditions, sku_str, entity_level=None, is_sp_pos=False, is_sp_word=False, is_sp_invalid=False):
    """
    应用筛选条件并处理数据。

    :param data: 包含广告相关数据的pandas.DataFrame对象。
    :param conditions: 用于筛选数据的条件组合。
    :param entity_level: 实体层级，如"商品广告"或"商品定向"。
    :param sku_str: 可选，字符串，前端表单输入的以逗号分隔的多个SKU值。
    :param is_sp_pos: 布尔值，表示是否为sp_pos函数调用。
    :param is_sp_word: 布尔值，表示是否为sp_word函数调用。
    :return: 筛选出满足条件的广告数据的DataFrame。
    """
    sku_list = [sku.strip() for sku in sku_str.split(",")] if sku_str else None

    if sku_list:
        num_threads = min(len(sku_list), 8)  # 使用最多8个线程
        chunked_sku_lists = [sku_list[i::num_threads] for i in range(num_threads)]

        result_queue = Queue()
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            for sku_sub_list in chunked_sku_lists:
                executor.submit(worker, data, sku_sub_list, conditions, entity_level, result_queue, is_sp_pos, is_sp_word, is_sp_invalid)

        results = []
        while not result_queue.empty():
            results.append(result_queue.get())

        to_pause = pd.concat(results)
    else:
        to_pause = filter_data_helper(data, None, conditions, entity_level, is_sp_pos, is_sp_word, is_sp_invalid)

    return to_pause


def sp_product(data, click, order, acos, conversion, sku_str):
    """
    根据指定的条件筛选出需要暂停的产品广告数据。

    :param data: 包含广告相关数据的pandas.DataFrame对象，需包含如点击量、订单数量、ACOS等列。
    :param click: 整数，表示点击量的阈值，用于条件判断。
    :param order: 整数，表示订单数量的阈值，用于条件判断。
    :param acos: 数值，表示ACOS的阈值，用于条件判断。
    :param conversion: 数值，表示转化率的阈值，用于条件判断。
    :param sku_str: 字符串，前端表单输入的以逗号分隔的多个SKU值，用于筛选特定的广告组合。
    :return: 筛选出满足条件的广告数据的DataFrame。
    """
    condition1 = (data["点击量"] > click) & (data["订单数量"] == 0)
    condition2 = (data["订单数量"] < order) & (data["ACOS"] > acos) & (data["转化率"] < conversion)
    conditions = condition1 | condition2
    entity_level = "商品广告"

    to_pause = apply_filters(data, conditions, sku_str, entity_level)

    if not to_pause.empty:
        for index, row in to_pause.iterrows():
            if condition1.loc[index]:
                data.loc[index, "操作"] = "Update"
                data.loc[index, "状态"] = "已暂停"
            elif condition2.loc[index]:
                data.loc[index, "操作"] = "Update"
                data.loc[index, "状态"] = "已暂停"
        return data.loc[to_pause.index]
    return None


def sp_ad(data, spend, order, acos, conversion, sku_str):
    """
    根据指定的条件筛选出需要暂停的广告数据。

    :param data: 包含广告相关数据的pandas.DataFrame对象，需包含如花费、订单数量、ACOS等列。
    :param spend: 数值，表示花费的阈值，用于条件判断。
    :param order: 整数，表示订单数量的阈值，用于条件判断。
    :param acos: 数值，表示ACOS的阈值，用于条件判断。
    :param conversion: 数值，表示转化率的阈值，用于条件判断。
    :param sku_str: 字符串，前端表单输入的以逗号分隔的多个SKU值，用于筛选特定的广告组合。
    :return: 筛选出满足条件的广告数据的DataFrame。
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


def sp_pos(data, spend, order, acos, conversion, sku_str):
    """
    根据指定的条件筛选出需要暂停的广告数据。

    :param data: 包含广告相关数据的pandas.DataFrame对象，需包含如花费、订单数量、ACOS等列。
    :param spend: 数值，表示花费的阈值，用于条件判断。
    :param order: 整数，表示订单数量的阈值，用于条件判断。
    :param acos: 数值，表示ACOS的阈值，用于条件判断。
    :param conversion: 数值，表示转化率的阈值，用于条件判断。
    :param sku_str: 字符串，前端表单输入的以逗号分隔的多个SKU值，用于筛选特定的广告组合。
    :return: 筛选出满足条件的广告数据的DataFrame。
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

def sp_word(data, click, click_rate, order, conversion, sku_str):
    """
    根据指定的条件筛选出需要暂停的广告数据。

    :param data: 包含广告相关数据的pandas.DataFrame对象，需包含如点击量、订单数量、点击率等列。
    :param click: 整数，表示点击量的阈值，用于条件判断。
    :param click_rate: 数值，表示点击率的阈值，用于条件判断。
    :param order: 整数，表示订单数量的阈值，用于条件判断。
    :param conversion: 数值，表示转化率的阈值，用于条件判断。
    :param sku_str: 字符串，前端表单输入的以逗号分隔的多个SKU值，用于筛选特定的广告组合。
    :return: 筛选出满足条件的广告数据的DataFrame。
    """
    condition1 = (data["点击量"] > click) & (data["点击率"] > click_rate) & (data["订单数量"] > order) & (data["转化率"] > conversion) & (data["ACOS"] < 0.25)
    conditions = condition1

    to_pause = apply_filters(data, conditions, sku_str, is_sp_word=True)

    if not to_pause.empty:
        return data.loc[to_pause.index]
    return None

def sp_keyword(data, click, click_rate, order, conversion, sku_str):
    """
    根据指定的条件筛选出需要暂停的广告数据。

    :param data: 包含广告相关数据的pandas.DataFrame对象，需包含如点击量、订单数量、点击率等列。
    :param click: 整数，表示点击量的阈值，用于条件判断。
    :param click_rate: 数值，表示点击率的阈值，用于条件判断。
    :param order: 整数，表示订单数量的阈值，用于条件判断。
    :param conversion: 数值，表示转化率的阈值，用于条件判断。
    :param sku_str: 字符串，前端表单输入的以逗号分隔的多个SKU值，用于筛选特定的广告组合。
    :return: 筛选出满足条件的广告数据的DataFrame。
    """
    condition1 = (data["点击量"] > click) & (data["点击率"] > click_rate) & (data["订单数量"] > order) & (data["转化率"] > conversion) & (data["ACOS"] < 0.30)
    conditions = condition1
    entity_level = "关键词"

    to_pause = apply_filters(data, conditions, sku_str, entity_level)

    if not to_pause.empty:
        return data.loc[to_pause.index]
    return None

def sp_invalid(data, click, sku_str):
    """
    筛选无效广告并调整相关广告的竞价。

    功能说明：
    1. 从“实体层级”列筛选出“广告活动”行；
    2. 检查“开始日期”（O列）是否在7天以内或者超过7天；
    3. 根据点击量判断是否为无效广告：
       - 如果在7天以内，AM列的点击量小于5；
       - 如果超过7天，AM列的点击量在10次以下；
    4. 对无效广告关联的“关键词”和“商品定向”行，调整竞价值。

    :param data: 包含广告相关数据的pandas.DataFrame对象。
    :return: 返回调整后的DataFrame。
    """
    # 创建数据副本
    data = data.copy()

    # 当前日期
    current_date = datetime.now()

    # 计算天数差
    data.loc[:, "天数差"] = (current_date - pd.to_datetime(data["开始日期"], format="%Y%m%d", errors='coerce')).dt.days

    condition1 = (data["点击量"] < 2) & (data["天数差"] >= 5) & (data["天数差"] <= 7)
    condition2 = (data["点击量"] < click) & (data["天数差"] > 7)
    conditions = condition1 | condition2
    entity_level = "广告活动"

    to_pause = apply_filters(data, conditions, sku_str, entity_level, is_sp_invalid=True)


    if not to_pause.empty:
            return data.loc[to_pause.index]
    return None

def sp_descent(old_data, new_data, spend, sku_str):
    """
    比较两个数据组，筛选无效广告并调整相关广告的竞价。

    功能说明：
    1. 从"实体层级"列筛选出"广告活动"行；
    2. 比较新旧数据的点击量变化；
    3. 根据点击量判断是否为无效广告：
       - 如果在7天以内，新数据中点击量小于2；
       - 如果超过7天，新数据中点击量小于指定阈值；
    4. 对无效广告关联的"关键词"和"商品定向"行，调整竞价值。

    :param data_old: 包含较早广告相关数据的pandas.DataFrame对象。
    :param data_new: 包含较新广告相关数据的pandas.DataFrame对象。
    :param click: 整数，表示点击量的阈值，用于条件判断。
    :param sku_str: 字符串，前端表单输入的以逗号分隔的多个SKU值，用于筛选特定的广告组合。
    :return: 返回筛选出的需要调整的DataFrame。
    """

    # 创建数据副本
    old_data = old_data.copy()
    new_data = new_data.copy()

    # 计算点击量的变化
    new_data.loc[:, "花费变化"] = (old_data['花费'] - new_data['花费']) / 23 - new_data['花费'] / 7

    condition1 = (new_data["花费变化"] > spend)
    conditions = condition1
    entity_level = "广告活动"

    to_pause = apply_filters(new_data, conditions, sku_str, entity_level, is_sp_invalid=True)
    
    if not to_pause.empty:
        return new_data.loc[to_pause.index]
    return None