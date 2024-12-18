import os
import config
import pandas as pd
import auto_adjust.filters as filter
from openpyxl import load_workbook
from openpyxl.styles import numbers
from concurrent.futures import ThreadPoolExecutor, as_completed

def read_excel_in_chunks(file_path, sheet_name, chunk_size):
    # 读取整个工作表到一个DataFrame中
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    
    # 按照指定的chunk_size分块返回DataFrame
    for start in range(0, len(df), chunk_size):
        yield df.iloc[start:start + chunk_size]

def read_excel_in_chunks_pair(file_path_old, file_path_new, sheet_name):
    df_old = pd.read_excel(file_path_old, sheet_name=sheet_name)
    df_new = pd.read_excel(file_path_new, sheet_name=sheet_name)

    # 1. 筛选 df_old 数据
    filtered_new = df_new[df_new['实体层级'] == '广告活动']

    # 2. 利用筛选结果匹配 df_old
    matched_old = df_old[df_old['广告活动名称'].isin(filtered_new['广告活动名称'])]

    filtered_new = filtered_new.set_index('广告活动名称').loc[matched_old['广告活动名称']].reset_index()
    matched_old = matched_old.set_index('广告活动名称').loc[filtered_new['广告活动名称']].reset_index()

    return matched_old, filtered_new


class SPModule:
    def __init__(self, file_path):
        self.file_path = file_path

    def sp_product_screen(self):
        # 构建新文件路径，使用原文件名加上后缀
        upload_dir = os.path.dirname(self.file_path)
        new_file_name = os.path.basename(self.file_path).rsplit('.', 1)[0] + '_' + 'SP商品筛选' + '.xlsx'
        output_file_path = os.path.join(upload_dir, new_file_name)
        """从包含'商品推广'的工作表名称中加载数据。"""
        condition_chunks = []
        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = []
            for chunk_df in read_excel_in_chunks(self.file_path, sheet_name='商品推广活动', chunk_size=50000):
                future = executor.submit(filter.sp_product, chunk_df, config.click, 
                                         config.order, config.acos, config.conversion, config.sku)
                futures.append(future)

            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    condition_chunks.append(result)

        if len(condition_chunks) != 0:
            write_content = pd.concat(condition_chunks, ignore_index=True)
            # 保存修改后的行到新文件
            self.save_modified_rows(write_content, output_file_path)
        else:
            print("没有符合条件的广告需要暂停。")

    def sp_advertise_screen(self):
        # 构建新文件路径，使用原文件名加上后缀
        upload_dir = os.path.dirname(self.file_path)
        new_file_name = os.path.basename(self.file_path).rsplit('.', 1)[0] + '_' + 'SP投放商品筛选' + '.xlsx'
        output_file_path = os.path.join(upload_dir, new_file_name)
        """从包含'商品推广'的工作表名称中加载数据。"""
        condition_chunks = []

        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = []
            for chunk_df in read_excel_in_chunks(self.file_path, sheet_name='商品推广活动', chunk_size=50000):
                future = executor.submit(filter.sp_ad, chunk_df, config.spend, config.order,
                                         config.acos, config.conversion, config.sku)
                futures.append(future)

            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    condition_chunks.append(result)

        if len(condition_chunks) != 0:
            write_content = pd.concat(condition_chunks, ignore_index=True)
            # 保存修改后的行到新文件
            self.save_modified_rows(write_content, output_file_path)
        else:
            print("没有符合条件的投放商品。")

    def sp_pos_screen(self):
        # 构建新文件路径，使用原文件名加上后缀
        upload_dir = os.path.dirname(self.file_path)
        new_file_name = os.path.basename(self.file_path).rsplit('.', 1)[0] + '_' + 'SP竞价调整' + '.xlsx'
        output_file_path = os.path.join(upload_dir, new_file_name)
        """从包含'商品推广'的工作表名称中加载数据。"""
        condition_chunks = []

        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = []
            for chunk_df in read_excel_in_chunks(self.file_path, sheet_name='商品推广活动', chunk_size=50000):
                future = executor.submit(filter.sp_pos, chunk_df, config.spend, config.order,
                                         config.acos, config.conversion, config.sku)
                futures.append(future)

            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    condition_chunks.append(result)
    
    
        if len(condition_chunks) != 0:
            write_content = pd.concat(condition_chunks, ignore_index=True)
            # 保存修改后的行到新文件
            self.save_modified_rows(write_content, output_file_path)
        else:
            print("没有符合条件的广告需要暂停。")

    def sp_word_screen(self):
        # 构建新文件路径，使用原文件名加上后缀
        upload_dir = os.path.dirname(self.file_path)
        new_file_name = os.path.basename(self.file_path).rsplit('.', 1)[0] + '_' + 'SP搜索词筛选' + '.xlsx'
        output_file_path = os.path.join(upload_dir, new_file_name)
        """从包含'商品推广'的工作表名称中加载数据。"""
        condition_chunks = []

        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = []
            for chunk_df in read_excel_in_chunks(self.file_path, sheet_name='商品推广搜索词报告', chunk_size=50000):
                future = executor.submit(filter.sp_word, chunk_df, config.click, config.click_rate,
                                         config.order, config.conversion, config.sku)
                futures.append(future)

            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    condition_chunks.append(result)

        if len(condition_chunks) != 0:
            write_content = pd.concat(condition_chunks, ignore_index=True)
            # 保存修改后的行到新文件
            self.save_modified_rows(write_content, output_file_path)
        else:
            print("没有符合条件的搜索词。")
    def sp_keyword_screen(self):
        # 构建新文件路径，使用原文件名加上后缀
        upload_dir = os.path.dirname(self.file_path)
        new_file_name = os.path.basename(self.file_path).rsplit('.', 1)[0] + '_' + 'SP投放关键词筛选' + '.xlsx'
        output_file_path = os.path.join(upload_dir, new_file_name)
        """从包含'商品推广'的工作表名称中加载数据。"""
        condition_chunks = []

        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = []
            for chunk_df in read_excel_in_chunks(self.file_path, sheet_name='商品推广活动', chunk_size=50000):
                future = executor.submit(filter.sp_keyword, chunk_df, config.click, config.click_rate,
                                         config.order, config.conversion, config.sku)
                futures.append(future)

            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    condition_chunks.append(result)

        if len(condition_chunks) != 0:
            write_content = pd.concat(condition_chunks, ignore_index=True)
            # 保存修改后的行到新文件
            self.save_modified_rows(write_content, output_file_path)
        else:
            print("没有符合条件的投放关键词。")

    def sp_invalid_screen(self):
        # 构建新文件路径，使用原文件名加上后缀
        upload_dir = os.path.dirname(self.file_path)
        new_file_name = os.path.basename(self.file_path).rsplit('.', 1)[0] + '_' + 'SP无效筛选' + '.xlsx'
        output_file_path = os.path.join(upload_dir, new_file_name)
        """从包含'商品推广'的工作表名称中加载数据。"""
        condition_chunks = []

        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = []
            for chunk_df in read_excel_in_chunks(self.file_path, sheet_name='商品推广活动', chunk_size=50000):
                future = executor.submit(filter.sp_invalid, chunk_df, config.click, config.sku)
                futures.append(future)

            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    condition_chunks.append(result)

        if len(condition_chunks) != 0:
            write_content = pd.concat(condition_chunks, ignore_index=True)
            # 保存修改后的行到新文件
            self.save_modified_rows(write_content, output_file_path)
        else:
            print("没有符合条件的无效广告。")
    

    def sp_descent_screen(self, file_path_old, file_path_new):
        upload_dir = os.path.dirname(file_path_new)
        new_file_name = os.path.basename(file_path_new).rsplit('.', 1)[0] + '_' + 'SP花费下降' + '.xlsx'
        output_file_path = os.path.join(upload_dir, new_file_name)


        old_chunk, new_chunk = read_excel_in_chunks_pair(file_path_old, file_path_new, '商品推广活动')
        write_content = filter.sp_descent(old_chunk, new_chunk, config.spend, config.sku)

        if not write_content.empty:
            self.save_modified_rows(write_content, output_file_path)
        else:
            print("没有符合条件的广告活动。")

    def save_modified_rows(self, modified_rows, output_file_path):
        """
        将修改后的行保存到指定的新文件中。
        :param modified_rows: 包含修改后的数据行的DataFrame
        :param output_file_path: 要保存的新文件路径
        """
        try:
            modified_rows.to_excel(output_file_path, index=False, engine="openpyxl")
            print(f"包含修改行的文件已保存为: {output_file_path}")
        except Exception as e:
            print(f"保存更新文件出错: {e}")

    def call_function(self, function_name, *args):
        """
        通过名称动态调用函数，并支持传递额外参数。

        :param function_name: 要调用的函数名称
        :param args: 可变参数，用于传递给被调用函数的额外参数
        """
        func = getattr(self, function_name, None)
        if callable(func):
            if function_name == 'sp_descent_screen':
                # sp_descent_screen 需要两个文件路径作为参数
                if len(args) == 2:
                    func(args[0], args[1])
                else:
                    print(f"错误：sp_descent_screen 函数需要两个文件路径作为参数。")
            else:
                # 其他函数不需要额外参数
                func()
        else:
            print(f"未找到函数 '{function_name}'。")
