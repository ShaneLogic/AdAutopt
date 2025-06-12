# -*- coding: utf-8 -*-
import os
import config
import pandas as pd
import auto_adjust.filters as filter
from openpyxl import load_workbook
from openpyxl.styles import numbers
from concurrent.futures import ThreadPoolExecutor, as_completed

def read_excel_in_chunks(file_path, sheet_name, chunk_size):
    """Read Excel file in chunks for better memory management
    
    Args:
        file_path (str): Path to the Excel file
        sheet_name (str): Name of the sheet to read
        chunk_size (int): Number of rows to read at once
        
    Yields:
        pd.DataFrame: Chunks of the Excel file
    """
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    for start in range(0, len(df), chunk_size):
        yield df.iloc[start:start + chunk_size]

def read_excel_in_chunks_pair(file_path_old, file_path_new, sheet_name):
    """Read and match data from old and new Excel files
    
    Args:
        file_path_old (str): Path to the old Excel file
        file_path_new (str): Path to the new Excel file
        sheet_name (str): Name of the sheet to read
        
    Returns:
        tuple: (matched_old_df, filtered_new_df) DataFrames containing matched data
    """
    df_old = pd.read_excel(file_path_old, sheet_name=sheet_name)
    df_new = pd.read_excel(file_path_new, sheet_name=sheet_name)

    # Filter new data for campaign level
    filtered_new = df_new[df_new['实体层级'] == '广告活动']

    # Match old data with filtered new data
    matched_old = df_old[df_old['广告活动名称'].isin(filtered_new['广告活动名称'])]

    # Align data using campaign names as index
    filtered_new = filtered_new.set_index('广告活动名称').loc[matched_old['广告活动名称']].reset_index()
    matched_old = matched_old.set_index('广告活动名称').loc[filtered_new['广告活动名称']].reset_index()

    return matched_old, filtered_new

class SPModule:
    """Main class for handling SP (Sponsored Products) related operations"""
    
    def __init__(self, file_path):
        """Initialize SP module with file path
        
        Args:
            file_path (str): Path to the input file
        """
        self.file_path = file_path

    def _process_chunks(self, sheet_name, filter_func, chunk_size=50000, **filter_args):
        """Process Excel data in chunks using specified filter function
        
        Args:
            sheet_name (str): Name of the sheet to process
            filter_func (callable): Filter function to apply
            chunk_size (int): Size of chunks to process
            **filter_args: Additional arguments for filter function
            
        Returns:
            pd.DataFrame: Concatenated results from all chunks
        """
        condition_chunks = []
        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = []
            for chunk_df in read_excel_in_chunks(self.file_path, sheet_name, chunk_size):
                future = executor.submit(filter_func, chunk_df, **filter_args)
                futures.append(future)

            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    condition_chunks.append(result)

        return pd.concat(condition_chunks, ignore_index=True) if condition_chunks else pd.DataFrame()

    def _save_results(self, data, suffix):
        """Save processed data to new Excel file
        
        Args:
            data (pd.DataFrame): Data to save
            suffix (str): Suffix to add to filename
            
        Returns:
            str: Path to saved file
        """
        upload_dir = os.path.dirname(self.file_path)
        base_name = os.path.basename(self.file_path).rsplit('.', 1)[0]
        new_file_name = "{0}_{1}.xlsx".format(base_name, suffix)
        output_file_path = os.path.join(upload_dir, new_file_name)
        
        if not data.empty:
            self.save_modified_rows(data, output_file_path)
            return output_file_path
        else:
            print("No matching data found for {0}".format(suffix))
            return None

    def sp_product_screen(self):
        """Screen products based on specified criteria"""
        data = self._process_chunks(
            '商品推广活动',
            filter.sp_product,
            click=config.click,
            order=config.order,
            acos=config.acos,
            conversion=config.conversion,
            sku=config.sku
        )
        return self._save_results(data, 'SP商品筛选')

    def sp_advertise_screen(self):
        """Screen advertising campaigns based on specified criteria"""
        data = self._process_chunks(
            '商品推广活动',
            filter.sp_ad,
            spend=config.spend,
            order=config.order,
            acos=config.acos,
            conversion=config.conversion,
            sku=config.sku
        )
        return self._save_results(data, 'SP投放商品筛选')

    def sp_pos_screen(self):
        """Screen and adjust bid positions based on specified criteria"""
        data = self._process_chunks(
            '商品推广活动',
            filter.sp_pos,
            spend=config.spend,
            order=config.order,
            acos=config.acos,
            conversion=config.conversion,
            sku=config.sku
        )
        return self._save_results(data, 'SP竞价调整')

    def sp_word_screen(self):
        """Screen search terms based on specified criteria"""
        data = self._process_chunks(
            '商品推广搜索词报告',
            filter.sp_word,
            click=config.click,
            click_rate=config.click_rate,
            order=config.order,
            conversion=config.conversion,
            sku=config.sku
        )
        return self._save_results(data, 'SP搜索词筛选')

    def sp_keyword_screen(self):
        """Screen keywords based on specified criteria"""
        data = self._process_chunks(
            '商品推广活动',
            filter.sp_keyword,
            click=config.click,
            click_rate=config.click_rate,
            order=config.order,
            conversion=config.conversion,
            sku=config.sku
        )
        return self._save_results(data, 'SP投放关键词筛选')

    def sp_invalid_screen(self):
        """Screen invalid campaigns based on specified criteria"""
        data = self._process_chunks(
            '商品推广活动',
            filter.sp_invalid,
            click=config.click,
            sku=config.sku
        )
        return self._save_results(data, 'SP无效筛选')

    def sp_descent_screen(self, file_path_old, file_path_new):
        """Screen campaigns with decreasing spend
        
        Args:
            file_path_old (str): Path to old data file
            file_path_new (str): Path to new data file
        """
        old_chunk, new_chunk = read_excel_in_chunks_pair(file_path_old, file_path_new, '商品推广活动')
        write_content = filter.sp_descent(old_chunk, new_chunk, config.spend, config.sku)
        return self._save_results(write_content, 'SP花费下降')

    def save_modified_rows(self, modified_rows, output_file_path):
        """Save modified rows to new Excel file
        
        Args:
            modified_rows (pd.DataFrame): Data to save
            output_file_path (str): Path to save file
        """
        try:
            modified_rows.to_excel(output_file_path, index=False, engine="openpyxl")
            print("Modified data saved to: {0}".format(output_file_path))
        except Exception as e:
            print("Error saving file: {0}".format(e))

    def adjust_bid(self):
        """Default function for bid adjustment"""
        print("Running default bid adjustment for SP campaigns")

    def call_function(self, function_name, *args):
        """Dynamically call specified function with arguments
        
        Args:
            function_name (str): Name of function to call
            *args: Additional arguments for the function
        """
        func = getattr(self, function_name, None)
        if callable(func):
            if function_name == 'sp_descent_screen':
                if len(args) == 2:
                    return func(args[0], args[1])
                print("Error: sp_descent_screen requires two file paths")
            else:
                return func()
        else:
            print("Function '{0}' not found".format(function_name))
