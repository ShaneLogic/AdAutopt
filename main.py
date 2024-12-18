import os
import time
import config
import asyncio
import threading
from datetime import datetime, timedelta
from flask import Flask, render_template, request, send_file, Response
from werkzeug.utils import secure_filename
from auto_adjust.auto_adjust import AutomationAdjustment
from data_analysis.data_analysis import DataAnalysis

# 文件清理配置
UPLOAD_FOLDER = 'uploads'  # 上传文件的保存目录
FILE_RETENTION_HOURS = 0.5  # 文件存活时间（小时）

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# 创建异步锁
file_lock = asyncio.Lock()

# 映射功能名称
function_mapping = {
    "SP商品筛选": "sp_product_screen",
    "SP投放商品筛选": "sp_advertise_screen",
    "SP投放关键词筛选": "sp_keyword_screen",
    "SP竞价调整": "sp_pos_screen",
    "SP搜索词筛选": "sp_word_screen",
    "SP无效筛选": "sp_invalid_screen",
    "SP花费下降": "sp_descent_screen"
    # 可以根据实际情况添加更多映射关系
}


class AmazonAdOptimizationSystem:
    def __init__(self, file_path):
        self.file_path = file_path
        self.automation_adjustment = AutomationAdjustment(self.file_path)
        self.data_analysis = DataAnalysis()

    def run_optimization(self, sp_function=None, file_path_old=None, file_path_new=None):
        actual_function_name = None
        if sp_function and sp_function in function_mapping:
            actual_function_name = function_mapping[sp_function]
        
        # 调用自动化调整模块
        self.automation_adjustment.adjust_all(actual_function_name, file_path_old, file_path_new)
        
        # 调用数据分析模块
        self.data_analysis.analyze_all()


def validate_threshold(value, value_type, min_val=None, max_val=None):
    """验证输入阈值的工具函数"""
    if value in [None, ""]:  # 直接处理空值
        return None

    try:
        value = value_type(value)
        if min_val is not None and value < min_val:
            raise ValueError(f"值 {value} 小于最小值 {min_val}")
        if max_val is not None and value > max_val:
            raise ValueError(f"值 {value} 超过最大值 {max_val}")
        return value
    except (ValueError, TypeError) as e:
        print(f"超过阈值: {e}")
        return None


def start_file_cleanup():
    """后台线程：定期清理超过存活时间的旧文件"""
    while True:
        try:
            now = datetime.now()
            for filename in os.listdir(UPLOAD_FOLDER):
                file_path = os.path.join(UPLOAD_FOLDER, filename)
                if os.path.isfile(file_path):
                    file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                    if now - file_mtime > timedelta(hours=FILE_RETENTION_HOURS):
                        os.remove(file_path)
                        print(f"已删除过期文件: {file_path}")
        except Exception as e:
            print(f"清理文件时发生错误: {e}")
        time.sleep(600)  # 每小时检查一次


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        thresholds = {
            "impress_threshold": ("impress", int, 0, None),
            "click_threshold": ("click", int, 0, None),
            "click_rate_threshold": ("click_rate", float, 0.0, 100.0),
            "spend_threshold": ("spend", float, 0.0, None),
            "sales_threshold": ("sales", float, 0.0, None),
            "order_threshold": ("order", int, 0, None),
            "conversion_threshold": ("conversion", float, 0.0, 100.0),
            "acos_threshold": ("acos", float, 0.0, None),
            "cpc_threshold": ("cpc", float, 0.0, None),
            "roas_threshold": ("roas", float, 0.0, None),
        }
        for field, (config_attr, value_type, min_val, max_val) in thresholds.items():
            if field_value := validate_threshold(request.form.get(field), value_type, min_val, max_val):
                setattr(config, config_attr, field_value)

        # 提取 SKU
        if sku := request.form.get('sku'):
            config.sku = str(sku)

        # 处理文件上传
        if 'file' not in request.files:
            return "请选择一个文件！"
        
        file_new = request.files['file']
        file_old = request.files.get('file_old')  # 使用 get 方法，如果没有 file_old 则返回 None
        
        if file_new.filename == '':
            return "请选择一个文件！"
        
        if file_new:
            # 保存新文件
            filename_new = secure_filename(file_new.filename)
            file_path_new = os.path.join(UPLOAD_FOLDER, filename_new)
            file_new.save(file_path_new)

            file_path_old = None
            if file_old and file_old.filename != '':
                # 保存旧文件（如果有）
                filename_old = secure_filename(file_old.filename)
                file_path_old = os.path.join(UPLOAD_FOLDER, filename_old)
                file_old.save(file_path_old)

            # 数据加载与优化
            optimization_system = AmazonAdOptimizationSystem(file_path_new)
            sp_function_name_cn = request.form.get('sp_function')
            
            optimization_system.run_optimization(sp_function_name_cn, file_path_old, file_path_new)

            # 优化后的文件路径
            new_file_path = file_path_new.rsplit('.', 1)[0] + '_' + sp_function_name_cn + '.xlsx'
            return render_template('index.html', download_link=new_file_path)
    
    return render_template('index.html')


@app.route('/download/<path:filename>', methods=['GET'])
async def download_file(filename):
    """处理文件下载"""
    file_path = os.path.join(UPLOAD_FOLDER, os.path.basename(filename))
    print(f"尝试下载文件，路径为: {file_path}")
    try:
        return await asyncio.to_thread(send_file, file_path, as_attachment=True)
    except FileNotFoundError:
        return "文件未找到！", 404
    except Exception as e:
        return f"下载失败：{e}", 500


if __name__ == '__main__':
    # 启动后台清理线程
    cleanup_thread = threading.Thread(target=start_file_cleanup, daemon=True)
    cleanup_thread.start()

    # 启动 Flask 应用
    app.run(host="0.0.0.0", debug=True)
