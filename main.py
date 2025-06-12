# -*- coding: utf-8 -*-
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

# File cleanup configuration
UPLOAD_FOLDER = 'uploads'  # Directory for saving uploaded files
FILE_RETENTION_HOURS = 0.5  # File retention time in hours

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Create upload directory if it doesn't exist
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# Create async lock for file operations
file_lock = asyncio.Lock()

# Function name mapping for different optimization tasks
FUNCTION_MAPPING = {
    "SP商品筛选": "sp_product_screen",
    "SP投放商品筛选": "sp_advertise_screen",
    "SP投放关键词筛选": "sp_keyword_screen",
    "SP竞价调整": "sp_pos_screen",
    "SP搜索词筛选": "sp_word_screen",
    "SP无效筛选": "sp_invalid_screen",
    "SP花费下降": "sp_descent_screen"
}

class AmazonAdOptimizationSystem:
    """Main system class for Amazon ad optimization"""
    
    def __init__(self, file_path):
        self.file_path = file_path
        self.automation_adjustment = AutomationAdjustment(self.file_path)
        self.data_analysis = DataAnalysis()

    def run_optimization(self, sp_function=None, file_path_old=None, file_path_new=None):
        """Run optimization process with specified function"""
        actual_function_name = FUNCTION_MAPPING.get(sp_function)
        if actual_function_name:
            self.automation_adjustment.adjust_all(actual_function_name, file_path_old, file_path_new)
            self.data_analysis.analyze_all()

def validate_threshold(value, value_type, min_val=None, max_val=None):
    """Validate input threshold values
    
    Args:
        value: Input value to validate
        value_type: Expected type of the value
        min_val: Minimum allowed value
        max_val: Maximum allowed value
    
    Returns:
        Validated value or None if invalid
    """
    if not value:
        return None

    try:
        value = value_type(value)
        if min_val is not None and value < min_val:
            raise ValueError("Value {0} is below minimum {1}".format(value, min_val))
        if max_val is not None and value > max_val:
            raise ValueError("Value {0} exceeds maximum {1}".format(value, max_val))
        return value
    except (ValueError, TypeError) as e:
        print("Threshold validation error: {0}".format(e))
        return None

def start_file_cleanup():
    """Background thread for cleaning up old files"""
    while True:
        try:
            now = datetime.now()
            for filename in os.listdir(UPLOAD_FOLDER):
                file_path = os.path.join(UPLOAD_FOLDER, filename)
                if os.path.isfile(file_path):
                    file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                    if now - file_mtime > timedelta(hours=FILE_RETENTION_HOURS):
                        os.remove(file_path)
                        print("Deleted expired file: {0}".format(file_path))
        except Exception as e:
            print("Error during file cleanup: {0}".format(e))
        time.sleep(600)  # Check every 10 minutes

@app.route('/', methods=['GET', 'POST'])
def index():
    """Main route handler for the application"""
    if request.method == 'POST':
        # Define threshold configurations
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

        # Update configuration with validated threshold values
        for field, (config_attr, value_type, min_val, max_val) in thresholds.items():
            field_value = validate_threshold(request.form.get(field), value_type, min_val, max_val)
            if field_value:
                setattr(config, config_attr, field_value)

        # Update SKU if provided
        sku = request.form.get('sku')
        if sku:
            config.sku = str(sku)

        # Handle file uploads
        if 'file' not in request.files:
            return "Please select a file!"

        file_new = request.files['file']
        file_old = request.files.get('file_old')

        if not file_new.filename:
            return "Please select a file!"

        # Save new file
        filename_new = secure_filename(file_new.filename)
        file_path_new = os.path.join(UPLOAD_FOLDER, filename_new)
        file_new.save(file_path_new)

        # Save old file if provided
        file_path_old = None
        if file_old and file_old.filename:
            filename_old = secure_filename(file_old.filename)
            file_path_old = os.path.join(UPLOAD_FOLDER, filename_old)
            file_old.save(file_path_old)

        # Run optimization
        optimization_system = AmazonAdOptimizationSystem(file_path_new)
        sp_function_name_cn = request.form.get('sp_function')
        optimization_system.run_optimization(sp_function_name_cn, file_path_old, file_path_new)

        # Generate download link
        base_name = file_path_new.rsplit('.', 1)[0]
        new_file_path = "{0}_{1}.xlsx".format(base_name, sp_function_name_cn)
        return render_template('index.html', download_link=new_file_path)

    return render_template('index.html')

@app.route('/download/<path:filename>', methods=['GET'])
def download_file(filename):
    """Handle file downloads"""
    file_path = os.path.join(UPLOAD_FOLDER, os.path.basename(filename))
    try:
        return send_file(file_path, as_attachment=True)
    except Exception as e:
        return "Download failed: {0}".format(e), 500

if __name__ == '__main__':
    # Start background cleanup thread
    cleanup_thread = threading.Thread(target=start_file_cleanup)
    cleanup_thread.daemon = True
    cleanup_thread.start()

    # Start Flask application
    app.run(host="0.0.0.0", debug=True)
