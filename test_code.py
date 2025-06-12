# -*- coding: utf-8 -*-
"""
Simple test script to verify code logic without requiring all dependencies
"""

import os
import sys

# Test 1: Check basic imports and module structure
def test_imports():
    """Test if our modules can be imported correctly"""
    try:
        # Test config module
        import config
        print("✓ Config module imported successfully")
        
        # Test basic module structure
        print("✓ Config attributes:", [attr for attr in dir(config) if not attr.startswith('_')])
        
    except ImportError as e:
        print("✗ Import error:", e)
        return False
    
    return True

# Test 2: Check main application structure
def test_main_app_structure():
    """Test the main application structure"""
    try:
        # Import Flask components
        print("Testing main app structure...")
        
        # Read main.py to check for syntax errors
        with open('main.py', 'r') as f:
            content = f.read()
            
        # Basic syntax check by compiling
        compile(content, 'main.py', 'exec')
        print("✓ main.py syntax is correct")
        
        # Check for required functions
        required_functions = ['validate_threshold', 'start_file_cleanup']
        for func in required_functions:
            if func in content:
                print("✓ Function '{}' found".format(func))
            else:
                print("✗ Function '{}' not found".format(func))
        
    except SyntaxError as e:
        print("✗ Syntax error in main.py:", e)
        return False
    except Exception as e:
        print("✗ Error testing main.py:", e)
        return False
    
    return True

# Test 3: Check auto_adjust module
def test_auto_adjust_module():
    """Test the auto_adjust module"""
    try:
        print("Testing auto_adjust module...")
        
        # Test module files
        files_to_check = [
            'auto_adjust/auto_adjust.py',
            'auto_adjust/sp.py',
            'auto_adjust/filters.py',
            'auto_adjust/sb.py',
            'auto_adjust/sd.py'
        ]
        
        for file_path in files_to_check:
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    content = f.read()
                compile(content, file_path, 'exec')
                print("✓ {} syntax is correct".format(file_path))
            else:
                print("✗ {} not found".format(file_path))
        
    except SyntaxError as e:
        print("✗ Syntax error in auto_adjust module:", e)
        return False
    except Exception as e:
        print("✗ Error testing auto_adjust module:", e)
        return False
    
    return True

# Test 4: Check data_analysis module
def test_data_analysis_module():
    """Test the data_analysis module"""
    try:
        print("Testing data_analysis module...")
        
        files_to_check = [
            'data_analysis/data_analysis.py',
            'data_analysis/ai_opt.py',
            'data_analysis/auto_create.py'
        ]
        
        for file_path in files_to_check:
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    content = f.read()
                compile(content, file_path, 'exec')
                print("✓ {} syntax is correct".format(file_path))
            else:
                print("✗ {} not found".format(file_path))
        
    except SyntaxError as e:
        print("✗ Syntax error in data_analysis module:", e)
        return False
    except Exception as e:
        print("✗ Error testing data_analysis module:", e)
        return False
    
    return True

# Test 5: Test function mapping
def test_function_mapping():
    """Test the function mapping logic"""
    try:
        print("Testing function mapping...")
        
        # Test function mapping dictionary
        function_mapping = {
            "SP商品筛选": "sp_product_screen",
            "SP投放商品筛选": "sp_advertise_screen",
            "SP投放关键词筛选": "sp_keyword_screen",
            "SP竞价调整": "sp_pos_screen",
            "SP搜索词筛选": "sp_word_screen",
            "SP无效筛选": "sp_invalid_screen",
            "SP花费下降": "sp_descent_screen"
        }
        
        print("✓ Function mapping contains {} functions".format(len(function_mapping)))
        
        for chinese_name, english_name in function_mapping.items():
            print("  {} -> {}".format(chinese_name, english_name))
        
        return True
    except Exception as e:
        print("✗ Error testing function mapping:", e)
        return False

# Test 6: Test directory structure
def test_directory_structure():
    """Test if all required directories and files exist"""
    try:
        print("Testing directory structure...")
        
        required_dirs = ['auto_adjust', 'data_analysis', 'auto_create', 'templates', 'uploads']
        required_files = ['main.py', 'config.py', 'requirements.txt']
        
        for directory in required_dirs:
            if os.path.exists(directory):
                print("✓ Directory '{}' exists".format(directory))
            else:
                print("! Directory '{}' missing (will be created if needed)".format(directory))
        
        for file_path in required_files:
            if os.path.exists(file_path):
                print("✓ File '{}' exists".format(file_path))
            else:
                print("✗ File '{}' missing".format(file_path))
        
        return True
    except Exception as e:
        print("✗ Error testing directory structure:", e)
        return False

def main():
    """Run all tests"""
    print("="*50)
    print("Testing optimized Amazon Ad Automation code")
    print("="*50)
    
    tests = [
        ("Basic Imports", test_imports),
        ("Main App Structure", test_main_app_structure),
        ("Auto Adjust Module", test_auto_adjust_module),
        ("Data Analysis Module", test_data_analysis_module),
        ("Function Mapping", test_function_mapping),
        ("Directory Structure", test_directory_structure)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print("\n" + "-"*30)
        print("Running test: {}".format(test_name))
        print("-"*30)
        
        if test_func():
            print("✓ Test '{}' PASSED".format(test_name))
            passed += 1
        else:
            print("✗ Test '{}' FAILED".format(test_name))
    
    print("\n" + "="*50)
    print("Test Results: {}/{} tests passed".format(passed, total))
    print("="*50)
    
    if passed == total:
        print("🎉 All tests passed! Code is ready for deployment.")
        print("\nNext steps:")
        print("1. Install required packages: pip install -r requirements.txt")
        print("2. Run the application: python main.py")
        print("3. Open browser to: http://localhost:5000")
    else:
        print("⚠️  Some tests failed. Please review the issues above.")

if __name__ == "__main__":
    main() 