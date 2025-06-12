from .sp import SPModule
from .sb import SBModule
from .sd import SDModule

class AutomationAdjustment:
    """Main class for handling automated adjustments across different advertising modules"""
    
    def __init__(self, file_path):
        """Initialize adjustment modules
        
        Args:
            file_path (str): Path to the input file for processing
        """
        self.sp = SPModule(file_path)
        self.sb = SBModule()
        self.sd = SDModule()

    def adjust_all(self, sp_function=None, file_path_old=None, file_path_new=None):
        """Execute adjustments across all modules
        
        Args:
            sp_function (str, optional): Specific SP function to call
            file_path_old (str, optional): Path to the old file for comparison
            file_path_new (str, optional): Path to the new file for comparison
        """
        # Execute SP module adjustments
        if sp_function == 'sp_descent_screen' and file_path_old and file_path_new:
            self.sp.sp_descent_screen(file_path_old, file_path_new)
        elif sp_function:
            self.sp.call_function(sp_function)
        else:
            self.sp.adjust_bid()  # Default function
            
        # Execute other module adjustments
        self.sb.adjust_sb()
        self.sd.adjust_sd()