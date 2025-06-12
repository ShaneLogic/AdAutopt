from .ai_opt import AIOptimization
from .auto_create import AutomatedCreation

class DataAnalysis:
    """Main class for handling data analysis and optimization tasks"""
    
    def __init__(self):
        """Initialize analysis modules"""
        self.automated_creation = AutomatedCreation()
        self.ai_optimization = AIOptimization()

    def analyze_all(self):
        """Execute all analysis and optimization tasks
        
        This method runs both automated ad creation and AI-based optimization
        in sequence to ensure comprehensive analysis of the advertising data.
        """
        self.automated_creation.create_ads()
        self.ai_optimization.optimize_ads()
