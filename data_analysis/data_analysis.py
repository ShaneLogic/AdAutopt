from .ai_opt import AIOptimization
from .auto_create import AutomatedCreation

class DataAnalysis:
    def __init__(self):
        self.automated_creation = AutomatedCreation()
        self.ai_optimization = AIOptimization()

    def analyze_all(self):
        self.automated_creation.create_ads()
        self.ai_optimization.optimize_ads()
