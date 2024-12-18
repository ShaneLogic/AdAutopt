from auto_create.keyword import KeywordModule
from auto_create.asin import ASINModule

class AutomatedCreation:
    def __init__(self):
        self.keyword_module = KeywordModule()
        self.asin_module = ASINModule()

    def create_ads(self):
        self.keyword_module.generate_keywords()
        self.asin_module.create_asin_ads()
