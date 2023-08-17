from data_product.adata_product import ADataProduct
from datetime import datetime

class ANonAIDataProduct(ADataProduct):

    def __init__(self,asset_class,time_horizon):
        super().__init__(asset_class,time_horizon)
        self.isai = False
    
    def create_sim(self):
        self.training_set()
        self.db.connect()
        self.db.store("sim",self.training_data)
        self.db.disconnect()
        return self.training_data
    
    def create_predictions(self):
        self.training_set()
        predictions = self.training_data
        predictions = predictions[predictions["year"]==datetime.now().year]
        self.db.connect()
        self.db.drop("predictions")
        self.db.store("predictions",predictions)
        self.db.disconnect()
        return predictions