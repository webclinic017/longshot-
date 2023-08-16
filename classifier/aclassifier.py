from data_product.adata_product import ADataProduct

class AClassifier(ADataProduct):
    
    def __init__(self,asset_class,time_horizon):
        super.__init__(self,asset_class,time_horizon)
