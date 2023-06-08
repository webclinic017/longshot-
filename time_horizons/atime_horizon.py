

class ATimeHorizon(object):

    def __init__(self,naming_convention,model_date_offset,y_pivot_column,y_pivot_number,prediction_pivot_column,prediction_pivot_number,rolling_number,n):
        self.naming_convention = naming_convention
        self.model_date_offset = model_date_offset
        self.y_pivot_number = y_pivot_number
        self.y_pivot_column = y_pivot_column
        self.prediction_pivot_column = prediction_pivot_column
        self.prediction_pivot_number = prediction_pivot_number
        self.rolling_number = rolling_number
        self.n = n