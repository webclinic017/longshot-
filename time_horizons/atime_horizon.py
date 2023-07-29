

class ATimeHorizon(object):
    # 
    # String name : name of the horizon
    # String naming_convention: naming and grouping string
    # String y_column = column to predict
    # Integer y_pivot_column: how far out to predict by
    # Integer rolling: the rolling value for averages
    # Integer window: the number of days to look back during window
    #     
    def __init__(self,name,naming_convention,y_column,y_pivot_number,rolling,window):
        self.name = name
        self.naming_convention = naming_convention
        self.y_column = y_column 
        self.y_pivot_number = y_pivot_number
        self.rolling = rolling
        self.window = window