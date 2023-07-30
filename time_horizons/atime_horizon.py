

class ATimeHorizon(object):
    # 
    # String name : name of the horizon
    # String naming_convention: naming and grouping string
    # String y_column = column to predict
    # Integer y_pivot_column: how far out to predict by
    # Integer model_offset: how far back we pull model data
    # Integer rolling: the rolling value for averages
    # Integer window: the number of days to look back during window
    # Integer instances_per_year: number of instances per year
    # Integer holding_period: number of days to hold a position    
    def __init__(self,name,naming_convention,y_column,y_pivot_number,model_offset,rolling,window,instances_per_year,holding_period):
        self.name = name
        self.naming_convention = naming_convention
        self.y_column = y_column 
        self.y_pivot_number = y_pivot_number
        self.model_offset = model_offset
        self.rolling = rolling
        self.window = window
        self.instances_per_year = instances_per_year
        self.holding_period = holding_period