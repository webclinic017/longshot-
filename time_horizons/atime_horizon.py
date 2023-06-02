

class ATimeHorizon(object):

    def __init__(self,naming_convention,time_pivot_column,projection_horizon):
        self.naming_convention = naming_convention
        self.time_pivot = time_pivot_column
        self.projection_horizon = projection_horizon