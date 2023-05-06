import pandas as pd
from clause.delta_clause import DeltaClause

class ClauseFactory(object):

    @classmethod
    def build_clause(self,clause_type):
        match clause_type:
            case "delta":
                portfolio = DeltaClause()
            case _:
                portfolio = None
        return portfolio