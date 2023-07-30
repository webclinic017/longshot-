from analysis.dately_analysis import DatelyAnalysis
from analysis.weekly_analysis import WeeklyAnalysis
# from analysis.monthly_analysis import DatelyAnalysis
class AnalysisFactory(object):

    @classmethod
    def build(self,name):
        match name:
            case "date":
                result = DatelyAnalysis
            case _:
                result = None
        return result