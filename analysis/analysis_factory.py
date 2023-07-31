from analysis.dately_analysis import DatelyAnalysis
from analysis.weekly_analysis import WeeklyAnalysis
from analysis.monthly_analysis import MonthlyAnalysis
from analysis.quarterly_analysis import QuarterlyAnalysis
class AnalysisFactory(object):

    @classmethod
    def build(self,name):
        match name:
            case "date":
                result = DatelyAnalysis
            case "week":
                result = WeeklyAnalysis
            case "month":
                result = MonthlyAnalysis
            case "quarter":
                result = QuarterlyAnalysis
            case _:
                result = None
        return result