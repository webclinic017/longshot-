from diversifier.basic_diversifier import BasicDiversifier
from diversifier.industry_diversifier import IndustryDiversifier
from diversifier.subindustry_diversifier import SubIndustryDiversifier

class DiversifierFactory(object):

    @classmethod
    def build_diversifier(self,diversifier_type):
        match diversifier_type:
            case "basic":
                portfolio = BasicDiversifier()
            case "industry":
                portfolio = IndustryDiversifier()
            case "subindustry":
                portfolio = SubIndustryDiversifier()
            case _:
                portfolio = None
        return portfolio