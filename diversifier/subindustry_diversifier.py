class SubIndustryDiversifier(object):
    
    def __init__(self):
        self.name = "subindustry"
    
    def diversify(self,offerings,position):
        sub_industry = offerings["GICS Sub-Industry"].unique()
        sub_industry.sort()
        sub_industry = sub_industry[position]
        return offerings[offerings["GICS Sub-Industry"]==sub_industry]
    
    def positions(self):
        return 121