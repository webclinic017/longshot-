class IndustryDiversifier(object):
    
    def __init__(self):
        self.name = "industry"
    
    def diversify(self,offerings,position):
        industries = offerings["GICS Sector"].unique()
        industries.sort()
        industry = industries[position]
        return offerings[offerings["GICS Sector"]==industry]
    
    def positions(self):
        return 11