import pandas as pd

## Class to transform sec filings mainly to read raw filing data
class SECTransformer(object):

    ## reading raw filing data from sec files
    @classmethod
    def transform(self,year,quarter,file_prefix):
        path = f"./sec/{year}q{quarter}/{file_prefix}.txt"
        try:
            data = pd.read_csv(path,engine="c",sep="\t",low_memory=False)
        except:
            data = pd.read_csv(path,engine="c",sep="\t",encoding="ISO-8859-1",low_memory=False)
        data["year"] = year
        data["quarter"] = quarter
        return data