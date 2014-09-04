# generic imports
import Utils.PythonUtils.TRGPythonFunctions
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import re

class NewInAnalysis():
    
    # set up connection and define some useful functions
    def __init__(self):
        self.fe=Utils.PythonUtils.TRGPythonFunctions.FrontEnd()
        self.fe.ConnectToDCA()
        self.table="ds.trends_master_weekly"


    def main(self):

        weeks=pd.DataFrame(self.fe.executeandfetch("select batch_week from " + self.table + " group by batch_week order by batch_week"))
        self.df = pd.DataFrame()

        #Categories=['tops']
        Categories=self.fe.executeandfetch("select category from " + self.table + " where country='uk' group by 1 order by 1");
        Categories  = filter(None, Categories)
        for i in range(0,len(Categories)):
            ChosenCategory = Categories[i][0]
            print ChosenCategory
            
            SubCategories=self.fe.executeandfetch("select subcategory from " + self.table + " where country='uk' and category = '" + ChosenCategory + "' group by 1 order by 1");
            SubCategories  = filter(None, SubCategories)
            for j in range(0, len(SubCategories)):

                ChosenSubCategory=SubCategories[j][0]
                print ChosenSubCategory
                print "select retailername from " + self.table + " where country='uk' and category = '" + ChosenCategory + "' and subcategory = '" + ChosenSubCategory + "' group by 1 order by 1"
                Retailers=self.fe.executeandfetch("select retailername from " + self.table + " where country='uk' and category = '" + ChosenCategory + "' and subcategory = '" + ChosenSubCategory + "' group by 1 order by 1");
                Retailers = filter(None, Retailers)
                
                Counts_all=pd.DataFrame(self.fe.executeandfetch("select batch_week, sum(numitems) from " + self.table + " where country='uk' and category = '" + ChosenCategory + "' group by batch_week order by batch_week"))
                Counts_subcategory=pd.DataFrame(self.fe.executeandfetch("select batch_week, sum(numitems) from " + self.table + " where country='uk' and category = '" + ChosenCategory + "' and subcategory = '" + ChosenSubCategory + "' group by batch_week order by batch_week"))                  
                Counts_newin_all=pd.DataFrame(self.fe.executeandfetch("select batch_week, sum(numitems) from " + self.table + " where country='uk' and category = '" + ChosenCategory + "' and newin_7 = 1 group by batch_week order by batch_week"))
                Counts_newin_subcategory=pd.DataFrame(self.fe.executeandfetch("select batch_week, sum(numitems) from " + self.table + " where country='uk' and category = '" + ChosenCategory + "' and subcategory = '" + ChosenSubCategory + "' and newin_7 = 1 group by batch_week order by batch_week"))

                if Counts_newin_subcategory.empty:
                    Counts_newin_subcategory[0]=0
                    Counts_newin_subcategory[1]=0
                if Counts_newin_subcategory.empty:
                    Counts_newin_all[0]=0
                    Counts_newin_all[1]=0

                all=weeks.merge(Counts_all, how='left', on=0)
                all=all.merge(Counts_newin_all, how='left', on=0)
                all=all.merge(Counts_subcategory, how='left', on=0)
                all=all.merge(Counts_newin_subcategory, how='left', on=0)
                all.columns = ['Week', 'Counts_all', 'Counts_newin_all', 'Counts_subcategory', 'Counts_newin_subcategory']                
                test=((all['Counts_newin_subcategory']/all['Counts_newin_all'])/(all['Counts_subcategory']/all['Counts_all']))
                all['Ratio_all'] = test
                all['Category'] = ChosenCategory
                all['Subcategory'] = ChosenSubCategory

                for l in range(0,len(Retailers)):
                    ChosenRetailer=Retailers[l][0]
                    QueryRetailer=re.sub("'", "''", ChosenRetailer)

                    print "select sum(numitems) from " + self.table + " where country='uk' and category = '" + ChosenCategory + "' and subcategory = '" + ChosenSubCategory + "' and retailername = '" + QueryRetailer + "'"
                    Counts_all=pd.DataFrame(self.fe.executeandfetch("select batch_week, sum(numitems) from " + self.table + " where country='uk' and category = '" + ChosenCategory + "' and retailername = '" + QueryRetailer + "' group by batch_week order by batch_week"))
                    Counts_newin_all=pd.DataFrame(self.fe.executeandfetch("select batch_week, sum(numitems) from " + self.table + " where country='uk' and category = '" + ChosenCategory + "' and retailername = '" + QueryRetailer + "' and newin_7 = 1 group by batch_week order by batch_week"))
                    Counts_subcategory=pd.DataFrame(self.fe.executeandfetch("select batch_week, sum(numitems) from " + self.table + " where country='uk' and category = '" + ChosenCategory + "' and subcategory = '" + ChosenSubCategory + "' and retailername = '" + QueryRetailer + "' group by batch_week order by batch_week"))
                    Counts_newin_subcategory=pd.DataFrame(self.fe.executeandfetch("select batch_week, sum(numitems) from " + self.table + " where country='uk' and category = '" + ChosenCategory + "' and subcategory = '" + ChosenSubCategory + "' and retailername = '" + QueryRetailer + "' and newin_7 = 1 group by batch_week order by batch_week"))
                    
                    if Counts_newin_subcategory.empty:
                        Counts_newin_subcategory[0]=0
                        Counts_newin_subcategory[1]=0
                    if Counts_newin_subcategory.empty:
                        Counts_newin_all[0]=0
                        Counts_newin_all[1]=0

                    ret=weeks.merge(Counts_all, how='left', on=0)
                    ret=ret.merge(Counts_newin_all, how='left', on=0)
                    ret=ret.merge(Counts_subcategory, how='left', on=0)
                    ret=ret.merge(Counts_newin_subcategory, how='left', on=0)
                    
                    ret.columns = ['Week', 'Counts_all_ret', 'Counts_newin_all_ret', 'Counts_subcategory_ret', 'Counts_newin_subcategory_ret']                
                    test=((ret['Counts_newin_subcategory_ret']/ret['Counts_newin_all_ret'])/(ret['Counts_subcategory_ret']/ret['Counts_all_ret']))
                    ret['Ratio_ret'] = test
                    ret['Category'] = ChosenCategory
                    ret['Subcategory'] = ChosenSubCategory
                    ret['Retailer'] = ChosenRetailer

                    self.df=self.df.append(all.merge(ret, how='left', on=['Week', 'Category', 'Subcategory']))
                    
        test=abs(self.df['Ratio_all']-self.df['Ratio_ret'])
        self.df['DeltaRatio']=test
        self.df.save("allOutput.df")
        self.df.to_csv("allOutput.csv")


    
    def closedown():
        # close the DB connection
        fe.psyconn.stopConn()
